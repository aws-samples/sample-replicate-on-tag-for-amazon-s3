"""Tests for the Lambda handler shim (src/lambda_handler.py).

Covers:
  - Property tests for _build_runtime_config and _load_solution_config
  - Unit tests for handler integration with run_interval

Feature: cloudformation-deployment
Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 8.1
"""
from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.models import RunOutcome
from src.lambda_handler import (
    _DEFAULT_CONFIG_KEY,
    _build_runtime_config,
    _load_solution_config,
    handler,
)

# A no-op RunOutcome for fake `run_interval` stand-ins that don't exercise
# Self_Reinvocation — any_capped_and_progressed=False keeps should_reinvoke
# from firing so these tests' non-reinvocation assertions are unaffected.
_NOOP_OUTCOME = RunOutcome(any_capped_and_progressed=False, buckets=[])


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Non-empty strings for required env vars
_non_empty_str = st.text(min_size=1, max_size=100).filter(lambda s: s.strip())

# KMS key ARN presence: absent, empty, whitespace-only, or a real value
_kms_variants = st.one_of(
    st.just(None),  # absent from env
    st.just(""),    # empty string
    st.just("   "),  # whitespace only
    _non_empty_str.map(lambda s: s.strip()).filter(lambda s: len(s) > 0),  # real value
)

# Minimal valid Solution_Config dict accepted by config_loader.load_config
_valid_bucket = st.fixed_dictionaries({
    "name": st.from_regex(r"[a-z0-9][a-z0-9\-]{1,10}[a-z0-9]", fullmatch=True),
    "region": st.sampled_from(["us-east-1", "eu-west-1", "ap-southeast-2"]),
})

_valid_config = st.fixed_dictionaries({
    "buckets": st.lists(
        _valid_bucket, min_size=1, max_size=3, unique_by=lambda b: (b["name"], b["region"])
    ),
})


# ---------------------------------------------------------------------------
# Property 1: Runtime_Config environment mapping (including conditional KMS)
# Feature: cloudformation-deployment, Property 1: runtime_config env mapping
# Validates: Requirements 1.2, 1.3, 2.3, 2.4
# ---------------------------------------------------------------------------


class TestBuildRuntimeConfigProperty:
    @given(
        state_bucket=_non_empty_str,
        athena_workgroup=_non_empty_str,
        athena_output_location=_non_empty_str,
        account_id=_non_empty_str,
        kms_variant=_kms_variants,
    )
    @settings(max_examples=100)
    def test_runtime_config_maps_env_vars_and_conditional_kms(
        self,
        state_bucket,
        athena_workgroup,
        athena_output_location,
        account_id,
        kms_variant,
    ):
        """_build_runtime_config maps required keys and includes kms_key_arn
        if and only if KMS_KEY_ARN is present and non-empty after strip."""
        env = {
            "STATE_BUCKET": state_bucket,
            "ATHENA_WORKGROUP": athena_workgroup,
            "ATHENA_OUTPUT_LOCATION": athena_output_location,
            "ACCOUNT_ID": account_id,
            "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
        }
        if kms_variant is not None:
            env["KMS_KEY_ARN"] = kms_variant

        result = _build_runtime_config(env)

        # Required keys always present
        assert result["state_bucket"] == state_bucket
        assert result["athena_workgroup"] == athena_workgroup
        assert result["athena_output_location"] == athena_output_location
        assert result["account_id"] == account_id
        assert result["batch_operations_role_arn"] == (
            "arn:aws:iam::123456789012:role/s3rot-batch-operations-role"
        )

        # KMS conditional logic
        if kms_variant is not None and kms_variant.strip():
            assert "kms_key_arn" in result
            assert result["kms_key_arn"] == kms_variant
        else:
            assert "kms_key_arn" not in result


# ---------------------------------------------------------------------------
# Property 2: Solution_Config parse round-trip
# Feature: cloudformation-deployment, Property 2: solution_config round-trip
# Validates: Requirements 1.1, 8.1
# ---------------------------------------------------------------------------


class TestLoadSolutionConfigProperty:
    @given(config=_valid_config)
    @settings(max_examples=100)
    def test_load_solution_config_round_trips(self, config):
        """JSON-serialized config loaded through _load_solution_config returns
        the original dict, and the result is accepted by load_config."""
        from src.core.config_loader import load_config

        config_bytes = json.dumps(config).encode("utf-8")

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(config_bytes),
        }

        result = _load_solution_config(mock_s3, "test-bucket", "config/key.json")
        assert result == config

        # Must be accepted by load_config without raising
        load_config(result)


# ---------------------------------------------------------------------------
# Property 3: Malformed Solution_Config surfaces as invocation failure
# Feature: cloudformation-deployment, Property 3: malformed config failure
# Validates: Requirements 1.5
# ---------------------------------------------------------------------------


class TestMalformedConfigProperty:
    @given(bad_bytes=st.binary(min_size=1, max_size=200).filter(
        lambda b: _is_not_valid_json(b)
    ))
    @settings(max_examples=100)
    def test_malformed_config_raises(self, bad_bytes):
        """Non-JSON bytes fed through the handler raise an exception
        (surfacing as Lambda invocation failure)."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(bad_bytes),
        }

        env = {
            "STATE_BUCKET": "example-bucket",
            "ATHENA_WORKGROUP": "primary",
            "ATHENA_OUTPUT_LOCATION": "s3://example-bucket/athena-results/",
            "ACCOUNT_ID": "123456789012",
            "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
        }

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with pytest.raises(Exception):
                    handler({}, None)


# ---------------------------------------------------------------------------
# Unit tests: handler integration
# Validates: Requirements 1.4, 1.5, 8.1
# ---------------------------------------------------------------------------


class TestHandlerUnit:
    def _make_env(self, **overrides):
        env = {
            "STATE_BUCKET": "example-state-bucket",
            "ATHENA_WORKGROUP": "example-workgroup",
            "ATHENA_OUTPUT_LOCATION": "s3://example-state-bucket/athena-results/",
            "ACCOUNT_ID": "123456789012",
            "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
        }
        env.update(overrides)
        return env

    def _mock_s3_with_config(self, config_dict):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(config_dict).encode("utf-8")),
        }
        return mock_s3

    def test_handler_calls_run_interval_with_parsed_config_and_runtime(self):
        """handler calls run_interval exactly once with the parsed config
        dict and the built Runtime_Config."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = self._mock_s3_with_config(config)
        env = self._make_env()

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval") as mock_run:
                    handler({}, None)

                    mock_run.assert_called_once()
                    call_args = mock_run.call_args
                    assert call_args[0][0] == config
                    assert call_args[0][1]["state_bucket"] == "example-state-bucket"
                    assert call_args[0][1]["athena_workgroup"] == "example-workgroup"
                    assert call_args[0][1]["account_id"] == "123456789012"

    def test_default_config_key_used_when_env_var_absent(self):
        """When SOLUTION_CONFIG_KEY is absent, _DEFAULT_CONFIG_KEY is used."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = self._mock_s3_with_config(config)
        env = self._make_env()
        # Ensure SOLUTION_CONFIG_KEY is NOT in env
        env.pop("SOLUTION_CONFIG_KEY", None)

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval"):
                    handler({}, None)

                    mock_s3.get_object.assert_called_once_with(
                        Bucket="example-state-bucket",
                        Key=_DEFAULT_CONFIG_KEY,
                    )

    def test_custom_config_key_used_when_env_var_present(self):
        """When SOLUTION_CONFIG_KEY is set, that key is used."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = self._mock_s3_with_config(config)
        env = self._make_env(SOLUTION_CONFIG_KEY="custom/path.json")

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval"):
                    handler({}, None)

                    mock_s3.get_object.assert_called_once_with(
                        Bucket="example-state-bucket",
                        Key="custom/path.json",
                    )

    def test_missing_s3_object_raises_client_error(self):
        """A missing S3 object raises ClientError propagating out of handler."""
        from botocore.exceptions import ClientError

        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}},
            "GetObject",
        )
        env = self._make_env()

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with pytest.raises(ClientError):
                    handler({}, None)

    def test_missing_required_env_var_raises_key_error(self):
        """A missing required env var raises KeyError propagating out of handler."""
        env = {
            # STATE_BUCKET intentionally omitted
            "ATHENA_WORKGROUP": "wg",
            "ATHENA_OUTPUT_LOCATION": "s3://b/r/",
            "ACCOUNT_ID": "123456789012",
            "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
        }

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(KeyError):
                handler({}, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_not_valid_json(b: bytes) -> bool:
    """Return True if bytes cannot be decoded as valid JSON."""
    try:
        json.loads(b.decode("utf-8", errors="replace"))
        return False
    except (json.JSONDecodeError, ValueError):
        return True


# ---------------------------------------------------------------------------
# CloudWatch metrics env var mapping — task 7.3
# Feature: cloudwatch-metrics
# Requirements: 1.1, 6.6, 9.5
# ---------------------------------------------------------------------------


class TestMetricsEnvVarMapping:
    """Verify METRICS_NAMESPACE and METRICS_DEPLOYMENT_ID are wired correctly."""

    _REQUIRED = {
        "STATE_BUCKET": "s3b",
        "ATHENA_WORKGROUP": "wg",
        "ATHENA_OUTPUT_LOCATION": "s3://s3b/r/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    def test_metrics_namespace_present_when_env_var_set(self):
        """METRICS_NAMESPACE set and non-empty → metrics_namespace in runtime config (Req 1.1, 6.6)."""
        env = {**self._REQUIRED, "METRICS_NAMESPACE": "MyOrg/S3Metrics"}
        result = _build_runtime_config(env)
        assert result.get("metrics_namespace") == "MyOrg/S3Metrics"

    def test_metrics_namespace_stripped(self):
        """METRICS_NAMESPACE is stripped of surrounding whitespace."""
        env = {**self._REQUIRED, "METRICS_NAMESPACE": "  MyOrg/S3Metrics  "}
        result = _build_runtime_config(env)
        assert result.get("metrics_namespace") == "MyOrg/S3Metrics"

    def test_metrics_namespace_absent_when_env_var_missing(self):
        """METRICS_NAMESPACE absent → metrics_namespace key omitted (Req 1.2)."""
        result = _build_runtime_config(self._REQUIRED)
        assert "metrics_namespace" not in result

    def test_metrics_namespace_absent_when_env_var_empty(self):
        """METRICS_NAMESPACE empty string → metrics_namespace key omitted (Req 1.2)."""
        env = {**self._REQUIRED, "METRICS_NAMESPACE": ""}
        result = _build_runtime_config(env)
        assert "metrics_namespace" not in result

    def test_metrics_namespace_absent_when_env_var_whitespace(self):
        """METRICS_NAMESPACE whitespace only → metrics_namespace key omitted (Req 1.2)."""
        env = {**self._REQUIRED, "METRICS_NAMESPACE": "   "}
        result = _build_runtime_config(env)
        assert "metrics_namespace" not in result

    def test_metrics_deployment_id_produces_deployment_dimension(self):
        """METRICS_DEPLOYMENT_ID set → metrics_dimensions={'Deployment': <value>} (Req 9.5)."""
        env = {
            **self._REQUIRED,
            "METRICS_NAMESPACE": "NS",
            "METRICS_DEPLOYMENT_ID": "stack-alpha",
        }
        result = _build_runtime_config(env)
        assert result.get("metrics_dimensions") == {"Deployment": "stack-alpha"}

    def test_metrics_deployment_id_stripped(self):
        """METRICS_DEPLOYMENT_ID is stripped of surrounding whitespace."""
        env = {
            **self._REQUIRED,
            "METRICS_NAMESPACE": "NS",
            "METRICS_DEPLOYMENT_ID": "  stack-alpha  ",
        }
        result = _build_runtime_config(env)
        assert result.get("metrics_dimensions") == {"Deployment": "stack-alpha"}

    def test_metrics_deployment_id_absent_when_env_var_missing(self):
        """METRICS_DEPLOYMENT_ID absent → no metrics_dimensions key (Req 9.5)."""
        env = {**self._REQUIRED, "METRICS_NAMESPACE": "NS"}
        result = _build_runtime_config(env)
        assert "metrics_dimensions" not in result

    def test_metrics_deployment_id_absent_when_env_var_empty(self):
        """METRICS_DEPLOYMENT_ID empty → no metrics_dimensions key."""
        env = {
            **self._REQUIRED,
            "METRICS_NAMESPACE": "NS",
            "METRICS_DEPLOYMENT_ID": "",
        }
        result = _build_runtime_config(env)
        assert "metrics_dimensions" not in result

    @given(
        ns=st.text(min_size=1, max_size=64).filter(lambda s: s.strip()),
        dep_id=st.text(min_size=1, max_size=64).filter(lambda s: s.strip()),
    )
    @settings(max_examples=100)
    def test_property_metrics_namespace_and_dimensions_populated(self, ns, dep_id):
        """Property: non-empty METRICS_NAMESPACE/METRICS_DEPLOYMENT_ID always populates
        both keys in runtime config.

        # Feature: cloudwatch-metrics, Property: metrics env var mapping
        """
        env = {
            **self._REQUIRED,
            "METRICS_NAMESPACE": ns,
            "METRICS_DEPLOYMENT_ID": dep_id,
        }
        result = _build_runtime_config(env)
        assert result.get("metrics_namespace") == ns.strip()
        assert result.get("metrics_dimensions") == {"Deployment": dep_id.strip()}


# ---------------------------------------------------------------------------
# Bucket disable-in-config on ceiling exceeded
# ---------------------------------------------------------------------------


class TestDisableBucketInConfig:
    """Verify that ceiling exceeded marks the bucket disabled in solution-config.json."""

    _BASE_ENV = {
        "STATE_BUCKET": "s3b",
        "ATHENA_WORKGROUP": "wg",
        "ATHENA_OUTPUT_LOCATION": "s3://s3b/r/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    def _make_s3_with_config(self, config: dict) -> MagicMock:
        mock = MagicMock()
        config_bytes = json.dumps(config).encode("utf-8")
        # Use side_effect so each get_object call gets a fresh BytesIO. The
        # ETag is what the disable write uses as its If-Match precondition.
        mock.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(config_bytes),
            "ETag": '"config-etag"',
        }
        mock.put_object.return_value = {}
        return mock

    @staticmethod
    def _config_put_body(mock_s3) -> bytes:
        """The Body of the put_object call that wrote solution-config.json."""
        calls = [
            c for c in mock_s3.put_object.call_args_list
            if c.kwargs.get("Key") == _DEFAULT_CONFIG_KEY
        ]
        assert len(calls) == 1, f"Expected one config write; got {len(calls)}"
        return calls[0].kwargs["Body"]

    def test_on_bucket_disable_callback_is_wired_and_called(self):
        """on_bucket_disable callback is injected and called when ceiling is exceeded."""
        disabled_calls: list[tuple] = []

        def fake_run_interval(config_source, runtime_config):
            cb = runtime_config.get("on_bucket_disable")
            assert cb is not None, "on_bucket_disable must be present in runtime_config"
            cb("my-bucket", "ceiling exceeded test")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)

        def boto3_client(service, **kwargs):
            return mock_s3

        with patch("src.lambda_handler.boto3.client", side_effect=boto3_client):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch.dict(os.environ, self._BASE_ENV, clear=True):
                    handler({}, None)

        # The callback should have called put_object to update the config
        # (the state-object write from the submission-record clear is a
        # separate, independent write to a different key).
        written = json.loads(self._config_put_body(mock_s3).decode("utf-8"))
        disabled_bucket = next(
            b for b in written["buckets"] if b["name"] == "my-bucket"
        )
        assert disabled_bucket["disabled"] is True
        assert "ceiling exceeded test" in disabled_bucket["disabled_reason"]
        assert "disabled_at" in disabled_bucket

    def test_on_bucket_disable_clears_stale_submission_records(self):
        """Self-service recovery: disabling a bucket also clears its
        persisted SubmissionRecord, so re-enabling doesn't immediately
        re-trip the circuit breaker on the same dead job_id."""

        def fake_run_interval(config_source, runtime_config):
            cb = runtime_config.get("on_bucket_disable")
            cb("my-bucket", "circuit breaker test")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)

        mock_store = MagicMock()

        with patch("src.lambda_handler.boto3.client", return_value=mock_s3):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch.dict(os.environ, self._BASE_ENV, clear=True):
                        handler({}, None)

        mock_store.clear_submission_records.assert_called_once()
        call_args = mock_store.clear_submission_records.call_args[0]
        assert call_args[1] == "s3b"  # state_bucket
        assert call_args[2] == "my-bucket"  # source_bucket

    def test_on_bucket_disable_publishes_alert_with_recovery_instructions(self):
        """The disabled-bucket alert is always logged, and published to SNS
        when BATCH_JOB_FAILURE_TOPIC_ARN is configured, mirroring
        check_report_handler's log-always/SNS-conditional pattern."""

        def fake_run_interval(config_source, runtime_config):
            cb = runtime_config.get("on_bucket_disable")
            cb("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)
        mock_logs = MagicMock()
        mock_sns = MagicMock()

        env = {
            **self._BASE_ENV,
            "BATCH_JOB_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:t",
            "BATCH_JOB_FAILURE_LOG_GROUP": "/some/log/group",
        }

        def boto3_client(service, **kwargs):
            return {"s3": mock_s3, "logs": mock_logs, "sns": mock_sns}[service]

        with patch("src.lambda_handler.boto3.client", side_effect=boto3_client):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch.dict(os.environ, env, clear=True):
                    handler({}, None)

        # The log entry stays structured JSON so it remains queryable.
        mock_logs.put_log_events.assert_called_once()
        logged = json.loads(
            mock_logs.put_log_events.call_args.kwargs["logEvents"][0]["message"]
        )
        assert logged["event"] == "bucket_disabled"
        assert logged["source_bucket"] == "my-bucket"
        assert 'disabled": false' in logged["recovery"]

        # The email is prose, not a JSON blob.
        mock_sns.publish.assert_called_once()
        published = mock_sns.publish.call_args.kwargs
        body = published["Message"]
        with pytest.raises(json.JSONDecodeError):
            json.loads(body)
        assert "Replication has been paused for source bucket my-bucket" in body
        assert "circuit breaker tripped" in body
        assert 'disabled": false' in body
        assert "Other monitored buckets are unaffected" in body

        subject = published["Subject"]
        assert "my-bucket" in subject
        assert len(subject) < 100
        subject.encode("ascii")  # SNS rejects a non-ASCII subject
        assert "\n" not in subject

    def test_on_bucket_disable_alert_logs_but_no_sns_when_topic_arn_absent(self):
        """Without BATCH_JOB_FAILURE_TOPIC_ARN, the disable is still logged
        (Requirement 8.4-style guarantee) but no SNS publish happens."""

        def fake_run_interval(config_source, runtime_config):
            cb = runtime_config.get("on_bucket_disable")
            cb("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)
        mock_logs = MagicMock()

        env = {**self._BASE_ENV, "BATCH_JOB_FAILURE_LOG_GROUP": "/some/log/group"}

        def boto3_client(service, **kwargs):
            return {"s3": mock_s3, "logs": mock_logs}[service]

        with patch("src.lambda_handler.boto3.client", side_effect=boto3_client):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch.dict(os.environ, env, clear=True):
                    handler({}, None)

        mock_logs.put_log_events.assert_called_once()

    def test_state_store_clear_failure_does_not_block_disable_write(self):
        """A ConditionalWriteError (or any exception) from
        clear_submission_records must not prevent the disabled flag itself
        from having already been persisted — it is best-effort."""
        from src.adapters.state_store import ConditionalWriteError

        def fake_run_interval(config_source, runtime_config):
            cb = runtime_config.get("on_bucket_disable")
            cb("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)

        mock_store = MagicMock()
        mock_store.clear_submission_records.side_effect = ConditionalWriteError("stale")

        with patch("src.lambda_handler.boto3.client", return_value=mock_s3):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch.dict(os.environ, self._BASE_ENV, clear=True):
                        handler({}, None)  # must not raise

        mock_s3.put_object.assert_called_once()
        written = json.loads(mock_s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
        disabled_bucket = next(
            b for b in written["buckets"] if b["name"] == "my-bucket"
        )
        assert disabled_bucket["disabled"] is True

    # --- Conditional write (security-scan-remediation Req 10.1-10.4) -------

    def test_disable_write_is_conditional_on_the_etag_it_read(self):
        """Req 10.1: the config write carries If-Match against the ETag from
        the read, so a concurrent writer (the config custom resource during a
        stack update) cannot have its change silently discarded."""

        def fake_run_interval(config_source, runtime_config):
            runtime_config["on_bucket_disable"]("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)

        with patch("src.lambda_handler.boto3.client", return_value=mock_s3):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch.dict(os.environ, self._BASE_ENV, clear=True):
                    handler({}, None)

        config_write = next(
            c for c in mock_s3.put_object.call_args_list
            if c.kwargs.get("Key") == _DEFAULT_CONFIG_KEY
        )
        assert config_write.kwargs["IfMatch"] == '"config-etag"'

    def test_precondition_failure_is_logged_and_does_not_raise(self, caplog):
        """Req 10.2/10.4: a PreconditionFailed on the config write is logged
        as an error naming the bucket and the disable reason, and does not
        raise into the caller."""
        from botocore.exceptions import ClientError

        def fake_run_interval(config_source, runtime_config):
            runtime_config["on_bucket_disable"]("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "PreconditionFailed", "Message": "stale"}},
            "PutObject",
        )

        with caplog.at_level("ERROR"):
            with patch("src.lambda_handler.boto3.client", return_value=mock_s3):
                with patch(
                    "src.lambda_handler.run_interval", side_effect=fake_run_interval
                ):
                    with patch.dict(os.environ, self._BASE_ENV, clear=True):
                        handler({}, None)  # must not raise

        messages = " ".join(r.getMessage() for r in caplog.records)
        assert "my-bucket" in messages
        assert "circuit breaker tripped" in messages
        assert "modified concurrently" in messages

    def test_precondition_failure_does_not_report_the_bucket_as_disabled(self):
        """Req 10.3: when the disable write does not land, none of the
        downstream effects that announce the bucket as disabled run — no
        submission-record clear and no bucket-disabled alert."""
        from botocore.exceptions import ClientError

        def fake_run_interval(config_source, runtime_config):
            runtime_config["on_bucket_disable"]("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        mock_s3 = self._make_s3_with_config(config)
        mock_s3.put_object.side_effect = ClientError(
            {"Error": {"Code": "PreconditionFailed", "Message": "stale"}},
            "PutObject",
        )
        mock_logs = MagicMock()
        mock_sns = MagicMock()
        mock_store = MagicMock()

        env = {
            **self._BASE_ENV,
            "BATCH_JOB_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:t",
            "BATCH_JOB_FAILURE_LOG_GROUP": "/some/log/group",
        }

        def boto3_client(service, **kwargs):
            return {"s3": mock_s3, "logs": mock_logs, "sns": mock_sns}[service]

        with patch("src.lambda_handler.boto3.client", side_effect=boto3_client):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch.dict(os.environ, env, clear=True):
                        handler({}, None)

        mock_store.clear_submission_records.assert_not_called()
        mock_logs.put_log_events.assert_not_called()
        mock_sns.publish.assert_not_called()

    def test_missing_etag_on_read_skips_the_write_entirely(self):
        """A config read that returns no ETag leaves no precondition to write
        under, so the write is skipped rather than performed unconditionally."""

        def fake_run_interval(config_source, runtime_config):
            runtime_config["on_bucket_disable"]("my-bucket", "circuit breaker tripped")
            return _NOOP_OUTCOME

        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        config_bytes = json.dumps(config).encode("utf-8")
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        with patch("src.lambda_handler.boto3.client", return_value=mock_s3):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch.dict(os.environ, self._BASE_ENV, clear=True):
                    handler({}, None)

        mock_s3.put_object.assert_not_called()

    def test_other_buckets_not_affected_by_callback(self):
        """Disabling one bucket does not touch other bucket entries."""
        config = {
            "buckets": [
                {"name": "bad-bucket", "region": "us-east-1"},
                {"name": "good-bucket", "region": "us-east-1"},
            ]
        }
        mock_s3 = self._make_s3_with_config(config)

        def fake_run_interval(config_source, runtime_config):
            runtime_config["on_bucket_disable"]("bad-bucket", "ceiling hit")
            return _NOOP_OUTCOME

        with patch("src.lambda_handler.boto3.client", return_value=mock_s3):
            with patch("src.lambda_handler.run_interval", side_effect=fake_run_interval):
                with patch.dict(os.environ, self._BASE_ENV, clear=True):
                    handler({}, None)

        written = json.loads(self._config_put_body(mock_s3).decode("utf-8"))
        bad = next(b for b in written["buckets"] if b["name"] == "bad-bucket")
        good = next(b for b in written["buckets"] if b["name"] == "good-bucket")
        assert bad["disabled"] is True
        assert "disabled" not in good or good.get("disabled") is False


# ---------------------------------------------------------------------------
# Completion-tracking env var mapping — task 22.2
# Feature: replication-completion-tracking
# Requirements: 3.5
# ---------------------------------------------------------------------------


class TestJournalReadRowCapEnvVarMapping:
    """Verify JOURNAL_READ_ROW_CAP is wired correctly into
    _build_runtime_config (code-review-remediation verification-notes.md
    "scaling risk" finding)."""

    _REQUIRED = {
        "STATE_BUCKET": "s3b",
        "ATHENA_WORKGROUP": "wg",
        "ATHENA_OUTPUT_LOCATION": "s3://s3b/r/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    def test_present_when_set(self):
        env = {**self._REQUIRED, "JOURNAL_READ_ROW_CAP": "750000"}
        result = _build_runtime_config(env)
        assert result.get("journal_read_row_cap") == 750000
        assert isinstance(result["journal_read_row_cap"], int)

    def test_absent_when_env_var_missing(self):
        result = _build_runtime_config(self._REQUIRED)
        assert "journal_read_row_cap" not in result

    def test_absent_when_env_var_empty(self):
        env = {**self._REQUIRED, "JOURNAL_READ_ROW_CAP": ""}
        result = _build_runtime_config(env)
        assert "journal_read_row_cap" not in result

    def test_invalid_value_omitted(self):
        env = {**self._REQUIRED, "JOURNAL_READ_ROW_CAP": "not-a-number"}
        result = _build_runtime_config(env)
        assert "journal_read_row_cap" not in result

    @given(row_cap=st.integers(min_value=1, max_value=10_000_000))
    @settings(max_examples=100)
    def test_property_valid_value_always_parsed_as_int(self, row_cap: int):
        env = {**self._REQUIRED, "JOURNAL_READ_ROW_CAP": str(row_cap)}
        result = _build_runtime_config(env)
        assert result["journal_read_row_cap"] == row_cap


# ---------------------------------------------------------------------------
# Journal_Read_Row_Cap memory-safety validation at config load — task 4.1
# Feature: scale-threshold-and-drain-throughput
# Requirements: 3.2
# ---------------------------------------------------------------------------


class TestRowCapValidationFailFast:
    """Verify handler() calls validate_row_cap against context.memory_limit_in_mb
    before any S3/Athena access, and fails fast (no run_interval call, no
    S3 client access) when the configured row cap violates the ceiling."""

    _ENV = {
        "STATE_BUCKET": "my-state-bucket",
        "ATHENA_WORKGROUP": "my-workgroup",
        "ATHENA_OUTPUT_LOCATION": "s3://my-state-bucket/athena-results/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    class _FakeContext:
        def __init__(self, memory_limit_in_mb):
            self.memory_limit_in_mb = memory_limit_in_mb

    def test_violating_row_cap_raises_config_error_before_s3_access(self):
        """A JOURNAL_READ_ROW_CAP exceeding the ceiling for
        context.memory_limit_in_mb raises ConfigError, and no S3 client
        method is ever called (fails fast, Requirement 3.2)."""
        from src.core.config_loader import ConfigError
        from src.core.row_cap_validation import IN_MEMORY_MEMORY_CEILING

        ceiling = IN_MEMORY_MEMORY_CEILING[1024]
        env = {**self._ENV, "JOURNAL_READ_ROW_CAP": str(ceiling + 1)}
        context = self._FakeContext(memory_limit_in_mb=1024)

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_s3 = MagicMock()
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval") as mock_run:
                    with pytest.raises(ConfigError):
                        handler({}, context)
                    mock_s3.get_object.assert_not_called()
                    mock_run.assert_not_called()

    def test_row_cap_within_ceiling_proceeds_normally(self):
        """A JOURNAL_READ_ROW_CAP within the ceiling for
        context.memory_limit_in_mb does not raise, and run_interval is
        still invoked normally."""
        from src.core.row_cap_validation import IN_MEMORY_MEMORY_CEILING

        ceiling = IN_MEMORY_MEMORY_CEILING[1024]
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(config).encode("utf-8")),
        }
        env = {**self._ENV, "JOURNAL_READ_ROW_CAP": str(ceiling)}
        context = self._FakeContext(memory_limit_in_mb=1024)

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval") as mock_run:
                    handler({}, context)
                    mock_run.assert_called_once()

    def test_default_row_cap_used_when_env_var_absent(self):
        """When JOURNAL_READ_ROW_CAP is unset, validation falls back to
        JOURNAL_READ_ROW_CAP_DEFAULT (500,000), which fits the 2,048 MiB
        ceiling but would violate a hypothetically much smaller ceiling —
        exercised here at the CloudFormation default memory size, where it
        must not raise."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(config).encode("utf-8")),
        }
        env = {**self._ENV}
        env.pop("JOURNAL_READ_ROW_CAP", None)
        context = self._FakeContext(memory_limit_in_mb=2048)

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval") as mock_run:
                    handler({}, context)
                    mock_run.assert_called_once()

    def test_no_context_memory_limit_skips_validation(self):
        """When context is None (e.g. a library caller), validation is
        skipped entirely rather than raising — no memory size is available
        to validate against."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(config).encode("utf-8")),
        }
        # A row cap that would violate every supported ceiling.
        env = {**self._ENV, "JOURNAL_READ_ROW_CAP": "999999999"}

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval") as mock_run:
                    handler({}, None)
                    mock_run.assert_called_once()


class TestReinvocationHasNoLeaseBypass:
    """Task 7.3 — Requirement 5.5: a reinvocation event must not bypass the
    per-bucket lease. handler() reads ``reinvocation_depth`` from the event
    only to feed ``should_reinvoke``'s depth argument and the outgoing
    self-invoke payload — it is never threaded into ``run_interval``, so a
    lease-contention condition inside ``run_interval`` is reached identically
    regardless of ``reinvocation_depth``.
    """

    _ENV = {
        "STATE_BUCKET": "my-state-bucket",
        "ATHENA_WORKGROUP": "my-workgroup",
        "ATHENA_OUTPUT_LOCATION": "s3://my-state-bucket/athena-results/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    def _mock_s3_with_config(self, config_dict):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(config_dict).encode("utf-8")),
        }
        return mock_s3

    def test_run_interval_called_with_identical_args_scheduled_vs_reinvocation(self):
        """A scheduled trigger (event={}) and a reinvocation
        (event={"reinvocation_depth": 5}) call run_interval with exactly
        the same (config, runtime_config) arguments — reinvocation_depth
        never reaches run_interval, so a lease-contention path inside it
        (mocked away here since run_interval itself is mocked) is exercised
        identically either way."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        env = self._ENV

        call_args_by_event = {}
        for label, event in (("scheduled", {}), ("reinvocation", {"reinvocation_depth": 5})):
            mock_s3 = self._mock_s3_with_config(config)
            with patch.dict(os.environ, env, clear=False):
                with patch("src.lambda_handler.boto3") as mock_boto3:
                    mock_boto3.client.return_value = mock_s3
                    with patch("src.lambda_handler.run_interval",
                                return_value=_NOOP_OUTCOME) as mock_run:
                        handler(event, None)
                        call_args_by_event[label] = mock_run.call_args

        # Compare everything except on_bucket_disable, which is a fresh
        # closure object each call (not comparable by identity/equality)
        # but is otherwise irrelevant to the lease-bypass question.
        scheduled_config, scheduled_runtime = call_args_by_event["scheduled"].args
        reinvocation_config, reinvocation_runtime = call_args_by_event["reinvocation"].args
        assert scheduled_config == reinvocation_config
        scheduled_runtime = {k: v for k, v in scheduled_runtime.items() if k not in ("on_bucket_disable", "on_submission_failure")}
        reinvocation_runtime = {k: v for k, v in reinvocation_runtime.items() if k not in ("on_bucket_disable", "on_submission_failure")}
        assert scheduled_runtime == reinvocation_runtime

    def test_lease_contention_outcome_identical_for_scheduled_and_reinvocation_event(self):
        """Simulate the existing lease-contention condition (run_interval
        propagating a ConditionalWriteError-derived RunOutcome — here
        modeled as run_interval raising, standing in for the orchestrator's
        real per-bucket skip-and-continue) and confirm handler() behaves
        identically whether the triggering event is a scheduled trigger or
        a reinvocation carrying a nonzero reinvocation_depth: in both
        cases, run_interval is invoked with the same arguments and any
        exception it raises propagates identically, since handler() has no
        reinvocation-aware branch anywhere near the run_interval call."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        env = self._ENV

        results = {}
        for label, event in (("scheduled", {}), ("reinvocation", {"reinvocation_depth": 7})):
            mock_s3 = self._mock_s3_with_config(config)
            with patch.dict(os.environ, env, clear=False):
                with patch("src.lambda_handler.boto3") as mock_boto3:
                    mock_boto3.client.return_value = mock_s3
                    with patch("src.lambda_handler.run_interval",
                                return_value=_NOOP_OUTCOME) as mock_run:
                        handler(event, None)
                        results[label] = mock_run.call_args

        # Same config + runtime_config passed to run_interval either way —
        # no reinvocation-specific argument, kwarg, or branch exists.
        # (on_bucket_disable excluded: a fresh, non-comparable closure per call.)
        scheduled_config, scheduled_runtime = results["scheduled"].args
        reinvocation_config, reinvocation_runtime = results["reinvocation"].args
        assert scheduled_config == reinvocation_config
        scheduled_runtime = {k: v for k, v in scheduled_runtime.items() if k not in ("on_bucket_disable", "on_submission_failure")}
        reinvocation_runtime = {k: v for k, v in reinvocation_runtime.items() if k not in ("on_bucket_disable", "on_submission_failure")}
        assert scheduled_runtime == reinvocation_runtime
        assert results["scheduled"].kwargs == results["reinvocation"].kwargs

    def test_reinvocation_depth_absent_from_runtime_config_passed_to_run_interval(self):
        """runtime_config passed to run_interval never carries
        reinvocation_depth under any key — confirming there is no channel
        by which a reinvocation could signal run_interval to take a
        different (lease-bypassing) path."""
        config = {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}
        mock_s3 = self._mock_s3_with_config(config)
        env = self._ENV

        with patch.dict(os.environ, env, clear=False):
            with patch("src.lambda_handler.boto3") as mock_boto3:
                mock_boto3.client.return_value = mock_s3
                with patch("src.lambda_handler.run_interval",
                            return_value=_NOOP_OUTCOME) as mock_run:
                    handler({"reinvocation_depth": 3}, None)
                    passed_config, passed_runtime_config = mock_run.call_args.args
                    assert "reinvocation_depth" not in passed_runtime_config
                    assert not any(
                        "reinvocation" in str(k).lower() for k in passed_runtime_config
                    )


class TestSelfReinvocation:
    """Task 7.4 — Self_Reinvocation wiring in handler() (task 7.1):
    reads ``reinvocation_depth`` from the event, computes ``should_reinvoke``
    from ``RunOutcome.any_capped_and_progressed`` (collapsed into
    capped=progressed=bucket_active), and issues an async self-invoke via
    ``boto3.client("lambda").invoke(...)`` when true.

    Requirements: 4.1, 4.4, 4.5, 5.1, 5.2, 5.3, 5.5.
    """

    _ENV = {
        "STATE_BUCKET": "my-state-bucket",
        "ATHENA_WORKGROUP": "my-workgroup",
        "ATHENA_OUTPUT_LOCATION": "s3://my-state-bucket/athena-results/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    _CAPPED_OUTCOME = RunOutcome(any_capped_and_progressed=True, buckets=[])

    class _FakeContext:
        def __init__(self, invoked_function_arn="arn:aws:lambda:us-east-1:123456789012:function:my-func"):
            self.invoked_function_arn = invoked_function_arn

    def _config(self):
        return {"buckets": [{"name": "test-bucket", "region": "us-east-1"}]}

    def _mock_s3(self):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps(self._config()).encode("utf-8")),
        }
        return mock_s3

    def _boto3_client_dispatch(self, mock_s3, mock_lambda):
        def boto3_client(service, **kwargs):
            return {"s3": mock_s3, "lambda": mock_lambda}[service]
        return boto3_client

    def _run_handler(self, event, context, run_outcome, env_overrides=None):
        """Run handler() with run_interval mocked to return run_outcome and
        boto3.client dispatching to fresh s3/lambda mocks. Returns
        (mock_s3, mock_lambda, mock_run)."""
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        env = {**self._ENV, **(env_overrides or {})}

        with patch.dict(os.environ, env, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval", return_value=run_outcome
                ) as mock_run:
                    handler(event, context)
        return mock_s3, mock_lambda, mock_run

    # -- 1. Reinvoke fires when capped+progressed and depth < chain_limit --

    def test_reinvoke_fires_when_capped_and_progressed_and_below_limit(self):
        context = self._FakeContext()
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {}, context, self._CAPPED_OUTCOME
        )

        mock_run.assert_called_once()
        mock_lambda.invoke.assert_called_once()
        call_kwargs = mock_lambda.invoke.call_args.kwargs
        assert call_kwargs["InvocationType"] == "Event"
        assert call_kwargs["FunctionName"] == context.invoked_function_arn
        payload = json.loads(call_kwargs["Payload"])
        assert payload == {"reinvocation_depth": 1}

    # -- 2. Reinvoke does NOT fire when any_capped_and_progressed is False --

    def test_reinvoke_does_not_fire_when_not_capped_and_progressed(self):
        context = self._FakeContext()
        not_capped_outcome = RunOutcome(any_capped_and_progressed=False, buckets=[])
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {}, context, not_capped_outcome
        )

        mock_run.assert_called_once()
        mock_lambda.invoke.assert_not_called()

    # -- 3. Reinvoke does NOT fire at the chain limit --

    def test_reinvoke_does_not_fire_at_chain_limit_default(self):
        """reinvocation_depth == default limit (20) even with
        any_capped_and_progressed=True must not reinvoke (5.1, 5.2)."""
        context = self._FakeContext()
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {"reinvocation_depth": 20}, context, self._CAPPED_OUTCOME
        )

        mock_run.assert_called_once()
        mock_lambda.invoke.assert_not_called()

    def test_reinvoke_fires_just_below_default_chain_limit(self):
        """depth=19 < default limit 20 -> fires, with payload depth 20."""
        context = self._FakeContext()
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {"reinvocation_depth": 19}, context, self._CAPPED_OUTCOME
        )

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload == {"reinvocation_depth": 20}

    # -- 4. Reinvoke fires with correct incremented depth at various depths --

    @pytest.mark.parametrize("depth", [0, 1, 5, 19])
    def test_reinvoke_payload_depth_is_always_depth_plus_one(self, depth):
        context = self._FakeContext()
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {"reinvocation_depth": depth}, context, self._CAPPED_OUTCOME
        )

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload == {"reinvocation_depth": depth + 1}

    # -- 4a. A malformed depth is clamped to 0 rather than trusted --

    @pytest.mark.parametrize(
        "bad_depth", [-1, -20, -99999, "5", 1.5, None, True, [3], {"depth": 3}]
    )
    def test_malformed_depth_is_clamped_to_zero(self, bad_depth):
        """A crafted event cannot lengthen the reinvocation chain.

        ``depth < chain_limit`` is always true for a negative depth, so an
        event carrying one would make every subsequent generation pass the
        chain-limit check and the chain would never terminate. A non-int
        depth is equally untrustworthy: a string compares as unorderable
        against the int limit and a float slips past the check while making
        the payload's ``depth + 1`` a non-integer. Every such value is
        clamped to 0, so the reinvocation issued is generation 1.

        ``True`` is included deliberately: ``isinstance(True, int)`` is true
        in Python, so a boolean is accepted and behaves as ``1`` — asserting
        depth 2 pins that rather than leaving it to be discovered later.
        """
        context = self._FakeContext()
        _mock_s3, mock_lambda, _mock_run = self._run_handler(
            {"reinvocation_depth": bad_depth}, context, self._CAPPED_OUTCOME
        )

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        expected = 2 if bad_depth is True else 1
        assert payload == {"reinvocation_depth": expected}

    def test_negative_depth_cannot_extend_the_chain_past_the_limit(self):
        """A negative depth does not buy extra generations beyond the limit.

        With the clamp, a depth of -5 and a chain limit of 1 reinvokes at
        generation 1; the next generation carries depth 1, which is not below
        the limit, so the chain stops. Without the clamp, -5 would be below
        the limit for six further generations.
        """
        context = self._FakeContext()
        _mock_s3, mock_lambda, _mock_run = self._run_handler(
            {"reinvocation_depth": -5},
            context,
            self._CAPPED_OUTCOME,
            env_overrides={"REINVOCATION_CHAIN_LIMIT": "1"},
        )

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload == {"reinvocation_depth": 1}

        # The generation that payload describes does not reinvoke again.
        _mock_s3b, mock_lambda_b, _mock_run_b = self._run_handler(
            payload,
            context,
            self._CAPPED_OUTCOME,
            env_overrides={"REINVOCATION_CHAIN_LIMIT": "1"},
        )
        mock_lambda_b.invoke.assert_not_called()

    # -- 5. Custom REINVOCATION_CHAIN_LIMIT env var is respected --

    def test_custom_chain_limit_fires_below_limit(self):
        context = self._FakeContext()
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {"reinvocation_depth": 2},
            context,
            self._CAPPED_OUTCOME,
            env_overrides={"REINVOCATION_CHAIN_LIMIT": "3"},
        )

        mock_lambda.invoke.assert_called_once()
        payload = json.loads(mock_lambda.invoke.call_args.kwargs["Payload"])
        assert payload == {"reinvocation_depth": 3}

    def test_custom_chain_limit_does_not_fire_at_limit(self):
        context = self._FakeContext()
        mock_s3, mock_lambda, mock_run = self._run_handler(
            {"reinvocation_depth": 3},
            context,
            self._CAPPED_OUTCOME,
            env_overrides={"REINVOCATION_CHAIN_LIMIT": "3"},
        )

        mock_lambda.invoke.assert_not_called()

    # -- 6. context=None or missing invoked_function_arn skips gracefully --

    def test_context_none_skips_reinvocation_without_raising(self):
        """A None context (e.g. a library/test caller) provides no target
        to self-invoke; handler must not raise and must not attempt any
        lambda client call."""
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=self._CAPPED_OUTCOME,
                ) as mock_run:
                    handler({}, None)  # must not raise

        mock_run.assert_called_once()
        mock_lambda.invoke.assert_not_called()

    def test_context_missing_invoked_function_arn_skips_reinvocation(self):
        """A context object without invoked_function_arn (falsy/absent)
        skips the self-invoke gracefully rather than raising."""

        class _ContextWithoutArn:
            pass

        mock_s3, mock_lambda, mock_run = self._run_handler(
            {}, _ContextWithoutArn(), self._CAPPED_OUTCOME
        )

        mock_run.assert_called_once()
        mock_lambda.invoke.assert_not_called()

    # -- 7. Async self-invoke trigger failure is swallowed (Req 5.3) --

    def test_invoke_failure_is_swallowed_and_run_completes_normally(self):
        """boto3 lambda client's .invoke() raising must not propagate out
        of handler() — the already-successful run must complete normally,
        and handler's own return value (None) is unaffected."""
        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        mock_lambda.invoke.side_effect = Exception("boom: async invoke failed")

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=self._CAPPED_OUTCOME,
                ) as mock_run:
                    result = handler({}, context)  # must not raise

        assert result is None
        mock_run.assert_called_once()
        mock_lambda.invoke.assert_called_once()

    def test_invoke_client_error_is_swallowed(self):
        """A botocore ClientError from .invoke() (e.g. throttling, access
        denied) is also swallowed, not just a generic Exception."""
        from botocore.exceptions import ClientError

        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        mock_lambda.invoke.side_effect = ClientError(
            {"Error": {"Code": "TooManyRequestsException", "Message": "throttled"}},
            "Invoke",
        )

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=self._CAPPED_OUTCOME,
                ) as mock_run:
                    handler({}, context)  # must not raise

        mock_run.assert_called_once()
        mock_lambda.invoke.assert_called_once()

    # -- 8. Observability entries (task 8.1, Requirements 6.2, 6.3) --

    def test_reinvocation_triggered_entry_emitted_with_chain_position(self):
        """When a reinvoke fires, a structured `reinvocation_triggered`
        entry is emitted recording the chain position (the depth the newly
        -triggered invocation will run at)."""
        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        emitted: list = []

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=self._CAPPED_OUTCOME,
                ):
                    with patch(
                        "src.lambda_handler.observability.emit",
                        side_effect=emitted.append,
                    ):
                        handler({"reinvocation_depth": 4}, context)

        triggered = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "reinvocation_triggered"
        ]
        assert len(triggered) == 1
        assert triggered[0]["chain_position"] == 5

    def test_no_reinvocation_triggered_entry_when_not_eligible(self):
        """No `reinvocation_triggered` entry is emitted when the run wasn't
        capped+progressed."""
        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        emitted: list = []
        not_capped_outcome = RunOutcome(any_capped_and_progressed=False, buckets=[])

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=not_capped_outcome,
                ):
                    with patch(
                        "src.lambda_handler.observability.emit",
                        side_effect=emitted.append,
                    ):
                        handler({}, context)

        triggered = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "reinvocation_triggered"
        ]
        assert triggered == []

    def test_chain_limit_reached_entry_emitted_when_eligible_but_at_limit(self):
        """When the run was capped+progressed but depth >= chain_limit, a
        `reinvocation_chain_limit_reached` entry is emitted, recording that
        the limit stopped further reinvocation and backlog remains
        (Requirement 6.3)."""
        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        emitted: list = []

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=self._CAPPED_OUTCOME,
                ):
                    with patch(
                        "src.lambda_handler.observability.emit",
                        side_effect=emitted.append,
                    ):
                        handler({"reinvocation_depth": 20}, context)

        limit_reached = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "reinvocation_chain_limit_reached"
        ]
        assert len(limit_reached) == 1
        assert limit_reached[0]["chain_limit"] == 20
        assert limit_reached[0]["depth"] == 20
        mock_lambda.invoke.assert_not_called()

    def test_no_chain_limit_reached_entry_when_not_capped(self):
        """A run that simply wasn't capped/progressed (nothing to
        reinvoke for) must not emit `reinvocation_chain_limit_reached`,
        even at a depth that would otherwise be at/over the limit — that
        entry is reserved for the "eligible but blocked by the limit"
        case specifically."""
        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        emitted: list = []
        not_capped_outcome = RunOutcome(any_capped_and_progressed=False, buckets=[])

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=not_capped_outcome,
                ):
                    with patch(
                        "src.lambda_handler.observability.emit",
                        side_effect=emitted.append,
                    ):
                        handler({"reinvocation_depth": 20}, context)

        limit_reached = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "reinvocation_chain_limit_reached"
        ]
        assert limit_reached == []

    def test_no_chain_limit_reached_entry_when_below_limit(self):
        """A run that is eligible and below the limit fires the reinvoke and
        must not also emit `reinvocation_chain_limit_reached`."""
        context = self._FakeContext()
        mock_s3 = self._mock_s3()
        mock_lambda = MagicMock()
        emitted: list = []

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._boto3_client_dispatch(mock_s3, mock_lambda),
            ):
                with patch(
                    "src.lambda_handler.run_interval",
                    return_value=self._CAPPED_OUTCOME,
                ):
                    with patch(
                        "src.lambda_handler.observability.emit",
                        side_effect=emitted.append,
                    ):
                        handler({"reinvocation_depth": 5}, context)

        limit_reached = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "reinvocation_chain_limit_reached"
        ]
        assert limit_reached == []
        mock_lambda.invoke.assert_called_once()


class TestCompletionTrackingEnvVarMapping:
    """Verify COMPLETION_REPORT_TOPIC_ARN and COMPLETION_CHECK_BATCH_SIZE are
    wired correctly into _build_runtime_config, and that the three env vars
    superseded by source-status completion tracking
    (DESTINATION_PRESENCE_CHECK_ROLE_ARN, COMPLETION_DESTINATION_REGION,
    COMPLETION_SOURCE_STATUS_THRESHOLD_SECONDS) are no longer read at all."""

    _REQUIRED = {
        "STATE_BUCKET": "s3b",
        "ATHENA_WORKGROUP": "wg",
        "ATHENA_OUTPUT_LOCATION": "s3://s3b/r/",
        "ACCOUNT_ID": "123456789012",
        "BATCH_OPERATIONS_ROLE_ARN": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    }

    # --- removed env vars — must never populate runtime_config -------------

    def test_destination_presence_check_role_arn_never_populated(self):
        """DESTINATION_PRESENCE_CHECK_ROLE_ARN is no longer read — even when
        set in the environment, no destination-account client parameter is
        ever produced (source-status completion tracking removes destination
        access entirely)."""
        env = {
            **self._REQUIRED,
            "DESTINATION_PRESENCE_CHECK_ROLE_ARN": "arn:aws:iam::999999999999:role/DestRole",
        }
        result = _build_runtime_config(env)
        assert "destination_presence_check_role_arn" not in result

    def test_completion_destination_region_never_populated(self):
        env = {**self._REQUIRED, "COMPLETION_DESTINATION_REGION": "eu-west-1"}
        result = _build_runtime_config(env)
        assert "completion_destination_region" not in result

    def test_completion_source_status_threshold_seconds_never_populated(self):
        env = {**self._REQUIRED, "COMPLETION_SOURCE_STATUS_THRESHOLD_SECONDS": "3600"}
        result = _build_runtime_config(env)
        assert "completion_source_status_threshold_seconds" not in result

    # --- completion_report_topic_arn ---------------------------------------

    def test_completion_report_topic_arn_present_when_set(self):
        env = {
            **self._REQUIRED,
            "COMPLETION_REPORT_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:MyTopic",
        }
        result = _build_runtime_config(env)
        assert result.get("completion_report_topic_arn") == (
            "arn:aws:sns:us-east-1:123456789012:MyTopic"
        )

    def test_completion_report_topic_arn_stripped(self):
        env = {
            **self._REQUIRED,
            "COMPLETION_REPORT_TOPIC_ARN": "  arn:aws:sns:us-east-1:123456789012:MyTopic  ",
        }
        result = _build_runtime_config(env)
        assert result.get("completion_report_topic_arn") == (
            "arn:aws:sns:us-east-1:123456789012:MyTopic"
        )

    def test_completion_report_topic_arn_absent_when_missing(self):
        result = _build_runtime_config(self._REQUIRED)
        assert "completion_report_topic_arn" not in result

    def test_completion_report_topic_arn_absent_when_empty(self):
        env = {**self._REQUIRED, "COMPLETION_REPORT_TOPIC_ARN": ""}
        result = _build_runtime_config(env)
        assert "completion_report_topic_arn" not in result

    # --- completion_check_batch_size ----------------------------------------

    def test_completion_check_batch_size_present_when_set(self):
        env = {**self._REQUIRED, "COMPLETION_CHECK_BATCH_SIZE": "500"}
        result = _build_runtime_config(env)
        assert result.get("completion_check_batch_size") == 500
        assert isinstance(result["completion_check_batch_size"], int)

    def test_completion_check_batch_size_absent_when_missing(self):
        result = _build_runtime_config(self._REQUIRED)
        assert "completion_check_batch_size" not in result

    def test_completion_check_batch_size_absent_when_empty(self):
        env = {**self._REQUIRED, "COMPLETION_CHECK_BATCH_SIZE": ""}
        result = _build_runtime_config(env)
        assert "completion_check_batch_size" not in result

    def test_completion_check_batch_size_invalid_omitted(self):
        env = {**self._REQUIRED, "COMPLETION_CHECK_BATCH_SIZE": "not-a-number"}
        result = _build_runtime_config(env)
        assert "completion_check_batch_size" not in result

    # --- property: any non-empty numeric-string value parses as int --------

    @given(
        batch_size=st.integers(min_value=1, max_value=10_000_000),
    )
    @settings(max_examples=100)
    def test_property_completion_check_batch_size_parsed_as_int(self, batch_size):
        """Property: a non-empty numeric COMPLETION_CHECK_BATCH_SIZE env var
        string always parses to its corresponding int runtime_config value.

        # Feature: source-status-completion-tracking, Property: completion env var mapping
        """
        env = {
            **self._REQUIRED,
            "COMPLETION_CHECK_BATCH_SIZE": str(batch_size),
        }
        result = _build_runtime_config(env)
        assert result["completion_check_batch_size"] == batch_size


# ---------------------------------------------------------------------------
# check_report_handler — report-missing detection (design.md Decision 9,
# task 23.8). Feature: source-status-completion-tracking.
# Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.7, 8.8
# ---------------------------------------------------------------------------


class TestCheckReportHandler:
    _ENV = {
        "STATE_BUCKET": "my-state-bucket",
        "ACCOUNT_ID": "123456789012",
    }
    _NOW = None  # set per-test via datetime patch where needed

    def _config(self, bucket_name="my-bucket", region="us-east-1"):
        return {"buckets": [{"name": bucket_name, "region": region}]}

    def _make_boto3_clients(self, s3_client, s3control_client, logs_client, sns_client=None):
        clients = {
            "s3": s3_client,
            "s3control": s3control_client,
            "logs": logs_client,
        }
        if sns_client is not None:
            clients["sns"] = sns_client

        def boto3_client(service, **kwargs):
            return clients[service]

        return boto3_client

    def _rec(self, config_id="cfg-1", job_id="job-1", manifest_key="manifests/cfg-1/ts/m.json"):
        from datetime import datetime as dt
        from src.core.models import SubmissionRecord, SubmissionStatus
        return SubmissionRecord(
            replication_config_id=config_id,
            source_bucket="my-bucket",
            job_id=job_id,
            manifest_key=manifest_key,
            submitted_at=dt(2024, 1, 1, tzinfo=timezone.utc),
            status=SubmissionStatus.SUBMITTED,
        )

    def test_report_found_is_a_noop_and_does_not_alert(self):
        from src.core.models import SubmissionRecord

        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Complete", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}
        mock_store.completion_job_exists.return_value = False

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=True,
                    ):
                        from src.lambda_handler import check_report_handler
                        check_report_handler({}, None)

        mock_store.add_alerted_config.assert_not_called()
        logs_client.put_log_events.assert_not_called()

    def test_report_absent_and_overdue_escalates_and_marks_alerted(self):
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Complete", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()
        sns_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}
        mock_store.completion_job_exists.return_value = False

        env = {**self._ENV, "BATCH_JOB_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:t"}

        # now far enough past terminal_at to be overdue
        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        with patch.dict(os.environ, env, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(
                    s3_client, s3control_client, logs_client, sns_client
                ),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        mock_store.add_alerted_config.assert_called_once()
        logs_client.put_log_events.assert_called_once()
        sns_client.publish.assert_called_once()
        call_kwargs = sns_client.publish.call_args.kwargs
        assert call_kwargs["TopicArn"] == "arn:aws:sns:us-east-1:123456789012:t"

    def test_report_absent_but_not_overdue_does_not_escalate(self):
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Complete", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}
        mock_store.completion_job_exists.return_value = False

        fixed_now = datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc)  # only 10 min elapsed

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        mock_store.add_alerted_config.assert_not_called()
        logs_client.put_log_events.assert_not_called()

    def test_already_suppressed_config_is_skipped(self):
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = {"my-bucket"}
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    from src.lambda_handler import check_report_handler
                    check_report_handler({}, None)

        s3control_client.describe_job.assert_not_called()
        mock_store.add_alerted_config.assert_not_called()

    def test_non_terminal_job_is_skipped(self):
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Active", "CreationTime": datetime(2024, 1, 1, tzinfo=timezone.utc)}
        }
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}
        mock_store.completion_job_exists.return_value = False

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                    ) as mock_exists:
                        from src.lambda_handler import check_report_handler
                        check_report_handler({}, None)
                        mock_exists.assert_not_called()

        mock_store.add_alerted_config.assert_not_called()

    def test_legacy_config_id_keyed_record_is_skipped_not_iterated(self):
        """Requirement 3.4: the per-config_id fallback is removed. A
        submission_records dict that carries only a legacy
        replication_config_id-keyed entry (no bucket_name sentinel key) is
        no longer iterated — the bucket is skipped entirely, with no
        describe_job call and no alert.

        This is the load-bearing regression test for Requirement 3.4: it
        fails if the old ``else: records_to_check = [... for config_id, rec
        in prior_submissions.items()]`` fallback is restored, since that
        fallback would call describe_job for the legacy-keyed record.
        """
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        # Only a legacy config_id key present — no "my-bucket" sentinel key.
        mock_store.get_submission_records.return_value = {
            "cfg-legacy": self._rec(config_id="cfg-legacy", job_id="job-legacy")
        }

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    from src.lambda_handler import check_report_handler
                    check_report_handler({}, None)

        s3control_client.describe_job.assert_not_called()
        mock_store.completion_job_exists.assert_not_called()
        mock_store.add_alerted_config.assert_not_called()

    def test_already_confirmed_job_is_skipped(self):
        """completion_job_exists True — nothing to detect as missing."""
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}
        mock_store.completion_job_exists.return_value = True

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    from src.lambda_handler import check_report_handler
                    check_report_handler({}, None)

        s3control_client.describe_job.assert_not_called()
        mock_store.add_alerted_config.assert_not_called()

    def test_missing_topic_arn_still_logs_but_does_not_publish_sns(self):
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Failed", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.return_value = {"my-bucket": self._rec()}
        mock_store.completion_job_exists.return_value = False

        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        logs_client.put_log_events.assert_called_once()
        mock_store.add_alerted_config.assert_called_once()

    def test_one_config_failure_does_not_block_the_rest(self):
        """Property 20 smoke case: an exception for one bucket must not
        prevent another bucket from being processed."""
        s3_client = MagicMock()
        config_bytes = json.dumps(
            {"buckets": [
                {"name": "bucket-bad", "region": "us-east-1"},
                {"name": "bucket-good", "region": "us-east-1"},
            ]}
        ).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

        def describe_job(AccountId, JobId):
            if JobId == "job-bad":
                raise RuntimeError("boom")
            return {
                "Job": {
                    "Status": "Complete",
                    "CreationTime": terminal_at,
                    "TerminationDate": terminal_at,
                }
            }

        s3control_client.describe_job.side_effect = describe_job
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()

        def get_submission_records(client, state_bucket, bucket_name):
            if bucket_name == "bucket-bad":
                return {bucket_name: self._rec(config_id=bucket_name, job_id="job-bad")}
            return {bucket_name: self._rec(config_id=bucket_name, job_id="job-good")}

        mock_store.get_submission_records.side_effect = get_submission_records
        mock_store.completion_job_exists.return_value = False

        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        # bucket-good must still have been alerted despite bucket-bad raising.
        mock_store.add_alerted_config.assert_called_once()
        call_args = mock_store.add_alerted_config.call_args
        assert call_args[0][3] == "bucket-good"

    def test_migrated_single_bucket_sentinel_record_uses_bucket_name_as_identity(self):
        """design.md D6 (task 6.1): once a bucket has migrated (its
        ``submission_records`` dict holds exactly one entry keyed by the
        per-bucket sentinel, ``bucket_name`` itself), escalation/suppression
        must use ``bucket_name`` as the identity passed to
        ``get_alerted_configs``/``add_alerted_config`` and to
        ``_publish_report_missing_alert``'s ``replication_config_id`` — not
        any legacy config_id string (there isn't one to use)."""
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Failed", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()
        sns_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        # Migrated form: the ONLY key in submission_records is the bucket's
        # own name (record_submission's D3 collapse, task 3.1/4.2).
        mock_store.get_submission_records.return_value = {
            "my-bucket": self._rec(config_id="my-bucket", job_id="job-migrated"),
        }
        mock_store.completion_job_exists.return_value = False

        env = {**self._ENV, "BATCH_JOB_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:t"}
        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        with patch.dict(os.environ, env, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(
                    s3_client, s3control_client, logs_client, sns_client
                ),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        mock_store.add_alerted_config.assert_called_once()
        add_call_args = mock_store.add_alerted_config.call_args
        assert add_call_args[0][3] == "my-bucket"

        mock_store.get_alerted_configs.assert_called_once()

        # The email body is prose; the per-bucket sentinel identity is asserted
        # against the structured log entry, which retains the field.
        logged = json.loads(
            logs_client.put_log_events.call_args.kwargs["logEvents"][0]["message"]
        )
        assert logged["replication_config_id"] == "my-bucket"

        body = sns_client.publish.call_args.kwargs["Message"]
        assert "my-bucket" in body
        assert "completion report has never appeared" in body
        assert "reporting gap, not a confirmed replication failure" in body

    def test_legacy_keyed_records_without_bucket_sentinel_are_skipped(self):
        """After removal of the per-config_id fallback: a bucket whose
        submission_records has only legacy per-config_id keys (no
        bucket-name sentinel key) is now skipped — no check is performed."""
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client = MagicMock()
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Failed", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        # Legacy form: two distinct per-config_id keys, no "my-bucket" key.
        mock_store.get_submission_records.return_value = {
            "cfg-legacy-1": self._rec(config_id="cfg-legacy-1", job_id="job-legacy-1"),
            "cfg-legacy-2": self._rec(config_id="cfg-legacy-2", job_id="job-legacy-2"),
        }
        mock_store.completion_job_exists.return_value = False

        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(s3_client, s3control_client, logs_client),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        # No alert is fired — the bucket is skipped because no sentinel key exists
        mock_store.add_alerted_config.assert_not_called()
        s3control_client.describe_job.assert_not_called()

    def test_migrated_bucket_sentinel_escalation_fires_at_most_once(self):
        """Requirement 8.5 / design.md D6: for a migrated bucket (the only
        ``submission_records`` key is the bucket-name sentinel), a
        persistently overdue-and-missing report must escalate on the first
        ``check_report_handler`` invocation and then be suppressed on the
        next scheduled invocation, once ``add_alerted_config``'s effect is
        reflected in ``get_alerted_configs``'s return value — exactly the
        same suppression contract already proven for the legacy per-config_id
        identity, now applied to the bucket-name identity."""
        s3_client = MagicMock()
        config_bytes = json.dumps(self._config()).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        s3control_client = MagicMock()
        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client.describe_job.return_value = {
            "Job": {"Status": "Failed", "CreationTime": terminal_at, "TerminationDate": terminal_at}
        }
        logs_client = MagicMock()
        sns_client = MagicMock()

        mock_store = MagicMock()
        # First invocation: not yet alerted. Second invocation (simulating
        # the next 5-minute schedule): add_alerted_config's effect from the
        # first invocation has persisted.
        mock_store.get_alerted_configs.side_effect = [set(), {"my-bucket"}]
        # Migrated form: the ONLY key in submission_records is the bucket's
        # own name (record_submission's D3 collapse, task 3.1/4.2).
        mock_store.get_submission_records.return_value = {
            "my-bucket": self._rec(config_id="my-bucket", job_id="job-migrated"),
        }
        mock_store.completion_job_exists.return_value = False

        env = {**self._ENV, "BATCH_JOB_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:t"}
        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        with patch.dict(os.environ, env, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(
                    s3_client, s3control_client, logs_client, sns_client
                ),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler

                            # First invocation: not yet alerted -> escalates.
                            check_report_handler({}, None)

        mock_store.add_alerted_config.assert_called_once()
        add_call_args = mock_store.add_alerted_config.call_args
        assert add_call_args[0][3] == "my-bucket"
        logs_client.put_log_events.assert_called_once()
        sns_client.publish.assert_called_once()

        # Second invocation (next 5-minute schedule): get_alerted_configs now
        # reflects the first invocation's add_alerted_config call, so the
        # already-alerted bucket must be suppressed — no second escalation.
        with patch.dict(os.environ, env, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=self._make_boto3_clients(
                    s3_client, s3control_client, logs_client, sns_client
                ),
            ):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler

                            check_report_handler({}, None)

        # Still only one call each, from the first invocation — the second
        # invocation must not have added any further escalation. describe_job
        # is called once in total (first invocation only): the second
        # invocation's suppression check short-circuits before reaching it.
        mock_store.add_alerted_config.assert_called_once()
        logs_client.put_log_events.assert_called_once()
        sns_client.publish.assert_called_once()
        s3control_client.describe_job.assert_called_once()


# ---------------------------------------------------------------------------
# Property 18: Alert delivery always logs, and publishes to SNS if and only
# if a topic ARN is configured
# Feature: source-status-completion-tracking, Property 18: Alert delivery
# always logs, and publishes to SNS if and only if a topic ARN is configured
# Validates: Requirements 8.3, 8.4
# ---------------------------------------------------------------------------


class TestProperty18AlertDeliveryLogAndSnsGating:
    """# Feature: source-status-completion-tracking, Property 18: Alert delivery always logs, and publishes to SNS if and only if a topic ARN is configured

    Validates: Requirements 8.3, 8.4
    """

    @given(topic_arn_present=st.booleans())
    @settings(max_examples=100)
    def test_log_always_writes_sns_iff_arn_present(self, topic_arn_present: bool) -> None:
        """# Feature: source-status-completion-tracking, Property 18: Alert delivery always logs, and publishes to SNS if and only if a topic ARN is configured"""
        from src.lambda_handler import _publish_report_missing_alert

        logs_client = MagicMock()
        sns_client = MagicMock()
        topic_arn = "arn:aws:sns:us-east-1:123456789012:t" if topic_arn_present else None

        _publish_report_missing_alert(
            sns_client,
            logs_client,
            topic_arn,
            "log-group",
            source_bucket="b",
            replication_config_id="cfg-1",
            job_id="job-1",
            now=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )

        logs_client.put_log_events.assert_called_once()
        if topic_arn_present:
            sns_client.publish.assert_called_once()
        else:
            sns_client.publish.assert_not_called()


# ---------------------------------------------------------------------------
# Property 20: A single failing config or bucket in the report-missing
# check never blocks the rest of that invocation's batch
# Feature: source-status-completion-tracking, Property 20: A single failing
# config or bucket in the report-missing check never blocks the rest of
# that invocation's batch
# Validates: Requirements 8.8
# ---------------------------------------------------------------------------


class TestProperty20BatchIsolation:
    """# Feature: source-status-completion-tracking, Property 20: A single failing config or bucket in the report-missing check never blocks the rest of that invocation's batch

    Validates: Requirements 8.8
    """

    @given(
        num_configs=st.integers(min_value=1, max_value=6),
        failing_indices=st.data(),
    )
    @settings(max_examples=100)
    def test_arbitrary_failing_subset_does_not_block_the_rest(
        self, num_configs: int, failing_indices
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 20: A single failing config or bucket in the report-missing check never blocks the rest of that invocation's batch"""
        failing = failing_indices.draw(
            st.sets(st.integers(min_value=0, max_value=num_configs - 1), max_size=num_configs)
        )

        # Create multiple buckets (one record per bucket, keyed by bucket name)
        bucket_names = [f"bucket-{i}" for i in range(num_configs)]
        s3_client = MagicMock()
        config_bytes = json.dumps(
            {"buckets": [{"name": b, "region": "us-east-1"} for b in bucket_names]}
        ).encode("utf-8")
        s3_client.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(config_bytes)}

        terminal_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        s3control_client = MagicMock()

        def describe_job(AccountId, JobId):
            idx = int(JobId.split("-")[1])
            if idx in failing:
                raise RuntimeError("boom")
            return {
                "Job": {
                    "Status": "Complete",
                    "CreationTime": terminal_at,
                    "TerminationDate": terminal_at,
                }
            }

        s3control_client.describe_job.side_effect = describe_job
        logs_client = MagicMock()

        from src.core.models import SubmissionRecord, SubmissionStatus

        # Each bucket has exactly one record keyed by its own name (the sentinel)
        def get_submission_records(client, state_bucket, bucket_name):
            idx = int(bucket_name.split("-")[1])
            return {
                bucket_name: SubmissionRecord(
                    replication_config_id=bucket_name,
                    source_bucket=bucket_name,
                    job_id=f"job-{idx}",
                    manifest_key=f"manifests/{bucket_name}/ts/m.json",
                    submitted_at=terminal_at,
                    status=SubmissionStatus.SUBMITTED,
                )
            }

        mock_store = MagicMock()
        mock_store.get_alerted_configs.return_value = set()
        mock_store.get_submission_records.side_effect = get_submission_records
        mock_store.completion_job_exists.return_value = False

        fixed_now = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        alerted_configs: list[str] = []

        def add_alerted_config(client, state_bucket, source_bucket, config_id, current_etag=None):
            alerted_configs.append(config_id)

        mock_store.add_alerted_config.side_effect = add_alerted_config

        def boto3_client(service, **kwargs):
            return {"s3": s3_client, "s3control": s3control_client, "logs": logs_client}[service]

        with patch.dict(os.environ, self._env(), clear=True):
            with patch("src.lambda_handler.boto3.client", side_effect=boto3_client):
                with patch(
                    "src.lambda_handler.state_store_module.StateStore",
                    return_value=mock_store,
                ):
                    with patch(
                        "src.lambda_handler.bops_report_reader.report_object_exists",
                        return_value=False,
                    ):
                        with patch("src.lambda_handler.datetime") as mock_dt:
                            mock_dt.now.return_value = fixed_now
                            from src.lambda_handler import check_report_handler
                            check_report_handler({}, None)

        expected_alerted = {f"bucket-{i}" for i in range(num_configs) if i not in failing}
        assert set(alerted_configs) == expected_alerted

    @staticmethod
    def _env():
        return {"STATE_BUCKET": "my-state-bucket", "ACCOUNT_ID": "123456789012"}


# ---------------------------------------------------------------------------
# Alert suppression across consecutive invocations, against a real StateStore
# Feature: security-scan-remediation
# Requirements: 8.1, 8.5
# ---------------------------------------------------------------------------


class _ConditionalWriteFakeS3:
    """In-memory S3 stand-in that enforces ``If-Match``/``If-None-Match``.

    The suppression defect this covers is invisible to a ``MagicMock`` S3
    client, which accepts any precondition. This fake rejects a mismatched
    ``If-Match`` and an ``If-None-Match: *`` against an existing key with
    ``PreconditionFailed``, exactly as S3 does, so the real
    ``StateStore.add_alerted_config`` write path is exercised end to end.
    """

    def __init__(self, objects: dict[str, bytes]):
        self._bodies: dict[str, bytes] = {}
        self._etags: dict[str, str] = {}
        self._counter = 0
        self.precondition_failures = 0
        # Every put_object kwarg set, so tests can assert on the encryption
        # headers a write carried.
        self.put_calls: list[dict] = []
        for key, body in objects.items():
            self._store(key, body)

    def _store(self, key: str, body: bytes) -> str:
        self._counter += 1
        self._bodies[key] = body
        self._etags[key] = f'"etag-{self._counter}"'
        return self._etags[key]

    def body_of(self, key: str) -> dict:
        return json.loads(self._bodies[key].decode("utf-8"))

    def get_object(self, Bucket, Key, **kwargs):
        if Key not in self._bodies:
            raise _s3_client_error("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self._bodies[Key]), "ETag": self._etags[Key]}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.put_calls.append({"Key": Key, **kwargs})
        exists = Key in self._bodies
        if_match = kwargs.get("IfMatch")
        if_none_match = kwargs.get("IfNoneMatch")
        if if_none_match == "*" and exists:
            self.precondition_failures += 1
            raise _s3_client_error("PreconditionFailed", "PutObject")
        if if_match is not None and (
            not exists or if_match != self._etags.get(Key)
        ):
            self.precondition_failures += 1
            raise _s3_client_error("PreconditionFailed", "PutObject")
        body = Body if isinstance(Body, bytes) else str(Body).encode("utf-8")
        return {"ETag": self._store(Key, body)}


def _s3_client_error(code: str, operation: str):
    from botocore.exceptions import ClientError

    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class TestReportMissingAlertSuppressionAcrossInvocations:
    """A report-missing condition that persists across consecutive
    ``check_report_handler`` invocations must alert exactly once.

    Unlike ``TestCheckReportHandler``, this exercises the real
    ``StateStore`` against an S3 fake that enforces conditional-write
    preconditions, so the suppression write actually has to land for the
    second invocation to be suppressed.
    """

    _STATE_BUCKET = "example-state-bucket"
    _SOURCE_BUCKET = "my-bucket"
    _JOB_ID = "job-overdue"
    _ENV = {
        "STATE_BUCKET": _STATE_BUCKET,
        "ACCOUNT_ID": "123456789012",
        "BATCH_JOB_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:t",
        "BATCH_JOB_FAILURE_LOG_GROUP": "/some/log/group",
    }
    _TERMINAL_AT = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _NOW = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)  # 2h later: overdue

    def _state_object_bytes(self) -> bytes:
        from src.core.checkpoint_serializer import serialize_submission_record
        from src.core.models import SubmissionRecord, SubmissionStatus

        record = SubmissionRecord(
            replication_config_id=self._SOURCE_BUCKET,
            source_bucket=self._SOURCE_BUCKET,
            job_id=self._JOB_ID,
            manifest_key=f"manifests/{self._SOURCE_BUCKET}/ts/m.json",
            submitted_at=self._TERMINAL_AT,
            status=SubmissionStatus.SUBMITTED,
        )
        payload = {
            "source_bucket": self._SOURCE_BUCKET,
            "last_processed_watermark": "",
            "lease": None,
            "processed_window": [],
            "submission_records": {
                self._SOURCE_BUCKET: serialize_submission_record(record)
            },
        }
        return json.dumps(payload).encode("utf-8")

    def _fake_s3(self) -> _ConditionalWriteFakeS3:
        config = {"buckets": [{"name": self._SOURCE_BUCKET, "region": "us-east-1"}]}
        return _ConditionalWriteFakeS3({
            _DEFAULT_CONFIG_KEY: json.dumps(config).encode("utf-8"),
            f"state/{self._SOURCE_BUCKET}.json": self._state_object_bytes(),
        })

    def _invoke(self, fake_s3, logs_client, sns_client):
        s3control_client = MagicMock()
        s3control_client.describe_job.return_value = {
            "Job": {
                "Status": "Failed",
                "CreationTime": self._TERMINAL_AT,
                "TerminationDate": self._TERMINAL_AT,
            }
        }
        clients = {
            "s3": fake_s3,
            "s3control": s3control_client,
            "logs": logs_client,
            "sns": sns_client,
        }

        with patch.dict(os.environ, self._ENV, clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=lambda service, **kw: clients[service],
            ):
                with patch(
                    "src.lambda_handler.bops_report_reader.report_object_exists",
                    return_value=False,
                ):
                    with patch("src.lambda_handler.datetime") as mock_dt:
                        mock_dt.now.return_value = self._NOW
                        from src.lambda_handler import check_report_handler

                        check_report_handler({}, None)

    def test_two_consecutive_invocations_publish_exactly_one_alert(self):
        fake_s3 = self._fake_s3()
        logs_client = MagicMock()
        sns_client = MagicMock()

        self._invoke(fake_s3, logs_client, sns_client)
        self._invoke(fake_s3, logs_client, sns_client)

        assert sns_client.publish.call_count == 1
        assert logs_client.put_log_events.call_count == 1
        assert fake_s3.precondition_failures == 0

    def test_first_invocation_persists_the_suppression_marker(self):
        fake_s3 = self._fake_s3()

        self._invoke(fake_s3, MagicMock(), MagicMock())

        state = fake_s3.body_of(f"state/{self._SOURCE_BUCKET}.json")
        assert state["completion_report_alerted_configs"] == [self._SOURCE_BUCKET]
        # Every other top-level key survives the suppression write.
        assert self._SOURCE_BUCKET in state["submission_records"]

    def test_alert_repeats_once_the_marker_is_cleared(self):
        """Suppression lasts only while the marker is present — clearing it
        (as the creation hook does once the report is observed) lets a
        subsequent invocation alert again."""
        from src.adapters.state_store import StateStore

        fake_s3 = self._fake_s3()
        sns_client = MagicMock()

        self._invoke(fake_s3, MagicMock(), sns_client)
        StateStore().clear_alerted_config(
            fake_s3, self._STATE_BUCKET, self._SOURCE_BUCKET, self._SOURCE_BUCKET
        )
        self._invoke(fake_s3, MagicMock(), sns_client)

        assert sns_client.publish.call_count == 2


# ---------------------------------------------------------------------------
# check_report_handler must not downgrade state-object encryption
#
# AWS Security Agent finding f-01a44434-1115-4388-89fd-7a2471a377e2:
# check_report_handler built its StateStore with no kms_key_arn, and the
# CompletionReportCheckLambda had no KMS_KEY_ARN env var. It writes the same
# state objects as the main handler (add_alerted_config / clear_alerted_config),
# so on a deployment with KmsKeyArn set its writes silently rewrote SSE-KMS
# objects under SSE-S3.
# ---------------------------------------------------------------------------


class TestCheckReportHandlerStateWriteEncryption:
    _STATE_BUCKET = "example-state-bucket"
    _SOURCE_BUCKET = "my-bucket"
    _JOB_ID = "job-overdue"
    _KMS_KEY_ARN = (
        "arn:aws:kms:us-east-1:123456789012:key/"
        "11111111-2222-3333-4444-555555555555"
    )
    _TERMINAL_AT = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _NOW = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)  # 2h later: overdue

    def _env(self, kms: bool) -> dict:
        env = {
            "STATE_BUCKET": self._STATE_BUCKET,
            "ACCOUNT_ID": "123456789012",
            "BATCH_JOB_FAILURE_TOPIC_ARN": (
                "arn:aws:sns:us-east-1:123456789012:t"
            ),
            "BATCH_JOB_FAILURE_LOG_GROUP": "/some/log/group",
        }
        if kms:
            env["KMS_KEY_ARN"] = self._KMS_KEY_ARN
        return env

    def _state_object_bytes(self) -> bytes:
        from src.core.checkpoint_serializer import serialize_submission_record
        from src.core.models import SubmissionRecord, SubmissionStatus

        record = SubmissionRecord(
            replication_config_id=self._SOURCE_BUCKET,
            source_bucket=self._SOURCE_BUCKET,
            job_id=self._JOB_ID,
            manifest_key=f"manifests/{self._SOURCE_BUCKET}/ts/m.json",
            submitted_at=self._TERMINAL_AT,
            status=SubmissionStatus.SUBMITTED,
        )
        payload = {
            "source_bucket": self._SOURCE_BUCKET,
            "last_processed_watermark": "",
            "lease": None,
            "processed_window": [],
            "submission_records": {
                self._SOURCE_BUCKET: serialize_submission_record(record)
            },
        }
        return json.dumps(payload).encode("utf-8")

    def _fake_s3(self) -> _ConditionalWriteFakeS3:
        config = {"buckets": [{"name": self._SOURCE_BUCKET, "region": "us-east-1"}]}
        return _ConditionalWriteFakeS3({
            _DEFAULT_CONFIG_KEY: json.dumps(config).encode("utf-8"),
            f"state/{self._SOURCE_BUCKET}.json": self._state_object_bytes(),
        })

    def _invoke(self, fake_s3, kms: bool):
        s3control_client = MagicMock()
        s3control_client.describe_job.return_value = {
            "Job": {
                "Status": "Failed",
                "CreationTime": self._TERMINAL_AT,
                "TerminationDate": self._TERMINAL_AT,
            }
        }
        clients = {
            "s3": fake_s3,
            "s3control": s3control_client,
            "logs": MagicMock(),
            "sns": MagicMock(),
        }
        with patch.dict(os.environ, self._env(kms), clear=True):
            with patch(
                "src.lambda_handler.boto3.client",
                side_effect=lambda service, **kw: clients[service],
            ):
                with patch(
                    "src.lambda_handler.bops_report_reader.report_object_exists",
                    return_value=False,
                ):
                    with patch("src.lambda_handler.datetime") as mock_dt:
                        mock_dt.now.return_value = self._NOW
                        from src.lambda_handler import check_report_handler

                        check_report_handler({}, None)

    def _state_writes(self, fake_s3) -> list[dict]:
        return [c for c in fake_s3.put_calls if c["Key"].startswith("state/")]

    def test_state_writes_use_sse_kms_when_key_configured(self):
        fake_s3 = self._fake_s3()
        self._invoke(fake_s3, kms=True)

        writes = self._state_writes(fake_s3)
        assert writes, "expected the suppression write to reach S3"
        for call in writes:
            assert call.get("ServerSideEncryption") == "aws:kms", (
                f"state write to {call['Key']} downgraded encryption"
            )
            assert call.get("SSEKMSKeyId") == self._KMS_KEY_ARN

    def test_state_writes_omit_kms_headers_when_no_key_configured(self):
        """Without KmsKeyArn the bucket default (SSE-S3) applies, so no
        KMS headers should be sent at all."""
        fake_s3 = self._fake_s3()
        self._invoke(fake_s3, kms=False)

        writes = self._state_writes(fake_s3)
        assert writes
        for call in writes:
            assert "ServerSideEncryption" not in call
            assert "SSEKMSKeyId" not in call


class TestAlertLogStreamNaming:
    """Each alert kind writes to a log stream named for that kind.

    All three alerts share ``_write_batch_job_failure_log``, which previously
    hardcoded a ``report-missing`` stream prefix — so the bucket-disabled and
    submission-failure alerts landed in a stream named for an unrelated
    condition, which is misleading when navigating the log group. These
    assertions exist so that collapsing them again fails a test rather than
    quietly degrading the log layout.
    """

    _NOW = datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)
    _TOPIC = "arn:aws:sns:us-west-2:123456789012:BatchJobFailureTopic"

    @staticmethod
    def _stream_names(logs_client) -> list[str]:
        return [
            c.kwargs["logStreamName"]
            for c in logs_client.put_log_events.call_args_list
        ]

    def test_report_missing_alert_stream(self):
        from src.lambda_handler import _publish_report_missing_alert

        logs_client = MagicMock()
        _publish_report_missing_alert(
            MagicMock(), logs_client, None, "log-group",
            source_bucket="b", replication_config_id="b", job_id="job-1",
            now=self._NOW,
        )
        assert self._stream_names(logs_client) == ["report-missing-2026-07-29"]

    def test_bucket_disabled_alert_stream(self):
        from src.lambda_handler import _publish_bucket_disabled_alert

        logs_client = MagicMock()
        _publish_bucket_disabled_alert(
            MagicMock(), logs_client, None, "log-group",
            state_bucket="state", config_key="config/solution-config.json",
            bucket_name="b", reason="because", now=self._NOW,
        )
        assert self._stream_names(logs_client) == ["bucket-disabled-2026-07-29"]

    def test_submission_failure_alert_stream(self):
        from src.lambda_handler import _publish_submission_failure_alert

        logs_client = MagicMock()
        _publish_submission_failure_alert(
            sns_client=MagicMock(), logs_client=logs_client, topic_arn=None,
            log_group_name="log-group", bucket_name="b",
            error_reason="Parameter validation failed", now=self._NOW,
        )
        assert self._stream_names(logs_client) == [
            "submission-failure-2026-07-29"
        ]

    def test_the_three_alert_kinds_use_distinct_streams(self):
        """The property that matters: no two alert kinds share a stream."""
        from src.lambda_handler import (
            _publish_bucket_disabled_alert,
            _publish_report_missing_alert,
            _publish_submission_failure_alert,
        )

        logs_client = MagicMock()
        _publish_report_missing_alert(
            MagicMock(), logs_client, None, "log-group",
            source_bucket="b", replication_config_id="b", job_id="job-1",
            now=self._NOW,
        )
        _publish_bucket_disabled_alert(
            MagicMock(), logs_client, None, "log-group",
            state_bucket="state", config_key="config/solution-config.json",
            bucket_name="b", reason="because", now=self._NOW,
        )
        _publish_submission_failure_alert(
            sns_client=MagicMock(), logs_client=logs_client, topic_arn=None,
            log_group_name="log-group", bucket_name="b",
            error_reason="Parameter validation failed", now=self._NOW,
        )
        names = self._stream_names(logs_client)
        assert len(names) == 3
        assert len(set(names)) == 3, f"alert kinds share a log stream: {names}"
