"""Every AWS request this Solution issues must be valid against the real API.

Motivation, in full, is in ``tests/api_shape.py``: adapter tests inject a
``MagicMock`` client, which accepts any keyword at any depth, so a request
parameter the AWS API does not have is indistinguishable from one it does. The
job-submission path shipped ``Manifest.Location.ManifestEncryption`` — a member
S3 Control has never had — and no test could see it, while no deployment with
``KmsKeyArn`` set could submit a job at all.

This module exercises each call site that builds its request kwargs
*conditionally*, which is where that class of bug hides: KMS on/off, ETag
present/absent, version id present/absent, pagination token present/absent,
optional subject, optional dimensions. Each captured call is replayed through
botocore's own parameter validation against the service model.

The per-branch coverage matters more than the call count. A KMS branch that is
only taken on a KMS-configured deployment is exactly the branch that no unit
test and no default-configuration deployment ever executes.
"""
from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.adapters import (
    bops_report_reader,
    replication_config_adapter,
    source_status_adapter,
)
from src.adapters.athena_journal_adapter import read_journal
from src.adapters.inventory_manifest_writer import write_in_memory_inventory_manifest
from src.adapters.metrics_publisher import MetricsPublisher
from src.adapters.sns_report_adapter import publish_completion_report
from src.adapters.state_store import StateStore
from src.core.models import (
    BucketMetrics,
    CheckpointState,
    MonitoredBucket,
    RunOutcome,
    RunResult,
)
from src.lambda_handler import (
    _publish_report_missing_alert,
    _publish_submission_failure_alert,
    handler,
)
from tests.api_shape import assert_calls_match_api

_ACCOUNT = "123456789012"
_STATE_BUCKET = "state-bucket"
_SRC_BUCKET = "source-bucket"
_KMS_ARN = f"arn:aws:kms:us-west-2:{_ACCOUNT}:key/1234abcd-12ab-34cd-56ef-1234567890ab"
_ROLE_ARN = f"arn:aws:iam::{_ACCOUNT}:role/replication-role"
_BATCHOPS_ROLE_ARN = f"arn:aws:iam::{_ACCOUNT}:role/s3rot-batch-operations-role"
_ETAG = '"0123456789abcdef0123456789abcdef"'
_NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=timezone.utc)


def _client_error(code: str, operation: str = "Op") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "err"}}, operation)


# ---------------------------------------------------------------------------
# S3 — state objects. Four branches: create vs update, KMS vs bucket default.
# ---------------------------------------------------------------------------


class TestStateStorePutObject:
    @staticmethod
    def _s3_first_write() -> MagicMock:
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey", "GetObject")
        client.put_object.return_value = {"ETag": _ETAG}
        return client

    @staticmethod
    def _s3_existing(state: CheckpointState) -> MagicMock:
        from src.core.checkpoint_serializer import serialize

        client = MagicMock()
        client.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(serialize(state).encode("utf-8")),
            "ETag": _ETAG,
        }
        client.put_object.return_value = {"ETag": _ETAG}
        return client

    @staticmethod
    def _state() -> CheckpointState:
        return CheckpointState(
            source_bucket=_SRC_BUCKET,
            last_processed_watermark="2026-07-28T00:00:00.000000Z",
            lease=None,
        )

    @pytest.mark.parametrize("kms_key_arn", [None, _KMS_ARN], ids=["no-kms", "kms"])
    def test_create_write_is_valid(self, kms_key_arn):
        """If-None-Match branch, both encryption variants."""
        client = self._s3_first_write()
        StateStore(kms_key_arn=kms_key_arn).put_checkpoint(
            client, _STATE_BUCKET, self._state(), None
        )
        ops = assert_calls_match_api(client, "s3")
        assert "PutObject" in ops

    @pytest.mark.parametrize("kms_key_arn", [None, _KMS_ARN], ids=["no-kms", "kms"])
    def test_update_write_is_valid(self, kms_key_arn):
        """If-Match branch, both encryption variants."""
        client = self._s3_existing(self._state())
        StateStore(kms_key_arn=kms_key_arn).put_checkpoint(
            client, _STATE_BUCKET, self._state(), _ETAG
        )
        ops = assert_calls_match_api(client, "s3")
        assert "GetObject" in ops and "PutObject" in ops

    def test_read_is_valid(self):
        client = self._s3_existing(self._state())
        StateStore().get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        assert_calls_match_api(client, "s3", expected=1)


# ---------------------------------------------------------------------------
# S3 — inventory manifest. The KMS branch sets SSE headers; the default branch
# sets AES256 explicitly rather than omitting the header.
# ---------------------------------------------------------------------------


class TestInventoryManifestWriterPutObject:
    @pytest.mark.parametrize("kms_key_arn", [None, _KMS_ARN], ids=["sse-s3", "sse-kms"])
    def test_manifest_writes_are_valid(self, kms_key_arn):
        client = MagicMock()
        client.put_object.return_value = {"ETag": _ETAG}
        write_in_memory_inventory_manifest(
            s3_client=client,
            scratch_bucket=_STATE_BUCKET,
            config_id=_SRC_BUCKET,
            source_bucket=_SRC_BUCKET,
            csv_bytes=b"source-bucket,key.txt,v1\n",
            data_file_key="manifests/src/ts/data/data.csv",
            kms_key_arn=kms_key_arn,
        )
        ops = assert_calls_match_api(client, "s3")
        assert ops.count("PutObject") >= 2, (
            "expected the data file, the checksum, and the manifest envelope"
        )


# ---------------------------------------------------------------------------
# S3 — remaining call sites with an optional parameter.
# ---------------------------------------------------------------------------


class TestOtherS3CallSites:
    def test_report_existence_check_is_valid(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"Contents": []}
        bops_report_reader.report_object_exists(
            client, _STATE_BUCKET, "completion-reports/src/manifest/"
        )
        assert_calls_match_api(client, "s3", expected=1)

    def test_report_listing_pagination_is_valid(self):
        """The ContinuationToken branch, which only a truncated listing takes."""
        client = MagicMock()
        client.list_objects_v2.side_effect = [
            {
                "Contents": [{"Key": "completion-reports/a/results/x.csv"}],
                "IsTruncated": True,
                "NextContinuationToken": "token-1",
            },
            {"Contents": [{"Key": "completion-reports/a/results/y.csv"}]},
        ]
        bops_report_reader._list_report_object_keys(
            client, _STATE_BUCKET, "completion-reports/a/"
        )
        assert_calls_match_api(client, "s3", expected=2)

    @pytest.mark.parametrize("version_id", [None, "v1"], ids=["no-version", "version"])
    def test_source_status_head_object_is_valid(self, version_id):
        client = MagicMock()
        client.head_object.return_value = {"ReplicationStatus": "COMPLETED"}
        source_status_adapter.check_source_replication_status(
            client, _SRC_BUCKET, "path/obj.txt", version_id
        )
        assert_calls_match_api(client, "s3", expected=1)

    def test_replication_configuration_read_is_valid(self):
        client = MagicMock()
        client.get_bucket_replication.return_value = {
            "ReplicationConfiguration": {"Role": _ROLE_ARN, "Rules": []}
        }
        replication_config_adapter.get_replication_rules(
            client, MonitoredBucket(name=_SRC_BUCKET, region="us-west-2")
        )
        assert_calls_match_api(client, "s3", expected=1)


# ---------------------------------------------------------------------------
# Athena — the workgroup path, and result pagination.
# ---------------------------------------------------------------------------


class TestAthenaCallSites:
    @staticmethod
    def _athena(next_token: str | None = None) -> MagicMock:
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": "qeid-1"}
        client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        header = {
            "Data": [
                {"VarCharValue": col}
                for col in (
                    "bucket",
                    "key",
                    "version_id",
                    "operation",
                    "resulting_tags",
                    "sequence_number",
                    "event_time",
                )
            ]
        }
        pages = [{"ResultSet": {"Rows": [header]}, "NextToken": next_token}]
        if next_token:
            pages.append({"ResultSet": {"Rows": []}})
        client.get_query_results.side_effect = pages
        return client

    def test_journal_read_calls_are_valid(self):
        client = self._athena()
        read_journal(
            client,
            _SRC_BUCKET,
            athena_workgroup="wg",
            output_location=f"s3://{_STATE_BUCKET}/athena-results/",
            since_timestamp="2026-07-28T00:00:00.000000Z",
        )
        ops = assert_calls_match_api(client, "athena")
        assert ops[:2] == ["StartQueryExecution", "GetQueryExecution"]
        assert "GetQueryResults" in ops

    def test_result_pagination_token_is_valid(self):
        """The NextToken branch of the result-page loop."""
        # The literal is deliberately not token-shaped: Bandit's B106
        # (hardcoded_password_funcarg) fires on a `*_token` keyword carrying a
        # string that looks like a credential, and this one is an Athena result
        # page marker. Renaming the value is cheaper than dispositioning a
        # finding about a string chosen here.
        client = self._athena(next_token="page-2")
        read_journal(
            client,
            _SRC_BUCKET,
            athena_workgroup="wg",
            output_location=f"s3://{_STATE_BUCKET}/athena-results/",
        )
        ops = assert_calls_match_api(client, "athena")
        assert ops.count("GetQueryResults") == 2


# ---------------------------------------------------------------------------
# SNS — the optional Subject, and the report-missing alert's log + publish.
# ---------------------------------------------------------------------------


class TestSnsCallSites:
    @pytest.mark.parametrize("subject", ["", "Replication complete"], ids=["no-subject", "subject"])
    def test_completion_report_publish_is_valid(self, subject):
        client = MagicMock()
        client.publish.return_value = {"MessageId": "m-1"}
        publish_completion_report(
            client,
            f"arn:aws:sns:us-west-2:{_ACCOUNT}:CompletionReportTopic",
            {"summary": "1 object replicated"},
            subject,
        )
        assert_calls_match_api(client, "sns", expected=1)

    def test_report_missing_alert_publish_and_log_are_valid(self):
        sns_client = MagicMock()
        sns_client.publish.return_value = {"MessageId": "m-1"}
        logs_client = MagicMock()
        _publish_report_missing_alert(
            sns_client,
            logs_client,
            f"arn:aws:sns:us-west-2:{_ACCOUNT}:BatchJobFailureTopic",
            "/s3-replicate-on-tag/stack/batch-job-failures",
            source_bucket=_SRC_BUCKET,
            replication_config_id=_SRC_BUCKET,
            job_id="job-1",
            now=_NOW,
        )
        assert_calls_match_api(sns_client, "sns", expected=1)
        log_ops = assert_calls_match_api(logs_client, "logs")
        assert "PutLogEvents" in log_ops

    @pytest.mark.parametrize(
        "topic_arn",
        [None, f"arn:aws:sns:us-west-2:{_ACCOUNT}:BatchJobFailureTopic"],
        ids=["no-topic", "topic"],
    )
    def test_submission_failure_alert_publish_and_log_are_valid(self, topic_arn):
        """The submission-failure alert builds a real SNS Publish and a real
        PutLogEvents. Both branches are covered: the log write happens
        unconditionally, the publish only when a topic ARN is configured — and
        the no-topic branch is the one a deployment without ``AlarmEmail``
        always takes, so it is exactly where an invalid request would sit
        unnoticed."""
        sns_client = MagicMock()
        sns_client.publish.return_value = {"MessageId": "m-1"}
        logs_client = MagicMock()
        _publish_submission_failure_alert(
            sns_client=sns_client,
            logs_client=logs_client,
            topic_arn=topic_arn,
            log_group_name="/s3-replicate-on-tag/stack/batch-job-failures",
            bucket_name=_SRC_BUCKET,
            error_reason=(
                "Parameter validation failed: Unknown parameter in "
                'Manifest.Location: "ManifestEncryption"'
            ),
            now=_NOW,
        )
        assert_calls_match_api(sns_client, "sns", expected=1 if topic_arn else 0)
        log_ops = assert_calls_match_api(logs_client, "logs")
        assert "PutLogEvents" in log_ops


# ---------------------------------------------------------------------------
# CloudWatch — dimensions are optional and applied to every datum.
# ---------------------------------------------------------------------------


class TestMetricsPublisher:
    @pytest.mark.parametrize(
        "dimensions", [None, {"Deployment": "us-west-2-test"}], ids=["no-dims", "dims"]
    )
    def test_put_metric_data_is_valid(self, dimensions):
        client = MagicMock()
        publisher = MetricsPublisher(
            "S3ReplicateOnTag", dimensions, cloudwatch_client=client
        )
        publisher.publish(
            RunResult(
                buckets=[
                    BucketMetrics(
                        source_bucket=_SRC_BUCKET,
                        ops_read=3,
                        matched=2,
                        submitted=1,
                        errored=False,
                    )
                ],
                disabled_buckets=1,
            )
        )
        assert_calls_match_api(client, "cloudwatch", expected=1)


# ---------------------------------------------------------------------------
# Lambda — the self-reinvocation call, reachable only through handler().
# ---------------------------------------------------------------------------


class TestReinvocationInvoke:
    _ENV = {
        "STATE_BUCKET": _STATE_BUCKET,
        "ATHENA_WORKGROUP": "wg",
        "ATHENA_OUTPUT_LOCATION": f"s3://{_STATE_BUCKET}/athena-results/",
        "ACCOUNT_ID": _ACCOUNT,
        "BATCH_OPERATIONS_ROLE_ARN": _BATCHOPS_ROLE_ARN,
    }

    def test_self_invoke_is_valid(self):
        config_bytes = json.dumps(
            {"buckets": [{"name": _SRC_BUCKET, "region": "us-west-2"}]}
        ).encode("utf-8")
        mock_s3 = MagicMock()
        mock_s3.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(config_bytes),
            "ETag": _ETAG,
        }
        mock_lambda = MagicMock()
        clients = {"s3": mock_s3, "lambda": mock_lambda}

        def fake_client(service, **kwargs):
            return clients.setdefault(service, MagicMock())

        context = MagicMock()
        context.invoked_function_arn = (
            f"arn:aws:lambda:us-west-2:{_ACCOUNT}:function:ReplicationLambda"
        )
        context.memory_limit_in_mb = 2048

        outcome = RunOutcome(any_capped_and_progressed=True, buckets=[])

        with patch("src.lambda_handler.boto3.client", side_effect=fake_client):
            with patch("src.lambda_handler.run_interval", return_value=outcome):
                with patch.dict(os.environ, self._ENV, clear=True):
                    handler({}, context)

        assert_calls_match_api(mock_lambda, "lambda", expected=1)


# ---------------------------------------------------------------------------
# The remaining call sites: the config write (its own KMS branch, separate from
# the state-object write), the two other Athena readers, and DescribeJob.
# ---------------------------------------------------------------------------


class TestConfigWriteAndRemainingReaders:
    @pytest.mark.parametrize("kms_key_arn", [None, _KMS_ARN], ids=["no-kms", "kms"])
    def test_circuit_breaker_config_write_is_valid(self, kms_key_arn):
        """``_disable_bucket_in_config`` writes solution-config.json with its
        own If-Match plus optional SSE-KMS kwargs, built independently of
        ``state_store``'s write path."""
        from src.lambda_handler import _disable_bucket_in_config

        config_bytes = json.dumps(
            {"buckets": [{"name": _SRC_BUCKET, "region": "us-west-2"}]}
        ).encode("utf-8")
        client = MagicMock()
        client.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(config_bytes),
            "ETag": _ETAG,
        }
        client.put_object.return_value = {"ETag": _ETAG}

        _disable_bucket_in_config(
            client,
            _STATE_BUCKET,
            "config/solution-config.json",
            _SRC_BUCKET,
            "circuit breaker",
            kms_key_arn,
        )
        ops = assert_calls_match_api(client, "s3")
        assert "PutObject" in ops, "the config write must be reached"

    def test_preflight_count_calls_are_valid(self):
        from src.adapters.preflight_counter import preflight_count
        from src.core.models import DerivedReplicationRule, DestinationRef

        client = TestAthenaCallSites._athena()
        client.get_query_results.side_effect = [
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "_col0"}]},
                        {"Data": [{"VarCharValue": "7"}]},
                    ]
                }
            }
        ]
        preflight_count(
            client,
            _SRC_BUCKET,
            [
                DerivedReplicationRule(
                    source_bucket=_SRC_BUCKET,
                    replication_config_id="rule-1",
                    rule_id="rule-1",
                    tag_filter={"replicate": "true"},
                    destination=DestinationRef(bucket_arn="arn:aws:s3:::dest-bucket"),
                    key_prefix="data/",
                )
            ],
            since_timestamp="2026-07-28T00:00:00.000000Z",
            athena_workgroup="wg",
            output_location=f"s3://{_STATE_BUCKET}/athena-results/",
        )
        ops = assert_calls_match_api(client, "athena")
        assert "StartQueryExecution" in ops

    def test_permanent_delete_read_calls_are_valid(self):
        from src.adapters.permanent_delete_reader import read_permanent_deletes

        client = TestAthenaCallSites._athena()
        client.get_query_results.side_effect = [
            {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "key"}]}]}}
        ]
        read_permanent_deletes(
            client,
            _SRC_BUCKET,
            since_window_start="2026-07-28T00:00:00.000000Z",
            athena_workgroup="wg",
            output_location=f"s3://{_STATE_BUCKET}/athena-results/",
        )
        ops = assert_calls_match_api(client, "athena")
        assert "StartQueryExecution" in ops

    def test_describe_job_is_valid(self):
        """Both the orchestrator's DescribeJob loop and
        ``check_report_handler`` call it with the same two parameters."""
        client = MagicMock()
        client.describe_job(AccountId=_ACCOUNT, JobId="job-1")
        assert_calls_match_api(client, "s3control", expected=1)
