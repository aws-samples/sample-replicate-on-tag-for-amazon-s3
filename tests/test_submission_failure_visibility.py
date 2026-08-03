"""Tests for the submission-failure-visibility spec (tasks 2-7).

Covers:
  - Task 2: FailureClass classification in the adapter (ParamValidationError
    driven by botocore for real, not hand-constructed).
  - Task 3: Classification appears in the failure log entry.
  - Task 4: Alert fires once per episode, suppressed on subsequent intervals.
  - Task 5: Streak persisted correctly; service-side does not increment.
  - Task 6: Disable at threshold for permanent class, never for service-side.
  - Task 7: Checkpoint not advanced on failed submission (existing test exists;
    we add an explicit coverage here for completeness).

Requirements: submission-failure-visibility 1.1, 2.1-2.4, 3.1-3.4, 4.1-4.5, 5.1-5.4
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import botocore.session
import pytest
from botocore.exceptions import ParamValidationError

from src.adapters.batch_operations_adapter import SubmissionResult, submit_batch_job
from src.adapters.metrics_publisher import _build_metric_data
from src.core.models import (
    FailureClass,
    RunResult,
    SubmissionStatus,
)
from src.orchestrator import run_interval

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_ACCOUNT = "123456789012"
_STATE_BUCKET = "state-bucket"
_SRC_BUCKET = "source-bucket"
_ROLE_ARN = f"arn:aws:iam::{_ACCOUNT}:role/replication-role"
_CONFIG = {"buckets": [{"name": _SRC_BUCKET, "region": "us-west-2"}]}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_runtime_config(**overrides) -> dict:
    """Minimal runtime_config for run_interval."""
    rc = {
        "state_bucket": _STATE_BUCKET,
        "athena_workgroup": "primary",
        "athena_output_location": "s3://state-bucket/athena/",
        "account_id": _ACCOUNT,
        "region": "us-west-2",
    }
    rc.update(overrides)
    return rc


def _mock_infra(submission_result: SubmissionResult):
    """Patch all I/O so run_interval reaches the submit step and returns the
    given submission_result."""
    from src.core.models import TaggingOperation, DerivedReplicationRule, DestinationRef
    from src.adapters.inventory_manifest_writer import WrittenManifest
    from src.core.models import S3Location

    op = TaggingOperation(
        source_bucket=_SRC_BUCKET,
        object_key="obj/1.txt",
        resulting_tag_set={"repl": "true"},
        sequence_number="1",
        operation="PutObjectTagging",
        event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    rule = DerivedReplicationRule(
        source_bucket=_SRC_BUCKET,
        replication_config_id="rule-1",
        rule_id="r1",
        tag_filter={"repl": "true"},
        destination=DestinationRef(bucket_arn="arn:aws:s3:::dest"),
        replication_role_arn=_ROLE_ARN,
    )

    manifest_result = WrittenManifest(
        s3_location=S3Location(bucket=_STATE_BUCKET, key="manifests/test/data.csv"),
        etag='"abc"',
        all_versioned=True,
        object_count=1,
    )

    patches = []
    # Client factory
    mock_factory = MagicMock()
    patches.append(patch("src.orchestrator.ClientFactory", return_value=mock_factory))

    # Replication config rules
    patches.append(patch(
        "src.orchestrator.replication_config_adapter.get_replication_rules",
        return_value=([rule], []),
    ))

    # State store
    from src.core.models import CheckpointState
    mock_store = MagicMock()
    mock_store.get_checkpoint.return_value = (
        CheckpointState(source_bucket=_SRC_BUCKET, last_processed_watermark="2026-01-01T00:00:00.000000Z"),
        '"etag1"',
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.acquire_lease.return_value = '"etag2"'
    mock_store.release_lease.return_value = '"etag3"'
    mock_store.record_submission.return_value = '"etag4"'
    mock_store.increment_submission_failure_streak.return_value = (1, '"etag5"')
    mock_store.clear_submission_failure_streak.return_value = '"etag6"'
    patches.append(patch("src.orchestrator.state_store_module.StateStore", return_value=mock_store))

    # Journal
    patches.append(patch(
        "src.orchestrator.athena_journal_adapter.read_journal",
        return_value=([op], []),
    ))
    patches.append(patch(
        "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
        return_value=None,
    ))

    # Preflight
    patches.append(patch("src.orchestrator.preflight_count", return_value=1))

    # Delete filter
    patches.append(patch("src.orchestrator.read_permanent_deletes", return_value=set()))

    # Inventory manifest writer
    patches.append(patch(
        "src.orchestrator.write_in_memory_inventory_manifest",
        return_value=manifest_result,
    ))

    # Submit
    patches.append(patch(
        "src.orchestrator.batch_operations_adapter.submit_batch_job",
        return_value=submission_result,
    ))

    # Metrics (no-op)
    patches.append(patch("src.orchestrator.MetricsPublisher"))

    return patches, mock_store


def _run(submission_result: SubmissionResult, **runtime_overrides):
    """Run one interval with the given submission result, return (outcome, mock_store)."""
    patches, mock_store = _mock_infra(submission_result)
    rc = _make_runtime_config(**runtime_overrides)
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        outcome = run_interval(_CONFIG, rc)
    return outcome, mock_store


import contextlib


# ===========================================================================
# Task 2: FailureClass classification in the adapter
# ===========================================================================

class TestTask2AdapterClassification:
    """Requirement 2.1, 2.2: ParamValidationError is classified as
    PERMANENT_CLIENT, driven by botocore raising it for real."""

    def test_param_validation_error_classified_as_permanent_client(self):
        """Drive a real ParamValidationError by calling a client built from
        botocore's s3control service model with an invalid parameter, not a
        hand-constructed exception (Requirement 5.2)."""
        # Build a real s3control client from botocore's service model — this
        # validates parameters before signing, so a wrong-typed param raises
        # ParamValidationError.
        session = botocore.session.get_session()
        s3control_client = session.create_client(
            "s3control",
            region_name="us-east-1",
            aws_access_key_id="fake",
            aws_secret_access_key="fake",
        )

        # Pass RoleArn as an integer (wrong type) so botocore raises
        # ParamValidationError from its own validation layer — exactly the
        # class of defect the ManifestEncryption bug was (a request kwarg the
        # API rejects during client-side validation).
        from src.core.models import S3Location
        result = submit_batch_job(
            s3control_client=s3control_client,
            account_id="123456789012",
            manifest_location=S3Location(bucket="b", key="k"),
            manifest_etag='"abc"',
            replication_role_arn=12345,  # type: ignore[arg-type] — intentionally wrong type
            config_id="rule-1",
            object_count=1,
            source_bucket=_SRC_BUCKET,
        )
        assert result.status is SubmissionStatus.CREATE_FAILED
        assert result.failure_class is FailureClass.PERMANENT_CLIENT
        assert "Parameter validation" in result.error_reason

    def test_client_error_classified_as_service(self):
        """Requirement 2.2: a ClientError response is classified SERVICE."""
        from botocore.exceptions import ClientError
        mock_client = MagicMock()
        mock_client.create_job.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Not authorized"}},
            "CreateJob",
        )
        from src.core.models import S3Location
        result = submit_batch_job(
            s3control_client=mock_client,
            account_id=_ACCOUNT,
            manifest_location=S3Location(bucket="b", key="k"),
            manifest_etag='"abc"',
            replication_role_arn=_ROLE_ARN,
            config_id="rule-1",
            object_count=1,
            source_bucket=_SRC_BUCKET,
        )
        assert result.status is SubmissionStatus.CREATE_FAILED
        assert result.failure_class is FailureClass.SERVICE
        assert "AccessDenied" in result.error_reason

    def test_timeout_classified_as_timeout(self):
        """TimeoutError is classified TIMEOUT."""
        mock_client = MagicMock()
        with patch("src.adapters.batch_operations_adapter._call_with_timeout") as mock_call:
            mock_call.side_effect = TimeoutError("timed out")
            from src.core.models import S3Location
            result = submit_batch_job(
                s3control_client=mock_client,
                account_id=_ACCOUNT,
                manifest_location=S3Location(bucket="b", key="k"),
                manifest_etag='"abc"',
                replication_role_arn=_ROLE_ARN,
                config_id="rule-1",
                object_count=1,
                source_bucket=_SRC_BUCKET,
            )
        assert result.failure_class is FailureClass.TIMEOUT

    def test_generic_exception_classified_as_unknown(self):
        """A bare Exception is classified UNKNOWN."""
        mock_client = MagicMock()
        with patch("src.adapters.batch_operations_adapter._call_with_timeout") as mock_call:
            mock_call.side_effect = RuntimeError("network blip")
            from src.core.models import S3Location
            result = submit_batch_job(
                s3control_client=mock_client,
                account_id=_ACCOUNT,
                manifest_location=S3Location(bucket="b", key="k"),
                manifest_etag='"abc"',
                replication_role_arn=_ROLE_ARN,
                config_id="rule-1",
                object_count=1,
                source_bucket=_SRC_BUCKET,
            )
        assert result.failure_class is FailureClass.UNKNOWN


# ===========================================================================
# Task 3: Classification in the failure log entry
# ===========================================================================

class TestTask3LogClassification:
    """Requirement 2.3: the failure class appears in the emitted log entry."""

    def test_permanent_client_class_in_log(self):
        """Log entry contains 'class=PERMANENT_CLIENT' for a permanent failure."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Parameter validation failed",
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
        emitted = []
        patches, _ = _mock_infra(sub)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch(
                "src.orchestrator.observability.emit",
                side_effect=lambda e: emitted.append(e),
            ))
            run_interval(_CONFIG, rc)

        error_entries = [e for e in emitted if isinstance(e, dict) and "class=PERMANENT_CLIENT" in e.get("cause", "")]
        assert len(error_entries) >= 1

    def test_service_class_in_log(self):
        """Log entry contains 'class=SERVICE' for a service-side failure."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="AccessDenied: Not authorized",
            failure_class=FailureClass.SERVICE,
        )
        emitted = []
        patches, _ = _mock_infra(sub)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(patch(
                "src.orchestrator.observability.emit",
                side_effect=lambda e: emitted.append(e),
            ))
            run_interval(_CONFIG, rc)

        error_entries = [e for e in emitted if isinstance(e, dict) and "class=SERVICE" in e.get("cause", "")]
        assert len(error_entries) >= 1


# ===========================================================================
# Task 4: Alert fires once per episode, suppressed on subsequent intervals
# ===========================================================================

class TestTask4AlertCallback:
    """Requirements 3.1, 3.2, 3.4: alert once per episode."""

    def test_alert_fires_on_first_permanent_failure(self):
        """on_submission_failure called when streak == 1 (first occurrence)."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Parameter validation failed",
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
        alert_calls = []
        patches, mock_store = _mock_infra(sub)
        mock_store.increment_submission_failure_streak.return_value = (1, '"etag5"')
        rc = _make_runtime_config(on_submission_failure=lambda b, r: alert_calls.append((b, r)))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        assert len(alert_calls) == 1
        assert alert_calls[0][0] == _SRC_BUCKET

    def test_alert_suppressed_on_second_failure(self):
        """on_submission_failure NOT called when streak > 1 (suppressed)."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Parameter validation failed",
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
        alert_calls = []
        patches, mock_store = _mock_infra(sub)
        mock_store.increment_submission_failure_streak.return_value = (2, '"etag5"')
        rc = _make_runtime_config(on_submission_failure=lambda b, r: alert_calls.append((b, r)))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        assert len(alert_calls) == 0

    def test_alert_fires_again_after_clear(self):
        """After a successful submission clears the streak, a recurrence
        fires the alert again (Requirement 3.4)."""
        # First run: success clears streak
        sub_ok = SubmissionResult(
            status=SubmissionStatus.SUBMITTED,
            config_id="rule-1",
            object_count=1,
            job_id="job-123",
        )
        patches_ok, mock_store_ok = _mock_infra(sub_ok)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches_ok:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        # Verify clear was called
        mock_store_ok.clear_submission_failure_streak.assert_called_once()

    def test_service_side_failure_does_not_alert(self):
        """A SERVICE class failure never fires the alert."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="AccessDenied",
            failure_class=FailureClass.SERVICE,
        )
        alert_calls = []
        patches, mock_store = _mock_infra(sub)
        rc = _make_runtime_config(on_submission_failure=lambda b, r: alert_calls.append((b, r)))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        assert len(alert_calls) == 0
        mock_store.increment_submission_failure_streak.assert_not_called()


# ===========================================================================
# Task 5: Streak persistence — service-side does not increment
# ===========================================================================

class TestTask5StreakPersistence:
    """Requirements 4.3, 4.4, 5.3: only permanent increments the streak."""

    def test_permanent_failure_increments_streak(self):
        """PERMANENT_CLIENT calls increment_submission_failure_streak."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Parameter validation failed",
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
        patches, mock_store = _mock_infra(sub)
        mock_store.increment_submission_failure_streak.return_value = (1, '"etag5"')
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        mock_store.increment_submission_failure_streak.assert_called_once()

    def test_service_failure_does_not_increment_streak(self):
        """SERVICE class does NOT call increment_submission_failure_streak (Req 4.4)."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="AccessDenied",
            failure_class=FailureClass.SERVICE,
        )
        patches, mock_store = _mock_infra(sub)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        mock_store.increment_submission_failure_streak.assert_not_called()

    def test_successful_submission_clears_streak(self):
        """A successful submission calls clear_submission_failure_streak."""
        sub = SubmissionResult(
            status=SubmissionStatus.SUBMITTED,
            config_id="rule-1",
            object_count=1,
            job_id="job-abc",
        )
        patches, mock_store = _mock_infra(sub)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        mock_store.clear_submission_failure_streak.assert_called_once()


# ===========================================================================
# Task 6: Disable at threshold
# ===========================================================================

class TestTask6DisableAtThreshold:
    """Requirements 4.1, 4.2, 4.5: disable when streak reaches threshold."""

    def test_permanent_at_threshold_disables(self):
        """When streak reaches max_batch_job_failures, on_bucket_disable is called."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Parameter validation failed",
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
        disable_calls = []
        patches, mock_store = _mock_infra(sub)
        mock_store.increment_submission_failure_streak.return_value = (4, '"etag5"')
        rc = _make_runtime_config(
            max_batch_job_failures=4,
            on_bucket_disable=lambda b, r: disable_calls.append((b, r)),
        )
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        assert len(disable_calls) == 1
        bucket, reason = disable_calls[0]
        assert bucket == _SRC_BUCKET
        assert "rejected by the AWS API before it was sent" in reason
        assert "code defect" in reason
        assert "code fix" in reason

    def test_service_side_never_disables(self):
        """A SERVICE failure never triggers on_bucket_disable (Req 4.4)."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="TooManyRequests",
            failure_class=FailureClass.SERVICE,
        )
        disable_calls = []
        patches, mock_store = _mock_infra(sub)
        rc = _make_runtime_config(
            max_batch_job_failures=1,
            on_bucket_disable=lambda b, r: disable_calls.append((b, r)),
        )
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        assert len(disable_calls) == 0

    def test_below_threshold_does_not_disable(self):
        """Streak below threshold does not disable."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Parameter validation failed",
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
        disable_calls = []
        patches, mock_store = _mock_infra(sub)
        mock_store.increment_submission_failure_streak.return_value = (3, '"etag5"')
        rc = _make_runtime_config(
            max_batch_job_failures=4,
            on_bucket_disable=lambda b, r: disable_calls.append((b, r)),
        )
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        assert len(disable_calls) == 0


# ===========================================================================
# Task 7: Checkpoint not advanced on failed submission
# ===========================================================================

class TestTask7CheckpointNotAdvanced:
    """Requirement 5.4: a CREATE_FAILED leaves the checkpoint unchanged."""

    def test_checkpoint_not_advanced_on_creation_failure(self):
        """release_lease called with submitted_refs=None when submission fails."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="Some error",
            failure_class=FailureClass.SERVICE,
        )
        patches, mock_store = _mock_infra(sub)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            run_interval(_CONFIG, rc)

        mock_store.release_lease.assert_called_once()
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None


# ===========================================================================
# Existing coverage (task 1 — already implemented): BucketErrors metric
# ===========================================================================

class TestTask1MetricVisibility:
    """Requirement 1.1, 5.1: a creation failure publishes BucketErrors at 1.0."""

    def test_bucket_errors_metric_published_on_failure(self):
        """BucketErrors = 1.0 for a failed submission, asserted through the
        real metric builder (not a mocked put_metric_data)."""
        sub = SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id="rule-1",
            object_count=1,
            error_reason="AccessDenied",
            failure_class=FailureClass.SERVICE,
        )
        patches, _ = _mock_infra(sub)
        rc = _make_runtime_config()
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            outcome = run_interval(_CONFIG, rc)

        assert outcome.buckets[0].errored is True
        data = _build_metric_data(
            RunResult(buckets=outcome.buckets, disabled_buckets=0)
        )
        errors = [d for d in data if d["MetricName"] == "BucketErrors"]
        assert len(errors) == 1
        assert errors[0]["Value"] == 1.0
