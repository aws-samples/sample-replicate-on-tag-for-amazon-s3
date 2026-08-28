"""A lost submission record is a visible failure, not a silent one.

``_persist_submission`` writes the ``SubmissionRecord`` for a job that has
already been submitted, and it runs after ``_lease_scope``'s ``finally`` has
released the lease and advanced the checkpoint. That ordering is deliberate and
is accepted rather than fixed (``f-ba79a9a8``,
scan-aa27a832-remediation Requirement 11.3), which is exactly what makes the
failure worth a metric: the job is billed, the watermark has already moved past
the range it covers, and no rollback is available. The record is what every
later run reads to poll the job, prune settled records, and count outstanding
work, so losing it means the job is never tracked to completion.

The handler caught every exception, logged, and returned. ``BucketErrors`` read
zero for a run that had leaked a job.

Feature: scan-aa27a832-remediation, task 6.3 (Requirement 6.2)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.adapters.batch_operations_adapter import SubmissionResult
from src.adapters.inventory_manifest_writer import WrittenManifest
from src.adapters.state_store import ConditionalWriteError
from src.core.models import (
    CheckpointState,
    DerivedReplicationRule,
    DestinationRef,
    RunResult,
    S3Location,
    SubmissionStatus,
    TaggingOperation,
)
from src.orchestrator import run_interval
from tests.support import mock_state_store

_NOW = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
_BUCKET = "my-bucket"
_STATE_BUCKET = "scratch-state-bucket"
_ACCOUNT_ID = "123456789012"

_RUNTIME = {
    "state_bucket": _STATE_BUCKET,
    "athena_workgroup": "primary",
    "athena_output_location": f"s3://{_STATE_BUCKET}/athena/",
    "account_id": _ACCOUNT_ID,
    "batch_operations_role_arn": (
        "arn:aws:iam::123456789012:role/s3rot-batch-operations-role"
    ),
    "region": "us-east-1",
}


def _config() -> dict:
    return {"buckets": [{"name": _BUCKET, "region": "us-east-1"}]}


def _rule() -> DerivedReplicationRule:
    return DerivedReplicationRule(
        source_bucket=_BUCKET,
        replication_config_id="rule-1",
        rule_id="rule-1",
        tag_filter={"env": "prod"},
        destination=DestinationRef(bucket_arn="arn:aws:s3:::dest-bucket"),
    )


def _op() -> TaggingOperation:
    return TaggingOperation(
        source_bucket=_BUCKET,
        object_key="path/obj.txt",
        resulting_tag_set={"env": "prod"},
        sequence_number="seq-001",
        operation="PutObjectTagging",
        event_time=_NOW,
    )


def _run(*, record_submission_side_effect=None):
    """Drive one clean single-bucket interval through to the persist step.

    Everything up to and including ``submit_batch_job`` succeeds, so the run
    reaches ``_persist_submission`` with a record to write and a lease that
    released cleanly. ``record_submission_side_effect`` is the only injected
    failure.

    Returns ``(outcome, mock_store, call_order)``.
    """
    call_order: list[str] = []

    mock_factory_cls = MagicMock()
    mock_factory_cls.return_value = MagicMock()

    mock_store_cls = MagicMock()
    mock_store = mock_state_store()
    mock_store_cls.return_value = mock_store
    mock_store.get_checkpoint.side_effect = lambda c, sb, name: (
        CheckpointState(source_bucket=name, last_processed_watermark="", lease=None),
        '"etag-0"',
    )
    mock_store.get_submission_records.side_effect = lambda c, sb, name: {}
    mock_store.acquire_lease.return_value = '"etag-1"'

    def release_lease(*args, **kwargs):
        call_order.append("release_lease")
        return '"etag-2"'

    def record_submission(*args, **kwargs):
        call_order.append("record_submission")
        if record_submission_side_effect is not None:
            raise record_submission_side_effect
        return '"etag-3"'

    mock_store.release_lease.side_effect = release_lease
    mock_store.record_submission.side_effect = record_submission

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            side_effect=lambda c, bucket: ([_rule()], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op()], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=WrittenManifest(
                s3_location=S3Location(
                    bucket=_STATE_BUCKET, key="manifests/rule-1/ts.csv"
                ),
                etag="abc123",
                object_count=1,
            ),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=SubmissionResult(
                status=SubmissionStatus.SUBMITTED,
                config_id="rule-1",
                object_count=1,
                job_id="job-abc-123",
            ),
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
    ):
        outcome = run_interval(_config(), _RUNTIME)

    return outcome, mock_store, call_order


def _bucket_metrics(outcome):
    assert len(outcome.buckets) == 1
    return outcome.buckets[0]


class TestPersistFailureIsReportedAsABucketError:
    def test_the_bucket_is_marked_errored(self):
        """ConditionalWriteError is the plausible trigger: the store is
        configured with no retries and the five-minute completion handler writes
        the same object, so ETag contention is what this most often looks like.
        """
        outcome, mock_store, _ = _run(
            record_submission_side_effect=ConditionalWriteError("ETag mismatch"),
        )

        assert mock_store.record_submission.call_count == 1
        assert _bucket_metrics(outcome).errored is True

    def test_any_exception_counts_not_only_a_conditional_write(self):
        outcome, _, _ = _run(
            record_submission_side_effect=RuntimeError("S3 unavailable"),
        )

        assert _bucket_metrics(outcome).errored is True

    def test_the_bucket_errors_metric_reads_one(self):
        from src.adapters.metrics_publisher import _build_metric_data

        outcome, _, _ = _run(
            record_submission_side_effect=RuntimeError("S3 unavailable"),
        )

        data = _build_metric_data(
            RunResult(buckets=outcome.buckets, disabled_buckets=0)
        )
        errors = [d for d in data if d["MetricName"] == "BucketErrors"]
        assert [d["Value"] for d in errors] == [1.0]

    def test_the_failure_is_logged_and_does_not_propagate(self, caplog):
        """Non-fatal is unchanged: the run still completes and still reports the
        submission it made."""
        with caplog.at_level(logging.ERROR):
            outcome, _, _ = _run(
                record_submission_side_effect=RuntimeError("S3 unavailable"),
            )

        messages = [r.message for r in caplog.records]
        assert any(
            "Failed to persist submission record" in msg for msg in messages
        ), messages
        assert _bucket_metrics(outcome).submitted == 1


class TestACleanPersistIsNotAnError:
    """Non-vacuous companion: the metric must distinguish the two runs."""

    def test_the_bucket_is_not_marked_errored(self):
        outcome, mock_store, _ = _run()

        assert mock_store.record_submission.call_count == 1
        assert _bucket_metrics(outcome).errored is False
        assert _bucket_metrics(outcome).submitted == 1


class TestTheCheckpointAdvancesBeforeTheRecordIsWritten:
    """The accepted ordering, asserted so a later change to it is deliberate.

    ``release_lease`` advances the checkpoint, and it runs first. That is the
    whole reason a persist failure cannot be rolled back and has to be visible
    instead. Requirement 11.3 records the reverse ordering as the preferred
    remediation; if it is ever adopted, this test is the one that should fail.
    """

    def test_release_precedes_record_submission(self):
        _, _, call_order = _run()

        assert call_order == ["release_lease", "record_submission"]
