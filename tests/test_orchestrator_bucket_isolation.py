"""The per-bucket loop in ``run_interval`` isolates an unexpected exception.

``_process_bucket`` documents that "any skip or error is logged and the function
returns with partial counters, without raising". Every known failure inside it is
caught, but nothing enforced the contract at the call site, so one escape aborted
every bucket after the failing one — a fault in a single bucket's state object
silently stopped replication for all the others, and the run's ``BucketErrors``
data was never published at all because the metrics publish sits after the loop.

The known concrete trigger is a malformed ``watermark_low`` read back from a
state object. ``deserialize_submission_record`` performs no coercion on that
field, so a non-string value reaches ``plan_recovery``'s ``min(usable_lows)``
and raises ``TypeError`` there, outside any handler. That value is deliberately
*not* validated — see design R2, which dispositions the validation half under
FP15 — so the isolation is what makes it survivable.

Feature: scan-aa27a832-remediation, task 6.1 (Requirement 6.3)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src import orchestrator
from src.adapters.batch_operations_adapter import SubmissionResult
from src.adapters.inventory_manifest_writer import WrittenManifest
from src.core.models import (
    BucketDisableState,
    CheckpointState,
    DerivedReplicationRule,
    DestinationRef,
    RunResult,
    S3Location,
    SubmissionRecord,
    SubmissionStatus,
    TaggingOperation,
)
from src.orchestrator import run_interval
from tests.support import mock_state_store

_NOW = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
_STATE_BUCKET = "scratch-state-bucket"
_ACCOUNT_ID = "123456789012"
_BATCHOPS_ROLE_ARN = "arn:aws:iam::123456789012:role/s3rot-batch-operations-role"
_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:s3rot-completion"

_RUNTIME = {
    "state_bucket": _STATE_BUCKET,
    "athena_workgroup": "primary",
    "athena_output_location": f"s3://{_STATE_BUCKET}/athena/",
    "account_id": _ACCOUNT_ID,
    "batch_operations_role_arn": _BATCHOPS_ROLE_ARN,
    "region": "us-east-1",
}

# The bucket that fails is deliberately first, so a lost isolation guard costs
# the second bucket its whole interval rather than merely being untidy.
_BAD = "bucket-aa"
_GOOD = "bucket-bb"


def _config() -> dict:
    return {
        "buckets": [
            {"name": _BAD, "region": "us-east-1"},
            {"name": _GOOD, "region": "us-east-1"},
        ]
    }


def _rule(source_bucket: str) -> DerivedReplicationRule:
    return DerivedReplicationRule(
        source_bucket=source_bucket,
        replication_config_id="rule-1",
        rule_id="rule-1",
        tag_filter={"env": "prod"},
        destination=DestinationRef(bucket_arn="arn:aws:s3:::dest-bucket"),
    )


def _op(source_bucket: str) -> TaggingOperation:
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key="path/obj.txt",
        resulting_tag_set={"env": "prod"},
        sequence_number="seq-001",
        operation="PutObjectTagging",
        event_time=_NOW,
    )


def _record(job_id: str, watermark_low) -> SubmissionRecord:
    """A stored record as ``deserialize_submission_record`` would return it.

    ``watermark_low`` is annotated ``str`` but read with a bare ``data.get``, so
    whatever type the state object holds arrives here unchanged. Passing a
    non-string is the point of this helper, not an abuse of it.
    """
    return SubmissionRecord(
        replication_config_id="rule-1",
        source_bucket=_BAD,
        job_id=job_id,
        manifest_key=f"manifests/{job_id}/manifest.csv",
        submitted_at=_NOW - timedelta(hours=1),
        status=SubmissionStatus.SUBMITTED,
        watermark_low=watermark_low,
        watermark_high="2026-08-28T11:00:00.000000Z",
    )


def _run(
    *,
    submission_records: dict[str, dict[str, SubmissionRecord]] | None = None,
    raise_for: str | None = None,
    completion_topic: bool = False,
):
    """Drive a full ``run_interval`` over ``_BAD`` then ``_GOOD``.

    ``submission_records`` maps bucket name to the records its state object
    holds; every described job comes back ``Failed`` so recovery scoring runs.
    ``raise_for`` instead makes ``_process_bucket`` itself raise for that bucket,
    which covers the generic case without depending on any particular internal
    path staying unguarded.

    Returns ``(outcome, read_journal_mock, submit_mock, run_state)``.
    """
    records = submission_records or {}

    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory
    mock_factory.create_s3control_client.return_value.describe_job.return_value = {
        "Job": {
            "Status": "Failed",
            "CreationTime": _NOW - timedelta(hours=1),
            "ProgressSummary": {
                "NumberOfTasksSucceeded": 0,
                "NumberOfTasksFailed": 1,
            },
        }
    }

    mock_store_cls = MagicMock()
    mock_store = mock_state_store()
    mock_store_cls.return_value = mock_store
    mock_store.get_disable_state.return_value = BucketDisableState()
    mock_store.get_checkpoint.side_effect = lambda c, sb, name: (
        CheckpointState(source_bucket=name, last_processed_watermark="", lease=None),
        '"etag-0"',
    )
    mock_store.get_submission_records.side_effect = (
        lambda c, sb, name: records.get(name, {})
    )
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'

    mock_read_journal = MagicMock(
        side_effect=lambda athena_client, bucket_name, *a, **k: ([_op(bucket_name)], [])
    )
    mock_submit = MagicMock(
        side_effect=lambda **kw: SubmissionResult(
            status=SubmissionStatus.SUBMITTED,
            config_id=kw.get("config_id", "rule-1"),
            object_count=1,
            job_id="job-new",
        )
    )

    real_process_bucket = orchestrator._process_bucket

    def process_bucket(**kwargs):
        if raise_for is not None and kwargs["bucket"].name == raise_for:
            raise RuntimeError("unexpected fault inside _process_bucket")
        return real_process_bucket(**kwargs)

    captured_run_state: list[dict] = []

    def capture_tracking(**kwargs):
        captured_run_state.append(kwargs["run_state"])

    runtime = dict(_RUNTIME)
    if completion_topic:
        runtime["completion_report_topic_arn"] = _TOPIC_ARN

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            side_effect=lambda c, bucket: ([_rule(bucket.name)], []),
        ),
        patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
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
        patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator._process_bucket", side_effect=process_bucket),
        patch(
            "src.orchestrator._run_completion_tracking_interval",
            side_effect=capture_tracking,
        ),
    ):
        outcome = run_interval(_config(), runtime)

    run_state = captured_run_state[0] if captured_run_state else None
    return outcome, mock_read_journal, mock_submit, run_state


def _journal_buckets(mock_read_journal) -> set[str]:
    return {
        call.kwargs.get("bucket_name", call.args[1] if len(call.args) > 1 else "")
        for call in mock_read_journal.call_args_list
    }


def _metrics_for(outcome, bucket_name):
    return next(bm for bm in outcome.buckets if bm.source_bucket == bucket_name)


class TestAnUnexpectedExceptionIsolatesToOneBucket:
    """The generic case: whatever raises, the run continues."""

    def test_a_later_bucket_is_still_processed(self):
        outcome, read_journal, submit, _ = _run(raise_for=_BAD)

        assert _journal_buckets(read_journal) == {_GOOD}
        assert submit.call_count == 1
        assert submit.call_args.kwargs["source_bucket"] == _GOOD

    def test_the_raising_bucket_publishes_a_bucket_error(self):
        """Without this, the isolation would trade a loud abort for a silent
        loss: the failing bucket would move no metric and BucketErrors would
        read zero for a run that dropped a bucket entirely."""
        outcome, *_ = _run(raise_for=_BAD)

        bad = _metrics_for(outcome, _BAD)
        assert bad.errored is True
        assert (bad.ops_read, bad.matched, bad.submitted) == (0, 0, 0)

    def test_the_healthy_bucket_is_not_marked_errored(self):
        """Non-vacuous companion: the handler must not smear the failure across
        the run."""
        outcome, *_ = _run(raise_for=_BAD)

        assert _metrics_for(outcome, _GOOD).errored is False

    def test_the_bucket_errors_metric_counts_exactly_one_bucket(self):
        from src.adapters.metrics_publisher import _build_metric_data

        outcome, *_ = _run(raise_for=_BAD)

        data = _build_metric_data(
            RunResult(buckets=outcome.buckets, disabled_buckets=0)
        )
        errors = [d for d in data if d["MetricName"] == "BucketErrors"]
        assert sorted(d["Value"] for d in errors) == [0.0, 1.0]

    def test_outstanding_jobs_is_unknown_rather_than_zero(self):
        """Processing blew up at an unknown point, so whether the DescribeJob
        loop ran is not known. Zero would be a claim, and a completion report
        saying nothing remains in tracking is the false all-clear."""
        _, _, _, run_state = _run(raise_for=_BAD, completion_topic=True)

        assert run_state is not None
        assert run_state[_BAD].outstanding_jobs is None
        assert run_state[_BAD].submission_deferred is False


class TestAMalformedWatermarkLowDoesNotAbortTheRun:
    """The concrete trigger, driven through the real code path.

    Two failed records whose ``watermark_low`` values differ in type make
    ``plan_recovery``'s ``min(usable_lows)`` raise ``TypeError``, which no
    handler between there and the loop catches.
    """

    _RECORDS = {
        _BAD: {
            "rule-1": _record("job-a", "2026-08-28T10:00:00.000000Z"),
            "rule-2": _record("job-b", 1787900000),
        }
    }

    def test_the_second_bucket_still_has_its_interval(self):
        outcome, read_journal, submit, _ = _run(submission_records=self._RECORDS)

        assert _GOOD in _journal_buckets(read_journal)
        assert submit.call_args_list[-1].kwargs["source_bucket"] == _GOOD

    def test_the_malformed_bucket_is_reported_as_errored(self):
        outcome, *_ = _run(submission_records=self._RECORDS)

        assert _metrics_for(outcome, _BAD).errored is True

    def test_the_run_returns_an_outcome_for_every_bucket(self):
        """Before isolation the exception propagated out of run_interval, so
        there was no outcome, no summary log, and no metrics publish at all."""
        outcome, *_ = _run(submission_records=self._RECORDS)

        assert {bm.source_bucket for bm in outcome.buckets} == {_BAD, _GOOD}
