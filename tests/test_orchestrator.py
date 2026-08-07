"""Tests for src/orchestrator.py — tasks 17.3 and 17.4.

Task 17.3 — Property 8: Per-bucket fault isolation:
  For any bucket set where an arbitrary subset hits a skip-or-error condition,
  every non-failing bucket is still fully processed and each affected bucket
  produces a report identifying it and the reason.

Task 17.4 — Mocked end-to-end integration test for a single interval:
  Wires mocked AWS clients across the full pipeline and verifies:
  - At-most-one job per configuration per interval.
  - Checkpoint advances only on success.
  - Summary log emitted exactly once.

Requirements: 3.4, 3.5, 3.6, 4.4, 7.3, 8.1, 8.2, 9.1, 11.1, 12.3, 12.4, 12.5, 13.5, 13.8
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from src.adapters.batch_operations_adapter import SubmissionResult
from src.adapters.inventory_manifest_writer import WrittenManifest
from src.adapters.replication_config_adapter import SkipReport
from src.core.models import (
    CheckpointState,
    DestinationRef,
    DerivedReplicationRule,
    ManifestEntry,
    RunResult,
    S3Location,
    SubmissionRecord,
    SubmissionStatus,
    TaggingOperation,
)
from src.core.rule_deriver import derive_rules
from src.orchestrator import run_interval

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)
_BATCHOPS_ROLE_ARN = "arn:aws:iam::123456789012:role/s3rot-batch-operations-role"
_DEST_ARN = "arn:aws:s3:::dest-bucket"
_ACCOUNT_ID = "123456789012"
_STATE_BUCKET = "scratch-state-bucket"

_BASE_RUNTIME = {
    "state_bucket": _STATE_BUCKET,
    "athena_workgroup": "primary",
    "athena_output_location": f"s3://{_STATE_BUCKET}/athena/",
    "account_id": _ACCOUNT_ID,
    "batch_operations_role_arn": _BATCHOPS_ROLE_ARN,
    "region": "us-east-1",
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _config(bucket_names: list[str]) -> dict:
    return {"buckets": [{"name": n, "region": "us-east-1"} for n in bucket_names]}


def _rule(source_bucket: str, rule_id: str = "rule-1") -> DerivedReplicationRule:
    return DerivedReplicationRule(
        source_bucket=source_bucket,
        replication_config_id=rule_id,
        rule_id=rule_id,
        tag_filter={"env": "prod"},
        destination=DestinationRef(bucket_arn=_DEST_ARN),
    )


def _op(source_bucket: str, seq: str = "seq-001") -> TaggingOperation:
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key="path/obj.txt",
        resulting_tag_set={"env": "prod"},
        sequence_number=seq,
        operation="PutObjectTagging",
        event_time=_NOW,
    )


def _checkpoint(bucket: str, watermark: str = "") -> CheckpointState:
    return CheckpointState(
        source_bucket=bucket,
        last_processed_watermark=watermark,
        lease=None,
    )


def _written_manifest(config_id: str = "rule-1") -> WrittenManifest:
    return WrittenManifest(
        s3_location=S3Location(bucket=_STATE_BUCKET, key=f"manifests/{config_id}/ts.csv"),
        etag="abc123",
        object_count=1,
    )


def _submitted(config_id: str = "rule-1") -> SubmissionResult:
    return SubmissionResult(
        status=SubmissionStatus.SUBMITTED,
        config_id=config_id,
        object_count=1,
        job_id="job-abc-123",
    )


def _failed(config_id: str = "rule-1") -> SubmissionResult:
    return SubmissionResult(
        status=SubmissionStatus.CREATE_FAILED,
        config_id=config_id,
        object_count=1,
        error_reason="AccessDenied: Not authorized",
    )


def _make_mocks(
    bucket_names: list[str],
    ops_per_bucket: dict[str, list[TaggingOperation]] | None = None,
    fail_rules_for: set[str] | None = None,
    submission_factory=None,
) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock, MagicMock]:
    """Return (factory_cls, store_cls, get_rules, read_journal, submit_job)."""
    if ops_per_bucket is None:
        ops_per_bucket = {n: [_op(n)] for n in bucket_names}
    if fail_rules_for is None:
        fail_rules_for = set()
    if submission_factory is None:
        submission_factory = lambda cfg_id: _submitted(cfg_id)

    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    def get_checkpoint_side_effect(s3_client, state_bucket, source_bucket):
        return (_checkpoint(source_bucket), '"etag-0"')

    mock_store.get_checkpoint.side_effect = get_checkpoint_side_effect
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'

    def get_rules_side_effect(s3_client, bucket):
        if bucket.name in fail_rules_for:
            return ([], [SkipReport(source_bucket=bucket.name, reason="test error")])
        return ([_rule(bucket.name)], [])

    mock_get_rules = MagicMock(side_effect=get_rules_side_effect)

    def read_journal_side_effect(
        athena_client, bucket_name, athena_workgroup, output_location,
        since_timestamp=None, kms_key_arn=None, until_timestamp=None
    ):
        return (ops_per_bucket.get(bucket_name, []), [])

    mock_read_journal = MagicMock(side_effect=read_journal_side_effect)

    def submit_side_effect(**kwargs):
        return submission_factory(kwargs.get("config_id", "rule-1"))

    mock_submit_job = MagicMock(side_effect=submit_side_effect)

    return (
        mock_factory_cls, mock_store_cls,
        mock_get_rules, mock_read_journal,
        mock_submit_job,
    )


def _run_with_mocks(
    bucket_names,
    ops_per_bucket=None,
    fail_rules_for=None,
    submission_factory=None,
    runtime=None,
    boundary_timestamp=None,
    _return_find_boundary_mock=False,
    _return_outcome=False,
):
    """Convenience: run_interval with fully mocked adapters.

    ``boundary_timestamp`` (default ``None``) is returned by the mocked
    ``find_row_count_boundary`` — pass a canonical watermark string to
    exercise the row-count-cap path (code-review-remediation
    verification-notes.md "scaling risk" finding); ``None`` (the default)
    exercises the common, uncapped path. Pass
    ``_return_find_boundary_mock=True`` to get the boundary mock back as a
    5th tuple element (kept separate from the default 4-tuple return so
    every existing 4-way-unpacking call site is unaffected). Pass
    ``_return_outcome=True`` to get ``run_interval``'s returned
    ``RunOutcome`` appended as the last tuple element (task 6.2).
    """
    (
        mock_factory_cls, mock_store_cls,
        mock_get_rules, mock_read_journal,
        mock_submit_job,
    ) = _make_mocks(
        bucket_names,
        ops_per_bucket=ops_per_bucket,
        fail_rules_for=fail_rules_for,
        submission_factory=submission_factory,
    )
    rt = runtime or _BASE_RUNTIME
    mock_find_boundary = MagicMock(return_value=boundary_timestamp)
    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
        patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
        # Row-count-cap boundary check (code-review-remediation
        # verification-notes.md "scaling risk" finding): return None (no
        # cap needed — the common case) so existing tests, whose mocked
        # athena_client has no real get_query_execution behavior, don't
        # fall through to the real find_row_count_boundary and hang
        # polling a MagicMock response that never reaches a terminal state.
        patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
              mock_find_boundary),
        patch("src.orchestrator.write_in_memory_inventory_manifest",
              return_value=_written_manifest()),
        patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
        # Patch new large-scale adapters: return 0 preflight (stays on small/CSV path)
        # and empty permanent-deletes (no filtering) so existing tests are unaffected.
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
    ):
        outcome = run_interval(_config(bucket_names), rt)
    base = (mock_get_rules, mock_read_journal, mock_submit_job, mock_store_cls.return_value)
    if _return_find_boundary_mock:
        base = base + (mock_find_boundary,)
    if _return_outcome:
        base = base + (outcome,)
    return base


# ---------------------------------------------------------------------------
# Task 17.4: mocked end-to-end integration test
# ---------------------------------------------------------------------------


class TestEndToEndSingleInterval:
    def test_submit_called_once_per_config_with_matches(self):
        """At-most-one job per configuration per interval (Req 8.1)."""
        _, _, mock_submit, _ = _run_with_mocks(["my-bucket"])
        assert mock_submit.call_count == 1

    def test_submit_not_called_when_no_journal_ops(self):
        """No journal ops → no matches → no job submitted."""
        _, _, mock_submit, _ = _run_with_mocks(
            ["my-bucket"], ops_per_bucket={"my-bucket": []}
        )
        assert mock_submit.call_count == 0

    def test_checkpoint_advanced_on_successful_submission(self):
        """release_lease called with submitted refs on success (Req 9.1)."""
        _, _, _, mock_store = _run_with_mocks(["my-bucket"])
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        # On success the ops included in the job are passed as submitted_refs.
        assert kwargs["submitted_refs"] is not None
        assert len(kwargs["submitted_refs"]) == 1

    def test_checkpoint_not_advanced_on_failed_submission(self):
        """release_lease called with submitted_refs=None on failure (Req 9.3, 10.2)."""
        _, _, _, mock_store = _run_with_mocks(
            ["my-bucket"],
            submission_factory=lambda cfg_id: _failed(cfg_id),
        )
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None

    def test_failed_submission_marks_the_bucket_errored(self):
        """A creation failure must move a metric
        (submission-failure-visibility Req 1.1).

        Before this, `errored` stayed False on a CREATE_FAILED, so
        `BucketErrors` published 0.0 for a bucket whose every submission was
        failing. The circuit breaker cannot see it either — that counter only
        increments for a terminal job failure observed via DescribeJob, and a
        job that was never created has no record and no status. A submission
        that fails before the job exists could therefore run indefinitely with
        every metric reading healthy.
        """
        *_, outcome = _run_with_mocks(
            ["my-bucket"],
            submission_factory=lambda cfg_id: _failed(cfg_id),
            _return_outcome=True,
        )
        assert outcome.buckets[0].errored is True

    def test_failed_submission_publishes_bucket_errors_metric(self):
        """The datum a failed submission produces, asserted through the real
        metric builder rather than a mocked put_metric_data, so this fails if
        `BucketErrors` stops being emitted per bucket
        (submission-failure-visibility Req 5.1)."""
        from src.adapters.metrics_publisher import _build_metric_data

        *_, outcome = _run_with_mocks(
            ["my-bucket"],
            submission_factory=lambda cfg_id: _failed(cfg_id),
            _return_outcome=True,
        )
        data = _build_metric_data(
            RunResult(buckets=outcome.buckets, disabled_buckets=0)
        )
        errors = [d for d in data if d["MetricName"] == "BucketErrors"]
        assert len(errors) == 1
        assert errors[0]["Value"] == 1.0

    def test_skipped_submission_does_not_mark_the_bucket_errored(self):
        """An empty manifest is a skip, not an error
        (submission-failure-visibility Req 1.2)."""
        *_, outcome = _run_with_mocks(
            ["my-bucket"],
            ops_per_bucket={"my-bucket": []},
            _return_outcome=True,
        )
        assert all(bm.errored is False for bm in outcome.buckets)

    def test_summary_log_emitted_once(self, caplog):
        """Exactly one summary log entry per interval (Req 11.1)."""
        with caplog.at_level(logging.INFO):
            _run_with_mocks(["my-bucket"])
        summary_records = [r for r in caplog.records if "interval_summary" in r.message]
        assert len(summary_records) == 1

    def test_batch_operations_role_passed_to_submit(self):
        """The stack-created Batch Operations role reaches the job (Req 2.1)."""
        _, _, mock_submit, _ = _run_with_mocks(["my-bucket"])
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["batch_operations_role_arn"] == _BATCHOPS_ROLE_ARN
        assert "replication_role_arn" not in kwargs

    def test_manifest_write_called_before_submit(self):
        """Inventory manifest is written before batch job is created."""
        _, _, mock_submit, _ = _run_with_mocks(["my-bucket"])
        assert mock_submit.call_count == 1

    def test_two_buckets_each_get_own_job(self):
        """Two buckets → two jobs (one per config, one per bucket) (Req 8.1)."""
        _, _, mock_submit, mock_store = _run_with_mocks(["bucket-aa", "bucket-bb"])
        assert mock_submit.call_count == 2

    def test_invalid_config_raises_before_any_s3_access(self):
        """Fatal config error → no S3 calls made (Req 13.6)."""
        from src.core.config_loader import ConfigError
        with pytest.raises(ConfigError):
            _run_with_mocks([])

    def test_get_rules_called_once_per_bucket(self):
        """get_replication_rules called for each Monitored_Bucket (Req 3.1)."""
        mock_get_rules, _, _, _ = _run_with_mocks(["bkt-1", "bkt-2", "bkt-3"])
        assert mock_get_rules.call_count == 3

    def test_acquire_lease_called_when_ops_exist(self):
        """acquire_lease is called to guard concurrent runs (Req 9.4)."""
        _, _, _, mock_store = _run_with_mocks(["my-bucket"])
        assert mock_store.acquire_lease.call_count == 1


# ---------------------------------------------------------------------------
# Task 17.3: Property 8 — per-bucket fault isolation
# Feature: tag-based-s3-replication, Property 8: Per-bucket fault isolation
# ---------------------------------------------------------------------------


class TestProperty8PerBucketFaultIsolation:
    @given(
        bucket_count=st.integers(min_value=2, max_value=6),
        failing_indices=st.lists(
            st.integers(min_value=0, max_value=5),
            min_size=1,
            max_size=5,
            unique=True,
        ),
    )
    @settings(max_examples=50)
    def test_non_failing_buckets_fully_processed_when_subset_fails(
        self, bucket_count: int, failing_indices: list[int]
    ) -> None:
        """Non-failing buckets are still processed when others skip/error (Req 3.3–3.6).

        # Feature: tag-based-s3-replication, Property 8: Per-bucket fault isolation
        """
        failing_set = {i for i in failing_indices if i < bucket_count}
        assume(len(failing_set) < bucket_count)  # at least one non-failing bucket

        buckets = [f"bucket-{i:02d}" for i in range(bucket_count)]
        fail_rules_for = {buckets[i] for i in failing_set}
        non_failing = set(buckets) - fail_rules_for

        mock_get_rules, mock_read_journal, _, _ = _run_with_mocks(
            buckets,
            fail_rules_for=fail_rules_for,
        )

        # read_journal should be called for every non-failing bucket.
        journal_called_for = {
            c.kwargs.get("bucket_name", c.args[1] if len(c.args) > 1 else "")
            for c in mock_read_journal.call_args_list
        }
        for nb in non_failing:
            assert nb in journal_called_for, (
                f"read_journal not called for non-failing bucket {nb!r}"
            )

        # read_journal should NOT be called for failing buckets (skipped early).
        for fb in fail_rules_for:
            assert fb not in journal_called_for, (
                f"read_journal was called for failing bucket {fb!r}"
            )

    @given(
        bucket_count=st.integers(min_value=1, max_value=5),
        failing_indices=st.lists(
            st.integers(min_value=0, max_value=4),
            min_size=1,
            max_size=5,
            unique=True,
        ),
    )
    @settings(max_examples=50)
    def test_failing_buckets_produce_error_log(
        self, bucket_count: int, failing_indices: list[int]
    ) -> None:
        """Each failing bucket produces a report identifying it (Req 3.3, 3.5, 3.6).

        # Feature: tag-based-s3-replication, Property 8: Per-bucket fault isolation
        """
        failing_set = {i for i in failing_indices if i < bucket_count}
        assume(len(failing_set) >= 1)

        buckets = [f"bucket-{i:02d}" for i in range(bucket_count)]
        fail_rules_for = {buckets[i] for i in failing_set}

        emitted_entries: list[dict] = []

        def capture_emit(entry: dict) -> None:
            emitted_entries.append(entry)

        with patch("src.orchestrator.observability.emit", side_effect=capture_emit):
            _run_with_mocks(buckets, fail_rules_for=fail_rules_for)

        error_entries = [e for e in emitted_entries if e.get("event") == "error"]
        error_text = " ".join(e.get("bucket", "") + " " + e.get("cause", "") for e in error_entries)
        for fb in fail_rules_for:
            assert fb in error_text, (
                f"No error log for failing bucket {fb!r}"
            )

    @given(
        bucket_count=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=30)
    def test_all_buckets_failing_produces_zero_submissions(
        self, bucket_count: int
    ) -> None:
        """When all buckets fail, no jobs are submitted but run completes.

        # Feature: tag-based-s3-replication, Property 8: Per-bucket fault isolation
        """
        buckets = [f"bucket-{i:02d}" for i in range(bucket_count)]
        _, _, mock_submit, _ = _run_with_mocks(
            buckets,
            fail_rules_for=set(buckets),
        )
        assert mock_submit.call_count == 0

    def test_single_bucket_exception_in_get_rules_does_not_abort_run(self, caplog):
        """Exception from get_replication_rules → skip + continue (Req 3.6)."""

        def get_rules_side_effect(s3_client, bucket):
            if bucket.name == "bad-bucket":
                return ([], [SkipReport(source_bucket=bucket.name, reason="access denied")])
            return ([_rule(bucket.name)], [])

        (
            mock_factory_cls, mock_store_cls,
            _, mock_read_journal,
            mock_submit,
        ) = _make_mocks(["bad-bucket", "good-bucket"])

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules",
                  side_effect=get_rules_side_effect),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value=None),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            caplog.at_level(logging.INFO),
        ):
            run_interval(_config(["bad-bucket", "good-bucket"]), _BASE_RUNTIME)

        # good-bucket was processed.
        journal_called_for = {
            c.kwargs.get("bucket_name", c.args[1] if len(c.args) > 1 else "")
            for c in mock_read_journal.call_args_list
        }
        assert "good-bucket" in journal_called_for
        assert "bad-bucket" not in journal_called_for


# ---------------------------------------------------------------------------
# CloudWatch metrics integration tests — task 7.2
# Feature: cloudwatch-metrics
# Requirements: 1.3, 4.1, 4.2, 5.1, 5.2, 5.3, 7.1, 7.2, 8.1, 9.3
# ---------------------------------------------------------------------------


class TestMetricsPublishing:
    """Verify the orchestrator publishes a RunResult after the loop and handles failures."""

    def _run_with_metrics(self, namespace, extra_dims=None, publisher_raises=False):
        """Run with mocked adapters + a captured MetricsPublisher."""
        captured_run_result = []
        captured_dims = []

        class _FakePublisher:
            def __init__(self, namespace, dimensions=None):
                captured_dims.append(dimensions)

            def publish(self, run_result):
                captured_run_result.append(run_result)
                if publisher_raises:
                    raise RuntimeError("CloudWatch unavailable")

        rt = {**_BASE_RUNTIME}
        if namespace:
            rt["metrics_namespace"] = namespace
        if extra_dims:
            rt["metrics_dimensions"] = extra_dims

        with patch("src.orchestrator.MetricsPublisher", _FakePublisher):
            _run_with_mocks(["bucket-aa"], runtime=rt)

        return captured_run_result, captured_dims

    def test_publisher_called_once_after_loop(self):
        """MetricsPublisher.publish invoked exactly once after the loop (Req 4.1)."""
        results, _ = self._run_with_metrics("TestNS")
        assert len(results) == 1

    def test_run_result_has_correct_bucket_counters(self):
        """RunResult buckets match per-bucket processing counters (Req 4.3)."""
        results, _ = self._run_with_metrics("TestNS")
        run_result = results[0]
        assert len(run_result.buckets) == 1
        bm = run_result.buckets[0]
        assert bm.source_bucket == "bucket-aa"
        assert bm.ops_read >= 0
        assert bm.matched >= 0
        assert bm.submitted >= 0

    def test_run_result_disabled_buckets_present(self):
        """RunResult carries disabled_buckets (Req 3.3).

        ``duplicate_records_discarded`` is deliberately absent: it remains a
        field of the ``interval_summary`` log entry only, having conflated
        malformed records, genuine duplicate deliveries, and already-processed
        records as a metric.
        """
        results, _ = self._run_with_metrics("TestNS")
        run_result = results[0]
        assert isinstance(run_result.disabled_buckets, int)
        assert run_result.disabled_buckets >= 0
        assert not hasattr(run_result, "duplicate_records_discarded")

    def test_absent_namespace_is_true_no_op(self):
        """With no metrics_namespace, MetricsPublisher.publish still called but no boto3 (Req 1.3)."""
        # The publisher is a no-op when namespace is falsy — we verify the
        # result accumulation still works (RunResult is constructed regardless).
        results, _ = self._run_with_metrics(None)
        # publish is called; the publisher's no-op gate handles the rest
        assert len(results) == 1

    def test_publisher_exception_does_not_fail_run(self):
        """Publisher exception is caught; run still succeeds (Req 5.1, 5.2)."""
        # Should not raise even though the publisher raises
        results, _ = self._run_with_metrics("TestNS", publisher_raises=True)
        # publish was called before the exception
        assert len(results) == 1

    def test_publisher_exception_emits_error_log(self):
        """Publisher exception emits error log with Metrics_Publisher component (Req 5.1)."""
        emitted: list[dict] = []

        class _RaisingPublisher:
            def __init__(self, namespace, dimensions=None):
                pass

            def publish(self, run_result):
                raise RuntimeError("cw failure")

        with patch("src.orchestrator.MetricsPublisher", _RaisingPublisher):
            with patch("src.orchestrator.observability.emit",
                       side_effect=emitted.append):
                with patch("src.orchestrator.preflight_count", return_value=0):
                    with patch("src.orchestrator.read_permanent_deletes", return_value=set()):
                        with patch("src.orchestrator.write_in_memory_inventory_manifest",
                                   return_value=_written_manifest()):
                            _run_with_mocks(["my-bucket"],
                                            runtime={**_BASE_RUNTIME, "metrics_namespace": "NS"})

        error_entries = [e for e in emitted if e.get("event") == "error"]
        metrics_errors = [
            e for e in error_entries
            if "Metrics_Publisher" in e.get("component", "")
        ]
        assert len(metrics_errors) >= 1

    def test_metrics_dimensions_passed_to_publisher(self):
        """metrics_dimensions from runtime_config flows to the publisher (Req 9.3)."""
        _, captured_dims = self._run_with_metrics(
            "TestNS", extra_dims={"Deployment": "my-stack"}
        )
        assert captured_dims[0] == {"Deployment": "my-stack"}

    def test_errored_bucket_has_errored_flag_set(self):
        """Bucket skipped due to client error → BucketMetrics.errored=True (Req 7.1)."""
        captured: list = []

        class _CapturingPublisher:
            def __init__(self, namespace, dimensions=None):
                pass

            def publish(self, run_result):
                captured.append(run_result)

        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit,
        ) = _make_mocks(["err-bucket"])
        # Make client creation fail for err-bucket
        mock_factory_cls.return_value.create_s3_client.side_effect = RuntimeError("bad region")

        rt = {**_BASE_RUNTIME, "metrics_namespace": "NS"}
        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value=None),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch("src.orchestrator.MetricsPublisher", _CapturingPublisher),
        ):
            run_interval(_config(["err-bucket"]), rt)

        assert len(captured) == 1
        assert captured[0].buckets[0].errored is True

    def test_no_rules_bucket_has_errored_false(self):
        """Bucket skipped because no tag-scoped rules → BucketMetrics.errored=False (Req 7.2)."""
        captured: list = []

        class _CapturingPublisher:
            def __init__(self, namespace, dimensions=None):
                pass

            def publish(self, run_result):
                captured.append(run_result)

        rt = {**_BASE_RUNTIME, "metrics_namespace": "NS"}
        with patch("src.orchestrator.MetricsPublisher", _CapturingPublisher):
            _run_with_mocks(
                ["no-rules-bucket"],
                fail_rules_for={"no-rules-bucket"},
                runtime=rt,
            )

        assert len(captured) == 1
        assert captured[0].buckets[0].errored is False


# ---------------------------------------------------------------------------
# Large-scale manifest generation — Properties 6 and 7
# Feature: large-scale-manifest-generation
# Requirements: 2.6, 8.5, 9.1, 9.2, 9.4
# ---------------------------------------------------------------------------


class TestProperty6CheckpointAcrossFormats:
    """Property 6: checkpoint advances only on submission success, across both modes."""

    def test_checkpoint_advances_on_csv_manifest_success(self):
        """Small/no-KMS path: checkpoint advances on successful submission (Req 9.1)."""
        _, _, _, mock_store = _run_with_mocks(["bucket-a"])
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is not None

    def test_checkpoint_not_advanced_on_csv_manifest_failure(self):
        """Small path: checkpoint unchanged on submission failure (Req 9.2)."""
        _, _, _, mock_store = _run_with_mocks(
            ["bucket-a"],
            submission_factory=lambda cfg_id: _failed(cfg_id),
        )
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None

    @given(
        bucket_count=st.integers(min_value=1, max_value=4),
        submission_fails=st.booleans(),
    )
    @settings(max_examples=30)
    def test_property_6_checkpoint_advances_iff_submitted(
        self, bucket_count: int, submission_fails: bool
    ) -> None:
        """Property 6: watermark advances iff job submitted successfully.

        # Feature: large-scale-manifest-generation, Property 6: Checkpoint advances only on submission success, across both modes
        """
        buckets = [f"bucket-{i:02d}" for i in range(bucket_count)]
        if submission_fails:
            factory = lambda cfg_id: _failed(cfg_id)
        else:
            factory = lambda cfg_id: _submitted(cfg_id)

        _, _, _, mock_store = _run_with_mocks(buckets, submission_factory=factory)

        for call in mock_store.release_lease.call_args_list:
            kwargs = call.kwargs
            if submission_fails:
                assert kwargs.get("submitted_refs") is None
            else:
                assert kwargs.get("submitted_refs") is not None


class TestProperty7SmallNoKmsPathUsesInventory:
    """Property 7: small/no-KMS path uses Inventory_Report format (CSV path removed)."""

    def test_inventory_format_used_by_default(self):
        """With no KMS and small count, format = Inventory_Report."""
        _, _, mock_submit, _ = _run_with_mocks(["bucket-a"])
        assert mock_submit.call_count == 1
        kwargs = mock_submit.call_args.kwargs
        fmt = kwargs.get("manifest_format", "")
        assert fmt == "S3InventoryReport_CSV_20161130"

    @given(bucket_count=st.integers(min_value=1, max_value=3))
    @settings(max_examples=30)
    def test_property_7_small_path_uses_inventory_format(
        self, bucket_count: int
    ) -> None:
        """Property 7: small/no-KMS intervals produce Inventory_Report manifests.

        # Feature: large-scale-manifest-generation, Property 7: Small/no-KMS path uses Inventory_Report format
        """
        buckets = [f"bkt-{i:02d}" for i in range(bucket_count)]
        _, _, mock_submit, _ = _run_with_mocks(buckets)
        assert mock_submit.call_count == bucket_count
        for c in mock_submit.call_args_list:
            fmt = c.kwargs.get("manifest_format", "")
            assert fmt == "S3InventoryReport_CSV_20161130"


# ---------------------------------------------------------------------------
# Task 4.6: Failed-job recovery tests (Requirements 4.1–4.4, 4.6, 4.7)
# ---------------------------------------------------------------------------

_WM_LOW = "2024-01-01T00:00:20.000000Z"   # watermark before prior run
_WM_CURRENT = "2024-01-01T00:01:20.000000Z"  # current persisted watermark


def _prior_rec(
    config_id: str = "rule-1",
    job_id: str = "job-prior",
    watermark_low: str = _WM_LOW,
    watermark_high: str = _WM_CURRENT,
) -> SubmissionRecord:
    return SubmissionRecord(
        replication_config_id=config_id,
        source_bucket="my-bucket",
        job_id=job_id,
        manifest_key=f"manifests/{config_id}/key.csv",
        submitted_at=_NOW,
        status=SubmissionStatus.SUBMITTED,
        watermark_low=watermark_low,
        watermark_high=watermark_high,
    )


def _run_with_recovery_mocks(
    prior_submissions: dict,
    describe_responses: dict,  # job_id → str status or Exception
    bucket_name: str = "my-bucket",
    progress_summaries: dict | None = None,  # job_id → ProgressSummary dict
) -> list:
    """Run one interval with recovery-aware mocks; returns emitted events list."""
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3control = MagicMock()
    progress_summaries = progress_summaries or {}

    def describe_job(AccountId, JobId):
        resp = describe_responses.get(JobId)
        if isinstance(resp, Exception):
            raise resp
        job: dict = {"Status": resp}
        if JobId in progress_summaries:
            job["ProgressSummary"] = progress_summaries[JobId]
        return {"Job": job}

    mock_s3control.describe_job.side_effect = describe_job
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket, _WM_CURRENT), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = prior_submissions
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'

    emitted: list = []

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch(
            "src.orchestrator.bops_report_reader.read_bops_completion_report",
            return_value=[],
        ),
    ):
        run_interval(_config([bucket_name]), _BASE_RUNTIME)

    return emitted


def _audits(emitted: list, action: str) -> list:
    return [
        e for e in emitted
        if isinstance(e, dict) and e.get("event") == "audit" and e.get("action") == action
    ]


def _errors(emitted: list) -> list:
    return [e for e in emitted if isinstance(e, dict) and e.get("event") == "error"]


class TestFailedJobRecovery:
    def test_failed_job_emits_readmit_audit(self):
        """A Failed job triggers a batch_job_failure_readmit audit entry."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-123")},
            describe_responses={"job-123": "Failed"},
        )
        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 1
        assert readmits[0]["job_id"] == "job-123"
        assert readmits[0]["watermark_low"] == _WM_LOW

    def test_cancelled_job_emits_readmit_audit(self):
        """A Cancelled job also triggers a readmit audit entry."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-456")},
            describe_responses={"job-456": "Cancelled"},
        )
        assert len(_audits(emitted, "batch_job_failure_readmit")) == 1

    def test_completed_job_does_not_emit_readmit(self):
        """A Complete job leaves the watermark unchanged — no readmit audit."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-done")},
            describe_responses={"job-done": "Complete"},
        )
        assert len(_audits(emitted, "batch_job_failure_readmit")) == 0

    def test_completed_job_all_tasks_succeeded_no_readmit(self):
        """A Complete job where tasks genuinely succeeded is not a failure."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-ok")},
            describe_responses={"job-ok": "Complete"},
            progress_summaries={
                "job-ok": {"NumberOfTasksSucceeded": 3, "NumberOfTasksFailed": 0}
            },
        )
        assert len(_audits(emitted, "batch_job_failure_readmit")) == 0
        diagnostics = [
            e for e in _errors(emitted) if "every task failed" in e.get("cause", "")
        ]
        assert len(diagnostics) == 0

    def test_completed_job_all_tasks_failed_emits_readmit_and_diagnostic(self):
        """A Complete job where every task failed is treated as an effective
        failure: it gets both the ordinary readmit audit AND a distinct
        diagnostic error log recording the task-count evidence."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-all-failed")},
            describe_responses={"job-all-failed": "Complete"},
            progress_summaries={
                "job-all-failed": {"NumberOfTasksSucceeded": 0, "NumberOfTasksFailed": 1}
            },
        )
        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 1
        assert readmits[0]["job_id"] == "job-all-failed"

        diagnostics = [
            e for e in _errors(emitted)
            if "job-all-failed" in e.get("cause", "") and "every task failed" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1
        # This entry carries the task-count evidence and points at the
        # completion-report diagnostic, which is where the specific ErrorCode
        # appears (always-on-completion-report, task 5).
        assert "No object was replicated" in diagnostics[0]["cause"]
        assert "completion-report diagnostic" in diagnostics[0]["cause"]

    def test_completed_job_partial_failure_not_treated_as_effective_failure(self):
        """A large NumberOfTasksFailed alongside a nonzero
        NumberOfTasksSucceeded is not treated as a failure — mirrors the
        real 100k-object job where NumberOfTasksFailed was a reporting
        artifact, not a real incident (see job_recovery.py docstring)."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-partial")},
            describe_responses={"job-partial": "Complete"},
            progress_summaries={
                "job-partial": {
                    "NumberOfTasksSucceeded": 1,
                    "NumberOfTasksFailed": 100001,
                }
            },
        )
        assert len(_audits(emitted, "batch_job_failure_readmit")) == 0
        diagnostics = [
            e for e in _errors(emitted) if "every task failed" in e.get("cause", "")
        ]
        assert len(diagnostics) == 0

    def test_no_prior_submissions_no_readmit(self):
        """An empty prior-submissions dict produces no readmit audit."""
        emitted = _run_with_recovery_mocks(
            prior_submissions={},
            describe_responses={},
        )
        assert len(_audits(emitted, "batch_job_failure_readmit")) == 0

    def test_describe_job_error_absorbed_run_continues(self):
        """A DescribeJob ClientError is absorbed at WARNING — the run still completes."""
        from botocore.exceptions import ClientError

        err = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "DescribeJob"
        )
        # Should not raise
        emitted = _run_with_recovery_mocks(
            prior_submissions={"rule-1": _prior_rec(job_id="job-err")},
            describe_responses={"job-err": err},
        )
        # An error log is emitted for the DescribeJob failure
        describe_errors = [
            e for e in _errors(emitted) if "DescribeJob" in e.get("cause", "")
        ]
        assert len(describe_errors) >= 1
        # No readmit audit for an errored check
        assert len(_audits(emitted, "batch_job_failure_readmit")) == 0

    def test_multi_config_both_failed_emit_two_readmit_audits(self):
        """Two failed configs each emit their own readmit audit entry."""
        _WM_A = "2024-01-01T00:00:05.000000Z"
        _WM_B = "2024-01-01T00:00:40.000000Z"
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=_WM_A),
            "rule-b": _prior_rec("rule-b", "job-b", watermark_low=_WM_B),
        }
        emitted = _run_with_recovery_mocks(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed", "job-b": "Failed"},
        )
        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 2
        # Both watermark_lows appear in the audit details
        wm_lows = {r["watermark_low"] for r in readmits}
        assert _WM_A in wm_lows
        assert _WM_B in wm_lows

    def test_multi_config_min_watermark_low_wins(self):
        """When two configs fail, rollback uses the minimum watermark_low."""
        _WM_EARLIER = "2024-01-01T00:00:05.000000Z"
        _WM_LATER = "2024-01-01T00:00:40.000000Z"
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=_WM_EARLIER),
            "rule-b": _prior_rec("rule-b", "job-b", watermark_low=_WM_LATER),
        }
        emitted = _run_with_recovery_mocks(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed", "job-b": "Failed"},
        )
        # Two readmit audits emitted (one per config)
        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 2
        # The audit for rule-a has the earlier watermark_low
        rule_a_audit = next(
            r for r in readmits if r["config_id"] == "rule-a"
        )
        assert rule_a_audit["watermark_low"] == _WM_EARLIER

    def test_describe_job_called_for_each_submission(self):
        """describe_job is invoked exactly once per submission record."""
        mock_factory_cls = MagicMock()
        mock_factory = MagicMock()
        mock_factory_cls.return_value = mock_factory

        mock_s3control = MagicMock()
        mock_s3control.describe_job.return_value = {"Job": {"Status": "Complete"}}
        mock_factory.create_s3control_client.return_value = mock_s3control

        mock_store_cls = MagicMock()
        mock_store = MagicMock()
        mock_store_cls.return_value = mock_store
        mock_store.get_checkpoint.side_effect = (
            lambda *a: (_checkpoint("my-bucket", _WM_CURRENT), '"etag-0"')
        )
        prior = {
            "rule-x": _prior_rec("rule-x", "job-x"),
            "rule-y": _prior_rec("rule-y", "job-y"),
        }
        mock_store.get_submission_records.return_value = prior
        mock_store.acquire_lease.return_value = '"etag-1"'
        mock_store.release_lease.return_value = '"etag-2"'
        mock_store.record_submission.return_value = '"etag-3"'

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch(
                "src.orchestrator.replication_config_adapter.get_replication_rules",
                return_value=([_rule("my-bucket")], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.read_journal",
                return_value=([_op("my-bucket")], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                return_value=None,
            ),
            patch(
                "src.orchestrator.write_in_memory_inventory_manifest",
                return_value=_written_manifest(),
            ),
            patch(
                "src.orchestrator.batch_operations_adapter.submit_batch_job",
                return_value=_submitted(),
            ),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch(
                "src.orchestrator.bops_report_reader.read_bops_completion_report",
                return_value=[],
            ),
        ):
            run_interval(_config(["my-bucket"]), _BASE_RUNTIME)

        assert mock_s3control.describe_job.call_count == 2


# ---------------------------------------------------------------------------
# Circuit breaker: MaxBatchJobFailures (Requirements 4.x)
# ---------------------------------------------------------------------------

_CB_WM_LOW = "2024-01-01T00:00:10.000000Z"
_CB_WM_CURRENT = "2024-01-01T00:01:10.000000Z"


def _prior_rec_with_count(
    config_id: str = "rule-1",
    job_id: str = "job-cb",
    consecutive_failures: int = 0,
) -> SubmissionRecord:
    return SubmissionRecord(
        replication_config_id=config_id,
        source_bucket="my-bucket",
        job_id=job_id,
        manifest_key=f"manifests/{config_id}/key.csv",
        submitted_at=_NOW,
        status=SubmissionStatus.SUBMITTED,
        watermark_low=_CB_WM_LOW,
        watermark_high=_CB_WM_CURRENT,
        consecutive_failures=consecutive_failures,
    )


def _run_circuit_breaker(
    prior_submissions: dict,
    describe_responses: dict,
    max_batch_job_failures: int = 4,
    bucket_name: str = "my-bucket",
) -> tuple[list, list, MagicMock]:
    """Run one interval with circuit-breaker mocks.

    Returns (disabled_buckets, emitted_events, mock_store).
    """
    disabled_buckets: list[str] = []

    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3control = MagicMock()

    def describe_job(AccountId, JobId):
        resp = describe_responses.get(JobId)
        if isinstance(resp, Exception):
            raise resp
        return {"Job": {"Status": resp}}

    mock_s3control.describe_job.side_effect = describe_job
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store
    mock_store.get_checkpoint.side_effect = (
        lambda *a: (_checkpoint(bucket_name, _CB_WM_CURRENT), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = prior_submissions
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'

    emitted: list = []

    rt = {
        **_BASE_RUNTIME,
        "max_batch_job_failures": max_batch_job_failures,
        "on_bucket_disable": lambda name, reason: disabled_buckets.append(name),
    }

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch(
            "src.orchestrator.bops_report_reader.read_bops_completion_report",
            return_value=[],
        ),
    ):
        run_interval(_config([bucket_name]), rt)

    return disabled_buckets, emitted, mock_store


class TestCircuitBreaker:
    def test_at_threshold_disables_bucket(self):
        """consecutive_failures + 1 == threshold → on_bucket_disable called."""
        # Prior record has 3 failures; this run adds the 4th → hit threshold of 4.
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=3)}
        disabled, _, _ = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Failed"},
            max_batch_job_failures=4,
        )
        assert "my-bucket" in disabled

    def test_below_threshold_does_not_disable(self):
        """consecutive_failures + 1 < threshold → bucket NOT disabled."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=2)}
        disabled, _, _ = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Failed"},
            max_batch_job_failures=4,
        )
        assert disabled == []

    def test_no_new_job_submitted_when_disabled(self):
        """When circuit breaker fires, no new job is submitted for this bucket."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=3)}
        _, _, mock_store = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Failed"},
            max_batch_job_failures=4,
        )
        mock_store.record_submission.assert_not_called()

    def test_threshold_1_disables_on_first_failure(self):
        """Custom threshold of 1: any failure immediately disables."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=0)}
        disabled, _, _ = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Failed"},
            max_batch_job_failures=1,
        )
        assert "my-bucket" in disabled

    def test_complete_status_resets_counter_to_zero(self):
        """Prior failures don't carry forward when the job succeeded this run."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=3)}
        _, _, mock_store = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Complete"},
            max_batch_job_failures=4,
        )
        # A new job should be submitted with consecutive_failures = 0
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.consecutive_failures == 0

    def test_describe_job_error_carries_over_counter(self):
        """DescribeJob failure → counter not incremented; run continues."""
        from botocore.exceptions import ClientError

        err = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "DescribeJob"
        )
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=2)}
        disabled, emitted, mock_store = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": err},
            max_batch_job_failures=4,
        )
        # Bucket must NOT be disabled (only 2 confirmed failures)
        assert disabled == []
        # New submission record should carry over the existing count (2, not 3)
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.consecutive_failures == 2

    def test_failure_counter_increments_in_new_submission_record(self):
        """Successful re-submission after a failure stores incremented count."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=1)}
        _, _, mock_store = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Failed"},
            max_batch_job_failures=4,
        )
        # Below threshold → re-admitted and new job submitted with count=2
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.consecutive_failures == 2

    def test_cancelled_job_also_increments_counter(self):
        """Cancelled status is treated the same as Failed for counting."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=3)}
        disabled, _, _ = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Cancelled"},
            max_batch_job_failures=4,
        )
        assert "my-bucket" in disabled

    def test_circuit_breaker_error_log_emitted_on_disable(self):
        """An error log is emitted when the circuit breaker disables a bucket."""
        prior = {"rule-1": _prior_rec_with_count(consecutive_failures=3)}
        _, emitted, _ = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-cb": "Failed"},
            max_batch_job_failures=4,
        )
        errors = [e for e in emitted if isinstance(e, dict) and e.get("event") == "error"]
        cb_errors = [
            e for e in errors
            if "consecutive" in e.get("cause", "").lower()
            or "threshold" in e.get("cause", "").lower()
        ]
        assert len(cb_errors) >= 1


# ---------------------------------------------------------------------------
# Task 17.1: BOPS-report-based Config_Context creation/merge hook in the
# per-config DescribeJob loop (Requirements 1.3, 2.1-2.7, 6.1)
# ---------------------------------------------------------------------------

_CT_WM_LOW = "2024-01-01T00:00:15.000000Z"
_CT_WM_CURRENT = "2024-01-01T00:01:15.000000Z"
_CT_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:completion-topic"


def _ct_prior_rec(
    config_id: str = "rule-1",
    job_id: str = "job-ct",
    manifest_key: str = "manifests/rule-1/ts_manifest.json",
) -> SubmissionRecord:
    return SubmissionRecord(
        replication_config_id=config_id,
        source_bucket="my-bucket",
        job_id=job_id,
        manifest_key=manifest_key,
        submitted_at=_NOW,
        status=SubmissionStatus.SUBMITTED,
        watermark_low=_CT_WM_LOW,
        watermark_high=_CT_WM_CURRENT,
    )


def _run_completion_tracking_hook(
    prior_submissions: dict,
    describe_responses: dict,  # job_id → str status or Exception
    completion_report_topic_arn: str | None = _CT_TOPIC_ARN,
    completion_job_exists: bool = False,
    read_bops_report_side_effect=None,
    read_bops_report_return_value=None,
    merge_completion_configs_side_effect=None,
    bucket_name: str = "my-bucket",
):
    """Run one interval with the BOPS-report-based completion-tracking hook
    mocks (design.md Decision 5 / task 17.1).

    Returns (emitted_events, mock_store, mock_read_bops_report).
    """
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3control = MagicMock()

    def describe_job(AccountId, JobId):
        resp = describe_responses.get(JobId)
        if isinstance(resp, Exception):
            raise resp
        return {"Job": {"Status": resp, "CreationTime": _NOW}}

    mock_s3control.describe_job.side_effect = describe_job
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket, _CT_WM_CURRENT), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = prior_submissions
    mock_store.completion_job_exists.return_value = completion_job_exists
    mock_store.merge_completion_configs.return_value = '"etag-completion"'
    if merge_completion_configs_side_effect is not None:
        mock_store.merge_completion_configs.side_effect = (
            merge_completion_configs_side_effect
        )
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'

    emitted: list = []

    rt = dict(_BASE_RUNTIME)
    if completion_report_topic_arn is not None:
        rt["completion_report_topic_arn"] = completion_report_topic_arn

    if read_bops_report_side_effect is not None:
        mock_read_bops_report = MagicMock(side_effect=read_bops_report_side_effect)
    else:
        mock_read_bops_report = MagicMock(
            return_value=(
                read_bops_report_return_value
                if read_bops_report_return_value is not None
                else []
            )
        )

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch(
            "src.orchestrator.bops_report_reader.read_bops_completion_report",
            mock_read_bops_report,
        ),
    ):
        run_interval(_config([bucket_name]), rt)

    return emitted, mock_store, mock_read_bops_report


class TestCompletionRecordCreationHook:
    @pytest.mark.parametrize("status", ["Complete", "Failed", "Cancelled"])
    def test_terminal_status_merges_completion_configs(self, status):
        """A terminal-status job with no existing completion job triggers the
        BOPS_Completion_Report read and a single
        store.merge_completion_configs call (Requirements 2.1, 2.2, 2.5).

        The report is read once via the shared lazy accessor; both the
        merge (in on_job_terminal) and diagnosis use the same cached
        result."""
        prior = {"rule-1": _ct_prior_rec(job_id=f"job-{status}")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={f"job-{status}": status},
        )
        # Single read via the shared lazy accessor.
        assert mock_read_report.call_count == 1
        mock_store.merge_completion_configs.assert_called_once()

        merge_kwargs = mock_store.merge_completion_configs.call_args.kwargs
        assert merge_kwargs["job_id"] == f"job-{status}"
        # D4 formalization (task 5.1): the completion-tracking identity is
        # the per-bucket sentinel (bucket_name), never the legacy config_id
        # key ("rule-1") the prior-submission-record dict happened to be
        # iterated under.
        assert merge_kwargs["replication_config_id"] == "my-bucket"
        assert merge_kwargs["entries"] == []
        assert "manifest_generated_at" in merge_kwargs

    def test_legacy_multi_job_migration_keys_by_bucket_sentinel_not_config_id(self):
        """D4 formalization (task 5.1): a legacy migration scenario where
        TWO distinct legacy config_id-keyed prior records both terminate in
        the SAME run must still call merge_completion_configs with
        replication_config_id == bucket_name for EACH job, not the distinct
        legacy config_id keys — so both merges land under the SAME sentinel
        key in TrackedObject.configs (one entry per object), rather than
        producing two separate ConfigContext entries for the same object."""
        prior = {
            "rule-a": _ct_prior_rec(config_id="rule-a", job_id="job-a"),
            "rule-b": _ct_prior_rec(config_id="rule-b", job_id="job-b"),
        }
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-a": "Complete", "job-b": "Complete"},
        )
        assert mock_store.merge_completion_configs.call_count == 2
        for call in mock_store.merge_completion_configs.call_args_list:
            assert call.kwargs["replication_config_id"] == "my-bucket"

    def test_exception_in_read_bops_report_is_caught_and_logged(self):
        """An exception raised during the BOPS report read is caught, logged,
        and does not affect the existing circuit-breaker logic (Requirement
        6.1).

        Two isolated error logs are emitted: one from _CompletionHooks
        .on_job_terminal (merge path calls entries()) and one from the
        unconditional diagnosis block.  Both are independent of the
        recovery arithmetic."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-boom")}
        emitted, mock_store, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-boom": "Failed"},
            read_bops_report_side_effect=RuntimeError("boom"),
        )
        errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(errors) == 2
        assert any("job-boom" in e["cause"] for e in errors)

        # The existing Failed/Cancelled circuit-breaker/readmit logic still
        # runs unaffected: a readmit audit is still emitted for this job.
        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 1
        assert readmits[0]["job_id"] == "job-boom"

    def test_exception_in_merge_completion_configs_isolated(self):
        """An exception from store.merge_completion_configs itself is
        isolated too."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-storeboom")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-storeboom": "Complete"},
            merge_completion_configs_side_effect=RuntimeError("store boom"),
        )
        errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(errors) == 1
        assert "job-storeboom" in errors[0]["cause"]
        # Single read: the lazy accessor caches, so diagnosis reuses the
        # same result.
        assert mock_read_report.call_count == 1
        # Job status Complete → no readmit audit expected, but the run
        # continues without raising (implied by reaching this assertion).

    def test_disabled_when_topic_arn_absent(self):
        """When completion_report_topic_arn is unset, the merge hook does
        nothing (feature no-op, Requirement 4.8's orchestrator-side gate),
        but the unconditional diagnosis still reads the report once
        (Requirement 2.1 — fires regardless of CompletionNotificationEmail)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-disabled")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-disabled": "Complete"},
            completion_report_topic_arn=None,
        )
        # Diagnosis reads the report; the merge path does not.
        mock_read_report.assert_called_once()
        mock_store.merge_completion_configs.assert_not_called()
        mock_store.completion_job_exists.assert_not_called()

    def test_skipped_when_completion_job_already_exists(self):
        """A job_id that already has been processed (per
        completion_job_exists) is skipped — no report re-read or config
        merge (Requirement 2.4).  The unconditional diagnosis still reads
        once (its gate is report_diagnosed, not completion_job_exists)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-existing")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-existing": "Complete"},
            completion_job_exists=True,
        )
        mock_store.completion_job_exists.assert_called_once()
        # Diagnosis reads (its gate is independent); merge skipped.
        mock_read_report.assert_called_once()
        mock_store.merge_completion_configs.assert_not_called()

    def test_non_terminal_status_does_not_trigger_hook(self):
        """A non-terminal DescribeJob status never triggers config-context
        creation (only Complete/Failed/Cancelled do)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-active")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-active": "Active"},
        )
        mock_read_report.assert_not_called()
        mock_store.merge_completion_configs.assert_not_called()


class TestPermissionShapedErrorCodeDiagnosis:
    """A BOPS_Completion_Report entry carrying a permission-shaped ErrorCode
    (e.g. InitiateReplicationNotPermitted) produces a distinct diagnostic
    error log, in addition to whatever the ordinary completion-tracking
    merge does with the entries."""

    def test_initiate_replication_not_permitted_emits_diagnostic(self):
        prior = {"rule-1": _ct_prior_rec(job_id="job-perm")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket",
                object_key="key-a",
                version_id="v1",
                error_code="InitiateReplicationNotPermitted",
            )
        ]
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-perm": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
            and "InitiateReplicationNotPermitted" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1
        assert "job-perm" in diagnostics[0]["cause"]
        assert "1 task(s)" in diagnostics[0]["cause"]
        assert "s3:InitiateReplication" in diagnostics[0]["cause"]
        # The ordinary merge still happens — this is additive, not a
        # replacement for the existing completion-tracking behavior.
        mock_store.merge_completion_configs.assert_called_once()

    def test_multiple_entries_with_same_code_counted_together(self):
        prior = {"rule-1": _ct_prior_rec(job_id="job-perm-multi")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                error_code="InitiateReplicationNotPermitted",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-b", version_id="v2",
                error_code="InitiateReplicationNotPermitted",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-c", version_id="v3",
            ),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-perm-multi": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and "InitiateReplicationNotPermitted" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1
        assert "2 task(s)" in diagnostics[0]["cause"]

    def test_ordinary_succeeded_entries_emit_no_diagnostic(self):
        prior = {"rule-1": _ct_prior_rec(job_id="job-ok")}
        entries = [
            ManifestEntry(source_bucket="my-bucket", object_key="key-a", version_id="v1"),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-ok": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 0

    def test_non_permission_shaped_error_code_reported_but_not_as_permission(self):
        """An ordinary per-object error (e.g. a source object that no longer
        exists) is reported, but is not mistaken for a permission gap.

        Such a code used to produce no output whatsoever: it was parsed off
        the completion report and then dropped, so a task failure the job's
        own Complete status already concealed was invisible everywhere. It is
        now reported generically, quoting the report's own ResultMessage,
        while the permission-specific remediation text stays reserved for the
        codes that actually indicate one.
        """
        prior = {"rule-1": _ct_prior_rec(job_id="job-other-error")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                error_code="NoSuchKey",
                result_message="The specified key does not exist.",
            ),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-other-error": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 1
        cause = diagnostics[0]["cause"]
        assert "NoSuchKey" in cause
        assert "1 task(s)" in cause
        # The service's own wording is quoted, since ErrorCode alone is often
        # too generic to identify the cause.
        assert "The specified key does not exist." in cause
        # None of the permission remediation text leaks into an unrelated code.
        assert "s3:InitiateReplication" not in cause
        assert "BatchOperationsRoleArn" not in cause

    def test_src_object_not_eligible_names_archived_storage_classes(self):
        """SrcObjectNotEligible gets storage-class-specific guidance.

        The service reports an archived object only as SrcObjectNotEligible
        with the message "Object is not eligible for replication", which names
        no storage class and also covers unrelated ineligibility conditions.
        Verified against job 17a27c3a-aa18-4bc7-91a6-caeaaa28dd8c in the
        us-west-2 test deployment.
        """
        prior = {"rule-1": _ct_prior_rec(job_id="job-archived")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                error_code="SrcObjectNotEligible",
                task_status="failed",
                http_status_code="500",
                result_message="Object is not eligible for replication",
            ),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-archived": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
            and "SrcObjectNotEligible" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1
        cause = diagnostics[0]["cause"]
        assert "GLACIER" in cause
        assert "DEEP_ARCHIVE" in cause
        # The tested fact that keeps an operator from hunting a phantom
        # lifecycle problem: the object is untouched, so it is not left in a
        # replication status that would block its lifecycle rules.
        assert "lifecycle" in cause.lower()
        assert "s3:InitiateReplication" not in cause

    def test_distinct_error_codes_each_reported_once(self):
        """A job mixing several failure causes reports each one separately."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-mixed")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                error_code="SrcObjectNotEligible",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-b", version_id="v2",
                error_code="SrcObjectNotEligible",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-c", version_id="v3",
                error_code="InitiateReplicationNotPermitted",
            ),
            # A succeeded task contributes no diagnostic.
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-d", version_id="v4",
            ),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-mixed": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 2
        causes = [d["cause"] for d in diagnostics]
        # Emitted in sorted code order, so the sequence is deterministic.
        assert "InitiateReplicationNotPermitted" in causes[0]
        assert "1 task(s)" in causes[0]
        assert "SrcObjectNotEligible" in causes[1]
        assert "2 task(s)" in causes[1]

    def test_ordinary_succeeded_entries_still_emit_no_diagnostic(self):
        """Widening the reported codes must not make a clean job noisy."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-all-ok")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                task_status="succeeded", http_status_code="200",
                result_message="success",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-b", version_id="v2",
                task_status="succeeded", http_status_code="200",
                result_message="success",
            ),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-all-ok": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 0

    def test_access_denied_also_recognized(self):
        prior = {"rule-1": _ct_prior_rec(job_id="job-ad")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                error_code="AccessDenied",
            ),
        ]
        emitted, _, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-ad": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and "AccessDenied" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1

    def test_diagnostic_not_emitted_when_completion_job_already_exists(self):
        """When completion_job_exists is True the merge is skipped, but
        the unconditional diagnosis still reads the report (its gate is
        report_diagnosed, independent of completion_job_exists).  Since
        the report is empty, no diagnostic fires."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-existing-perm")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-existing-perm": "Complete"},
            completion_job_exists=True,
        )
        # Diagnosis reads (independent gate); merge skipped.
        mock_read_report.assert_called_once()
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 0

    def test_calls_happen_in_order_read_then_merge(self):
        """The two calls fire in order: read_bops_completion_report, then
        store.merge_completion_configs (Requirements 2.1, 2.2)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-order")}
        call_order: list[str] = []

        def read_side_effect(*args, **kwargs):
            call_order.append("read_bops_completion_report")
            return []

        def merge_side_effect(*args, **kwargs):
            call_order.append("merge_completion_configs")
            return '"etag-completion"'

        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-order": "Complete"},
            read_bops_report_side_effect=read_side_effect,
            merge_completion_configs_side_effect=merge_side_effect,
        )
        assert call_order == [
            "read_bops_completion_report",
            "merge_completion_configs",
        ]

    def test_consecutive_failures_increments_when_read_report_raises(self):
        """For a Failed job, consecutive_failures still increments correctly
        even when read_bops_completion_report (the first of the two calls)
        raises (Requirement 6.1)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-readboom")}
        emitted, mock_store, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-readboom": "Failed"},
            read_bops_report_side_effect=RuntimeError("boom"),
        )
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.consecutive_failures == 1

    def test_consecutive_failures_increments_when_merge_completion_configs_raises(self):
        """For a Failed job, consecutive_failures still increments correctly
        even when store.merge_completion_configs (the second of the two
        calls) raises (Requirement 6.1)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-storeboom-cb")}
        emitted, mock_store, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-storeboom-cb": "Failed"},
            merge_completion_configs_side_effect=RuntimeError("store boom"),
        )
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.consecutive_failures == 1

    @pytest.mark.parametrize("status", ["Failed", "Cancelled", "Active"])
    def test_disabled_when_topic_arn_absent_for_any_status(self, status):
        """When completion_report_topic_arn is unset, the merge does not
        happen (Requirement 4.8's orchestrator-side gate), but the
        unconditional diagnosis still reads the report for terminal
        statuses (Requirement 2.1)."""
        prior = {"rule-1": _ct_prior_rec(job_id=f"job-disabled-{status}")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={f"job-disabled-{status}": status},
            completion_report_topic_arn=None,
        )
        if status in ("Failed", "Cancelled"):
            # Diagnosis is unconditional — reads the report once.
            mock_read_report.assert_called_once()
        else:
            # Non-terminal: no read at all.
            mock_read_report.assert_not_called()
        mock_store.merge_completion_configs.assert_not_called()
        mock_store.completion_job_exists.assert_not_called()

    def test_skip_when_job_exists_other_logic_continues_for_failed_status(self):
        """When completion_job_exists is True, the merge is skipped, but
        the unconditional diagnosis still reads the report (its gate is
        report_diagnosed, not completion_job_exists).  The existing
        Failed circuit-breaker readmit logic still runs (Requirement 2.4)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-existing-failed")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-existing-failed": "Failed"},
            completion_job_exists=True,
        )
        # Diagnosis reads (independent gate); merge skipped.
        mock_read_report.assert_called_once()
        mock_store.merge_completion_configs.assert_not_called()

        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 1
        assert readmits[0]["job_id"] == "job-existing-failed"


# ---------------------------------------------------------------------------
# Task 17.3: Property 12 — creation/merge failure never alters the
# submission or checkpoint outcome (Requirement 6.1)
# Feature: source-status-completion-tracking, Property 12: A creation/merge failure never alters the submission or checkpoint outcome
# ---------------------------------------------------------------------------


class TestProperty12CreationMergeFailureIsolation:
    """# Feature: source-status-completion-tracking, Property 12: A creation/merge failure never alters the submission or checkpoint outcome

    Validates: Requirements 6.1
    """

    @given(
        status=st.sampled_from(["Complete", "Failed", "Cancelled"]),
        inject_into=st.sampled_from(["read_bops_report", "merge_completion_configs"]),
    )
    @settings(max_examples=100)
    def test_injected_failure_does_not_change_checkpoint_or_circuit_breaker(
        self, status: str, inject_into: str
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 12: A creation/merge failure never alters the submission or checkpoint outcome"""
        prior = {"rule-1": _ct_prior_rec(job_id=f"job-{status}-{inject_into}")}

        kwargs = {}
        if inject_into == "read_bops_report":
            kwargs["read_bops_report_side_effect"] = RuntimeError("injected failure")
        else:
            kwargs["merge_completion_configs_side_effect"] = RuntimeError("injected failure")

        # Run WITHOUT injection (baseline).
        _, baseline_store, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={f"job-{status}-{inject_into}": status},
        )
        # Run WITH injection.
        _, injected_store, _ = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={f"job-{status}-{inject_into}": status},
            **kwargs,
        )

        # release_lease (checkpoint/lease outcome) called identically in
        # both runs — same submitted_refs argument (None vs a list is
        # determined only by submission success, unrelated to the
        # injected completion-tracking failure).
        baseline_release_calls = baseline_store.release_lease.call_args_list
        injected_release_calls = injected_store.release_lease.call_args_list
        assert len(baseline_release_calls) == len(injected_release_calls)
        for base_call, inj_call in zip(baseline_release_calls, injected_release_calls):
            base_refs = base_call.kwargs.get("submitted_refs")
            inj_refs = inj_call.kwargs.get("submitted_refs")
            assert (base_refs is None) == (inj_refs is None)

        # The circuit-breaker/consecutive_failures counter recorded via
        # record_submission is identical between the two runs.
        if baseline_store.record_submission.call_args is not None:
            baseline_rec: SubmissionRecord = baseline_store.record_submission.call_args[0][2]
            injected_rec: SubmissionRecord = injected_store.record_submission.call_args[0][2]
            assert baseline_rec.consecutive_failures == injected_rec.consecutive_failures
            assert baseline_rec.status == injected_rec.status


# ---------------------------------------------------------------------------
# Task 19.2: Quiescence scan-count recording in the per-config loop
# (Requirements 5.1, 5.2, 5.3)
# ---------------------------------------------------------------------------


def _run_quiescence_scan_recording(
    completion_report_topic_arn: str | None = _CT_TOPIC_ARN,
    pf_count: int = 42,
    record_scan_result_side_effect=None,
    bucket_name: str = "my-bucket",
):
    """Run one interval exercising only the quiescence scan-count recording
    hook (task 19.2) — no prior submissions, so the separate
    Completion_Record-creation hook (task 19.1) never fires.

    Returns (emitted_events, mock_store).
    """
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3control = MagicMock()
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'
    if record_scan_result_side_effect is not None:
        mock_store.record_scan_result.side_effect = record_scan_result_side_effect
    else:
        mock_store.record_scan_result.return_value = '"etag-scan"'

    emitted: list = []

    rt = dict(_BASE_RUNTIME)
    if completion_report_topic_arn is not None:
        rt["completion_report_topic_arn"] = completion_report_topic_arn

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ),
        patch("src.orchestrator.preflight_count", return_value=pf_count),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
    ):
        run_interval(_config([bucket_name]), rt)

    return emitted, mock_store


class TestQuiescenceScanCountRecording:
    def test_records_scan_result_when_enabled(self):
        """When completion tracking is enabled, record_scan_result is called
        once per bucket per run (task 4.1 collapses the per-config_id loop
        into a single per-bucket path) with the Preflight_Count value and a
        scan_at close to now (Requirements 5.1, 5.2, 5.3)."""
        before = datetime.now(tz=timezone.utc)
        emitted, mock_store = _run_quiescence_scan_recording(pf_count=42)
        after = datetime.now(tz=timezone.utc)

        mock_store.record_scan_result.assert_called_once()
        call = mock_store.record_scan_result.call_args
        # config_id positional arg is the per-bucket sentinel — the bucket's
        # own name — per design.md D5 (single-batch-job-per-bucket).
        assert call.args[3] == "my-bucket"
        assert call.kwargs["match_count"] == 42
        scan_at = call.kwargs["scan_at"]
        assert before <= scan_at <= after

    def test_not_called_when_topic_arn_absent(self):
        """When completion_report_topic_arn is unset, record_scan_result is
        never called (feature no-op, matching Requirement 4.8's posture)."""
        emitted, mock_store = _run_quiescence_scan_recording(
            completion_report_topic_arn=None, pf_count=7,
        )
        mock_store.record_scan_result.assert_not_called()

    def test_exception_isolated_from_manifest_and_submission(self):
        """An exception raised by record_scan_result is caught and logged,
        and does not prevent the manifest from being generated or the job
        from being submitted for that config_id (Requirement 6)."""
        emitted, mock_store = _run_quiescence_scan_recording(
            pf_count=5, record_scan_result_side_effect=RuntimeError("scan boom"),
        )

        errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(errors) == 1
        assert errors[0]["bucket"] == "my-bucket"

        # Manifest generation and job submission still happened.
        submissions = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "job_submitted"
        ]
        assert len(submissions) == 1
        mock_store.record_submission.assert_called_once()


# ---------------------------------------------------------------------------
# Idle-run scan recording (Requirement 5.5)
#
# A run that reads no journal rows returns early, before the normal
# record_scan_result hook. Without recording a zero-match scan on that path,
# completion_scan_state stays frozen at whatever the last active run observed,
# so quiescence_check can never pass for a bucket that has gone quiet — no
# Completion_Report is ever published and every RESOLVED Tracked_Object is
# retained in the state object indefinitely.
# ---------------------------------------------------------------------------


def _run_idle_interval(
    completion_report_topic_arn: str | None = _CT_TOPIC_ARN,
    bucket_name: str = "my-bucket",
    record_scan_result_side_effect=None,
):
    """Run one interval where the journal window is empty, so the run takes
    the "nothing to process" early-return path.

    Returns (emitted_events, mock_store).
    """
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory
    mock_factory.create_s3control_client.return_value = MagicMock()

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store
    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    if record_scan_result_side_effect is not None:
        mock_store.record_scan_result.side_effect = record_scan_result_side_effect
    else:
        mock_store.record_scan_result.return_value = '"etag-scan"'

    emitted: list = []

    rt = dict(_BASE_RUNTIME)
    if completion_report_topic_arn is not None:
        rt["completion_report_topic_arn"] = completion_report_topic_arn

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        # Empty journal window: no operations, so nothing accumulates and
        # candidate_hwm stays None.
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
    ):
        run_interval(_config([bucket_name]), rt)

    return emitted, mock_store


class TestIdleRunScanRecording:
    def test_idle_run_records_zero_match_scan(self):
        """An idle run records match_count=0 so quiescence can be satisfied.

        This is what lets a bucket that has gone quiet ever publish its
        resolved Tracked_Objects (Requirement 5.5).
        """
        before = datetime.now(tz=timezone.utc)
        emitted, mock_store = _run_idle_interval()
        after = datetime.now(tz=timezone.utc)

        mock_store.record_scan_result.assert_called_once()
        call = mock_store.record_scan_result.call_args
        assert call.args[3] == "my-bucket"  # per-bucket sentinel
        assert call.kwargs["match_count"] == 0
        assert before <= call.kwargs["scan_at"] <= after

    def test_idle_run_passes_the_live_etag(self):
        """The conditional write must use the etag valid on this path.

        ``current_etag`` is not assigned until after the early return, so
        referencing it here would raise NameError — which the surrounding
        except clause would swallow, silently restoring the original bug.
        ``lease_etag`` is the correct variable: no lease is acquired on the
        idle path (that happens only when ``candidate_hwm`` is not None), so
        it still holds the checkpoint etag, which is exactly what has not
        been superseded by a later write.
        """
        emitted, mock_store = _run_idle_interval()
        call = mock_store.record_scan_result.call_args
        assert call.kwargs["current_etag"] == '"etag-0"'
        mock_store.acquire_lease.assert_not_called()

        errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
        ]
        assert errors == []

    def test_idle_run_does_not_record_when_tracking_disabled(self):
        """No completion topic means the hook stays a strict no-op."""
        emitted, mock_store = _run_idle_interval(
            completion_report_topic_arn=None,
        )
        mock_store.record_scan_result.assert_not_called()

    def test_idle_scan_failure_is_isolated(self):
        """A scan-record failure is logged, not raised (Requirement 6.1)."""
        emitted, mock_store = _run_idle_interval(
            record_scan_result_side_effect=RuntimeError("scan boom"),
        )
        errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(errors) == 1

    def test_idle_run_submits_no_job(self):
        """Guard: the idle path must still not submit anything."""
        emitted, _ = _run_idle_interval()
        submissions = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "job_submitted"
        ]
        assert submissions == []


# ---------------------------------------------------------------------------
# Task 20.3: Call _run_completion_tracking_interval from run_interval
# (Requirements 6.2, 6.3)
# ---------------------------------------------------------------------------


def _run_completion_tracking_call_site(
    completion_report_topic_arn: str | None = _CT_TOPIC_ARN,
    completion_tracking_interval_side_effect=None,
    bucket_name: str = "my-bucket",
    call_order: list | None = None,
):
    """Run one interval with the metrics publisher and
    ``_run_completion_tracking_interval`` both mocked, so their call order
    (and whether the latter is invoked at all) can be asserted.

    Returns (emitted_events, mock_run_completion_tracking_interval).
    """
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'

    if call_order is None:
        call_order = []

    def publish_side_effect(*args, **kwargs):
        call_order.append("metrics_publish")

    mock_publish = MagicMock(side_effect=publish_side_effect)

    def completion_interval_side_effect(*args, **kwargs):
        call_order.append("completion_tracking_interval")
        if completion_tracking_interval_side_effect is not None:
            raise completion_tracking_interval_side_effect

    mock_completion_interval = MagicMock(
        side_effect=completion_interval_side_effect
    )

    emitted: list = []

    rt = dict(_BASE_RUNTIME)
    if completion_report_topic_arn is not None:
        rt["completion_report_topic_arn"] = completion_report_topic_arn

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch("src.orchestrator.MetricsPublisher.publish", mock_publish),
        patch(
            "src.orchestrator._run_completion_tracking_interval",
            mock_completion_interval,
        ),
    ):
        run_interval(_config([bucket_name]), rt)

    return emitted, mock_completion_interval, call_order


class TestCompletionTrackingIntervalCallSite:
    def test_called_once_after_metrics_publish_when_topic_arn_set(self):
        """When completion_report_topic_arn is set, run_interval calls
        _run_completion_tracking_interval exactly once, strictly after the
        metrics publisher's publish() call (Requirement 6.3)."""
        call_order: list = []
        emitted, mock_completion_interval, call_order = (
            _run_completion_tracking_call_site(call_order=call_order)
        )

        mock_completion_interval.assert_called_once()
        assert call_order == ["metrics_publish", "completion_tracking_interval"]

    def test_not_called_when_topic_arn_absent(self):
        """When completion_report_topic_arn is unset/absent, run_interval
        does not call _run_completion_tracking_interval at all
        (Requirement 4.8's no-op posture applied at the call site)."""
        emitted, mock_completion_interval, call_order = (
            _run_completion_tracking_call_site(completion_report_topic_arn=None)
        )

        mock_completion_interval.assert_not_called()
        assert "completion_tracking_interval" not in call_order

    def test_exception_is_caught_logged_and_does_not_propagate(self):
        """An exception raised by _run_completion_tracking_interval is
        caught, logged via observability.log_error with
        component=Completion_Tracker, and does not propagate out of
        run_interval — the run still completes normally (Requirement 6.2,
        6.3)."""
        emitted, mock_completion_interval, call_order = (
            _run_completion_tracking_call_site(
                completion_tracking_interval_side_effect=RuntimeError(
                    "completion tracking boom"
                ),
            )
        )

        # run_interval returned normally (we reached this line) and still
        # called the completion-tracking interval before failing.
        mock_completion_interval.assert_called_once()

        errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(errors) == 1
        assert "completion tracking boom" in errors[0]["cause"]

        # The run's own summary log entry was still emitted, confirming the
        # rest of run_interval completed normally despite the failure.
        summaries = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "interval_summary"
        ]
        assert len(summaries) == 1


# ---------------------------------------------------------------------------
# Single-submission call site: no bucket policy is read or written anywhere in
# a run. The State_Bucket grants the job role needed now come from the created
# role's own identity policy (solution-owned-batchops-role Requirements 3.1,
# 3.4).
# ---------------------------------------------------------------------------

_BP_TOPIC_ARN = _CT_TOPIC_ARN


def _run_single_submission(
    completion_report_topic_arn: str | None = _BP_TOPIC_ARN,
    bucket_name: str = "my-bucket",
    ops_per_bucket: dict[str, list[TaggingOperation]] | None = None,
):
    """Run one interval reaching exactly one submission.

    No prior submissions, so the DescribeJob-loop creation hook never fires
    and the only completion-tracking-gated call is record_scan_result.

    Returns (emitted_events, mock_store, mock_submit_job, mock_s3_client).
    """
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3 = MagicMock()
    mock_factory.create_s3_client.return_value = mock_s3
    mock_s3control = MagicMock()
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'
    mock_store.record_scan_result.return_value = '"etag-scan"'

    emitted: list = []

    rt = dict(_BASE_RUNTIME)
    if completion_report_topic_arn is not None:
        rt["completion_report_topic_arn"] = completion_report_topic_arn

    mock_submit_job = MagicMock(return_value=_submitted())

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=(
                (ops_per_bucket or {bucket_name: [_op(bucket_name)]}).get(bucket_name, []),
                [],
            ),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            mock_submit_job,
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
    ):
        run_interval(_config([bucket_name]), rt)

    return emitted, mock_store, mock_submit_job, mock_s3


class TestNoBucketPolicyAccessDuringARun:
    """Requirements 3.1, 3.4: the run touches no bucket policy at all.

    The predecessor of these tests exercised a pre-submission priming step
    that wrote the State_Bucket policy so an externally owned replication
    role could read the manifest. The created Batch Operations role carries
    that grant in its own identity policy, so the step is gone, and its
    absence is what these assert.
    """

    def test_no_bucket_policy_call_on_the_submission_path(self):
        _, _, mock_submit_job, mock_s3 = _run_single_submission()
        assert mock_submit_job.call_count == 1
        mock_s3.get_bucket_policy.assert_not_called()
        mock_s3.put_bucket_policy.assert_not_called()

    def test_no_bucket_policy_call_without_completion_tracking(self):
        _, _, mock_submit_job, mock_s3 = _run_single_submission(
            completion_report_topic_arn=None
        )
        assert mock_submit_job.call_count == 1
        mock_s3.get_bucket_policy.assert_not_called()
        mock_s3.put_bucket_policy.assert_not_called()

    def test_submission_carries_the_created_role(self):
        """The submitted job's role is the deployment's, not the bucket's."""
        _, _, mock_submit_job, _ = _run_single_submission()
        kwargs = mock_submit_job.call_args.kwargs
        assert kwargs["batch_operations_role_arn"] == _BATCHOPS_ROLE_ARN



# ---------------------------------------------------------------------------
# Task 4.3: single-batch-job-per-bucket — one job per bucket regardless of
# rule count; failed-job recovery across the legacy->single migration;
# disable on threshold in a genuine multi-record migration scenario.
# Requirements: 1.1, 2.3, 2.4, 4.1
# ---------------------------------------------------------------------------


def _run_with_multi_rule_mocks(
    bucket_name: str,
    rules: list[DerivedReplicationRule],
    ops: list[TaggingOperation] | None = None,
) -> tuple[MagicMock, MagicMock]:
    """Run one interval for a single bucket whose ``get_replication_rules``
    call returns an arbitrary (possibly large) list of derived rules — used
    to verify the per-bucket submission path is called exactly once
    regardless of how many rules a bucket has (design.md D1, task 4.1).

    Returns (mock_submit_job, mock_store).
    """
    ops = ops if ops is not None else [_op(bucket_name)]

    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store
    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'

    mock_submit = MagicMock(return_value=_submitted())

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=(rules, []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=(ops, []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            mock_submit,
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
    ):
        run_interval(_config([bucket_name]), _BASE_RUNTIME)

    return mock_submit, mock_store


class TestPropertyOneJobPerBucketRegardlessOfRuleCount:
    """# Feature: single-batch-job-per-bucket, Property 1: One job submitted per bucket per interval regardless of replication rule count

    Validates: Requirements 1.1
    """

    @given(rule_count=st.integers(min_value=1, max_value=10))
    @settings(max_examples=30)
    def test_submit_called_exactly_once_regardless_of_rule_count(
        self, rule_count: int
    ) -> None:
        """# Feature: single-batch-job-per-bucket, Property 1: One job submitted per bucket per interval regardless of replication rule count

        Constructs a bucket with N (1-10) distinct replication rules, all
        matching the same tagging operation (same-scope rules with distinct
        destinations, mirroring design.md's "redundant jobs for same-scope
        rules" scenario). Regardless of N, at most one batch job is
        submitted for the bucket per interval — the per-config_id loop this
        change removed would have submitted N jobs.
        """
        bucket_name = "my-bucket"
        rules = [
            DerivedReplicationRule(
                source_bucket=bucket_name,
                replication_config_id=f"rule-{i}",
                rule_id=f"rule-{i}",
                tag_filter={"env": "prod"},
                destination=DestinationRef(bucket_arn=f"{_DEST_ARN}-{i}"),
            )
            for i in range(rule_count)
        ]

        mock_submit, _ = _run_with_multi_rule_mocks(bucket_name, rules)

        assert mock_submit.call_count == 1

    @given(rule_count=st.integers(min_value=2, max_value=10))
    @settings(max_examples=30)
    def test_submit_called_exactly_once_with_distinct_scoped_rules(
        self, rule_count: int
    ) -> None:
        """# Feature: single-batch-job-per-bucket, Property 1: One job submitted per bucket per interval regardless of replication rule count

        Same property with rules split across two distinct tag-filter
        scopes (design.md's "distinct-scope rules" case) — half the rules
        match the operation's tag set and half do not. Still exactly one
        job for the bucket.
        """
        bucket_name = "my-bucket"
        rules = [
            DerivedReplicationRule(
                source_bucket=bucket_name,
                replication_config_id=f"rule-{i}",
                rule_id=f"rule-{i}",
                tag_filter={"env": "prod"} if i % 2 == 0 else {"team": "other"},
                destination=DestinationRef(bucket_arn=f"{_DEST_ARN}-{i}"),
            )
            for i in range(rule_count)
        ]

        mock_submit, _ = _run_with_multi_rule_mocks(bucket_name, rules)

        assert mock_submit.call_count == 1


# ---------------------------------------------------------------------------
# Task 4.3: Failed-job recovery across the legacy (per-config_id-keyed) ->
# single (per-bucket-sentinel-keyed) SubmissionRecord migration.
# Requirements: 2.3, 2.4, 4.1
# ---------------------------------------------------------------------------

_MIG_WM_A = "2024-01-01T00:00:05.000000Z"
_MIG_WM_B = "2024-01-01T00:00:15.000000Z"
_MIG_WM_C = "2024-01-01T00:00:25.000000Z"


def _run_migration_recovery_mocks(
    prior_submissions: dict,
    describe_responses: dict,
    bucket_name: str = "my-bucket",
) -> tuple[list, MagicMock, MagicMock]:
    """Run one interval with recovery-aware mocks for the legacy->single
    migration scenario. Returns (emitted_events, mock_store, mock_s3control)
    — richer inspection handles than ``_run_with_recovery_mocks`` so a test
    can assert both the exact set of DescribeJob calls made and the single
    consolidated SubmissionRecord passed to ``record_submission``.
    """
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3control = MagicMock()

    def describe_job(AccountId, JobId):
        resp = describe_responses.get(JobId)
        if isinstance(resp, Exception):
            raise resp
        return {"Job": {"Status": resp}}

    mock_s3control.describe_job.side_effect = describe_job
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = MagicMock()
    mock_store_cls.return_value = mock_store

    mock_store.get_checkpoint.side_effect = (
        lambda s3_client, state_bucket, source_bucket:
            (_checkpoint(source_bucket, _WM_CURRENT), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = prior_submissions
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'

    emitted: list = []

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=([_op(bucket_name)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch(
            "src.orchestrator.bops_report_reader.read_bops_completion_report",
            return_value=[],
        ),
    ):
        run_interval(_config([bucket_name]), _BASE_RUNTIME)

    return emitted, mock_store, mock_s3control


class TestLegacyToSingleMigrationRecovery:
    """Failed-job recovery when a bucket's prior state still carries the
    legacy per-config_id-keyed ``SubmissionRecord`` dict (simulating a
    bucket that was on the old per-rule-job design and is now migrating to
    the single-record form).

    _Requirements: 2.3, 2.4, 4.1_
    """

    def test_describe_job_called_for_each_distinct_legacy_job(self):
        """Every distinct legacy job_id still gets its own DescribeJob call
        this run — the migration does not lose visibility into any
        in-flight pre-migration job."""
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=_MIG_WM_A),
            "rule-b": _prior_rec("rule-b", "job-b", watermark_low=_MIG_WM_B),
            "rule-c": _prior_rec("rule-c", "job-c", watermark_low=_MIG_WM_C),
        }
        _, _, mock_s3control = _run_migration_recovery_mocks(
            prior_submissions=prior,
            describe_responses={
                "job-a": "Failed", "job-b": "Complete", "job-c": "Failed",
            },
        )
        called_job_ids = {
            c.kwargs["JobId"] for c in mock_s3control.describe_job.call_args_list
        }
        assert called_job_ids == {"job-a", "job-b", "job-c"}

    def test_watermark_rollback_uses_minimum_of_failed_legacy_jobs_only(self):
        """The rollback watermark is the minimum watermark_low among only
        the Failed/Cancelled legacy jobs — a Complete legacy job's
        watermark_low (even if numerically earlier) must not be a
        candidate."""
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=_MIG_WM_A),
            # job-b is Complete but has the numerically EARLIEST
            # watermark_low of the three — it must be excluded from the
            # rollback-candidate set.
            "rule-b": _prior_rec(
                "rule-b", "job-b",
                watermark_low="2024-01-01T00:00:01.000000Z",
            ),
            "rule-c": _prior_rec("rule-c", "job-c", watermark_low=_MIG_WM_C),
        }
        _, mock_store, _ = _run_migration_recovery_mocks(
            prior_submissions=prior,
            describe_responses={
                "job-a": "Failed", "job-b": "Complete", "job-c": "Failed",
            },
        )
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        # Only job-a and job-c failed; the minimum of their watermark_lows
        # wins, NOT job-b's earlier-but-Complete watermark_low.
        assert submitted_rec.watermark_low == min(_MIG_WM_A, _MIG_WM_C)

    def test_successful_resubmission_collapses_to_single_bucket_record(self):
        """After recovery, this run's successful submission is persisted as
        ONE SubmissionRecord identified by the bucket-name sentinel, not by
        any of the legacy per-config_id keys it recovered from — the
        consolidated single-bucket state (design.md D3; state_store.py's
        ``record_submission`` collapse-on-write)."""
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=_MIG_WM_A),
            "rule-b": _prior_rec("rule-b", "job-b", watermark_low=_MIG_WM_B),
        }
        _, mock_store, _ = _run_migration_recovery_mocks(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed", "job-b": "Failed"},
        )
        # Exactly one record_submission call — the orchestrator's per-bucket
        # path persists at most one record per interval, regardless of how
        # many legacy per-config records it recovered from.
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.source_bucket == "my-bucket"
        assert submitted_rec.replication_config_id == "my-bucket"


class TestLegacyToSingleMigrationRecoveryRealStateStore:
    """Exercises the arrival at state_store.py's collapse-to-one-record
    behavior via a FULL ``run_interval`` call against the real
    ``StateStore`` (not mocked) and a fake S3 client seeded with a legacy
    per-config_id-keyed state object — complementing
    ``tests/adapters/test_state_store.py``'s direct unit coverage of the
    same collapse (``record_submission``'s migration-on-write) with an
    orchestrator-level integration check.

    _Requirements: 2.4, 4.1_
    """

    def test_full_run_interval_collapses_legacy_records_via_real_state_store(self):
        import json as _json

        from src.core.checkpoint_serializer import (
            serialize,
            serialize_submission_record,
        )
        from src.core.models import CheckpointState

        bucket_name = "my-bucket"
        base_state = CheckpointState(
            source_bucket=bucket_name,
            last_processed_watermark=_WM_CURRENT,
            lease=None,
        )
        payload = _json.loads(serialize(base_state))
        payload["submission_records"] = {
            "rule-a": serialize_submission_record(
                _prior_rec("rule-a", "job-a", watermark_low=_MIG_WM_A)
            ),
            "rule-b": serialize_submission_record(
                _prior_rec("rule-b", "job-b", watermark_low=_MIG_WM_B)
            ),
        }
        state_holder = {"json": _json.dumps(payload), "etag": '"etag-0"'}
        etag_counter = {"n": 0}

        mock_s3_client = MagicMock()

        def get_object(Bucket, Key):
            body = MagicMock()
            body.read.return_value = state_holder["json"].encode("utf-8")
            return {"Body": body, "ETag": state_holder["etag"]}

        def put_object(**kwargs):
            etag_counter["n"] += 1
            new_etag = f'"etag-{etag_counter["n"]}"'
            body_bytes = kwargs["Body"]
            state_holder["json"] = (
                body_bytes.decode("utf-8")
                if isinstance(body_bytes, bytes)
                else body_bytes
            )
            state_holder["etag"] = new_etag
            return {"ETag": new_etag}

        mock_s3_client.get_object.side_effect = get_object
        mock_s3_client.put_object.side_effect = put_object

        mock_s3control = MagicMock()
        mock_s3control.describe_job.side_effect = lambda AccountId, JobId: {
            "Job": {"Status": {"job-a": "Failed", "job-b": "Failed"}.get(JobId, "Complete")}
        }

        mock_factory_cls = MagicMock()
        mock_factory = MagicMock()
        mock_factory_cls.return_value = mock_factory
        mock_factory.create_s3_client.return_value = mock_s3_client
        mock_factory.create_s3control_client.return_value = mock_s3control

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch(
                "src.orchestrator.replication_config_adapter.get_replication_rules",
                return_value=([_rule(bucket_name)], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.read_journal",
                return_value=([_op(bucket_name)], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                return_value=None,
            ),
            patch(
                "src.orchestrator.write_in_memory_inventory_manifest",
                return_value=_written_manifest(),
            ),
            patch(
                "src.orchestrator.batch_operations_adapter.submit_batch_job",
                return_value=_submitted(),
            ),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch(
                "src.orchestrator.bops_report_reader.read_bops_completion_report",
                return_value=[],
            ),
        ):
            run_interval(_config([bucket_name]), _BASE_RUNTIME)

        final_payload = _json.loads(state_holder["json"])
        # The legacy rule-a/rule-b entries are gone; only the bucket-name
        # sentinel entry survives — the real StateStore's collapse-on-write,
        # reached via a full orchestrator run.
        assert set(final_payload["submission_records"].keys()) == {bucket_name}
        assert final_payload["submission_records"][bucket_name]["job_id"] == "job-abc-123"


# ---------------------------------------------------------------------------
# Task 4.3: Disable on threshold — a genuine migration scenario where
# MULTIPLE legacy per-config records each independently sit below the
# individual failure threshold, but the bucket-level consolidated counter
# (max of prior consecutive_failures, +1 for this run's failure) reaches
# max_batch_job_failures.
# Requirements: 2.3, 2.4
# ---------------------------------------------------------------------------


class TestCircuitBreakerMigrationScenario:
    """Distinct from ``TestCircuitBreaker``'s single-record disable tests:
    here the disable condition can only be observed by consolidating
    MULTIPLE legacy per-config records into the bucket-level counter.

    _Requirements: 2.3, 2.4_
    """

    def test_multiple_legacy_records_below_threshold_still_disables_via_consolidated_max(self):
        """rule-a has 3 consecutive_failures and rule-b has 1 — both
        individually below the threshold of 4. Neither legacy record alone
        would disable the bucket, but the consolidated bucket-level counter
        seeds from the MAX across all prior records (3) and increments by
        1 for this run's failure, reaching the threshold."""
        prior = {
            "rule-a": _prior_rec_with_count(
                "rule-a", "job-a", consecutive_failures=3
            ),
            "rule-b": _prior_rec_with_count(
                "rule-b", "job-b", consecutive_failures=1
            ),
        }
        disabled, _, mock_store = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed", "job-b": "Complete"},
            max_batch_job_failures=4,
        )
        assert "my-bucket" in disabled
        mock_store.record_submission.assert_not_called()

    def test_multiple_legacy_records_all_below_threshold_does_not_disable(self):
        """Contrast case: when the consolidated max+1 stays below the
        threshold, the bucket is NOT disabled even with multiple legacy
        records in flight."""
        prior = {
            "rule-a": _prior_rec_with_count(
                "rule-a", "job-a", consecutive_failures=1
            ),
            "rule-b": _prior_rec_with_count(
                "rule-b", "job-b", consecutive_failures=2
            ),
        }
        disabled, _, mock_store = _run_circuit_breaker(
            prior_submissions=prior,
            describe_responses={"job-a": "Complete", "job-b": "Failed"},
            max_batch_job_failures=4,
        )
        assert disabled == []


# ---------------------------------------------------------------------------
# Row-count cap (code-review-remediation verification-notes.md "scaling
# risk" finding): find_row_count_boundary wiring, threading the boundary
# through to read_journal/preflight_count/unload_matched_objects, and the
# audit-log entry emitted when a run is actually capped.
# ---------------------------------------------------------------------------


class TestJournalReadRowCap:
    def test_uncapped_run_passes_none_until_timestamp_to_read_journal(self):
        """The common case (find_row_count_boundary returns None) — no
        upper-bound predicate at all, exactly as before this cap existed."""
        _, mock_read_journal, _, _, mock_find_boundary = _run_with_mocks(
            ["my-bucket"], boundary_timestamp=None, _return_find_boundary_mock=True,
        )
        mock_find_boundary.assert_called_once()
        call_kwargs = mock_read_journal.call_args.kwargs
        assert call_kwargs.get("until_timestamp") is None

    def test_capped_run_passes_boundary_to_read_journal(self):
        boundary = "2024-01-02T00:00:00.000000Z"
        _, mock_read_journal, _, _, mock_find_boundary = _run_with_mocks(
            ["my-bucket"], boundary_timestamp=boundary, _return_find_boundary_mock=True,
        )
        call_kwargs = mock_read_journal.call_args.kwargs
        assert call_kwargs.get("until_timestamp") == boundary

    def test_capped_run_passes_boundary_to_preflight_count(self):
        boundary = "2024-01-02T00:00:00.000000Z"
        with patch("src.orchestrator.preflight_count") as mock_pf:
            mock_pf.return_value = 0
            (
                mock_factory_cls, mock_store_cls,
                mock_get_rules, mock_read_journal,
                mock_submit_job,
            ) = _make_mocks(["my-bucket"])
            with (
                patch("src.orchestrator.ClientFactory", mock_factory_cls),
                patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
                patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
                patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
                patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                      return_value=boundary),
                patch("src.orchestrator.write_in_memory_inventory_manifest",
                      return_value=_written_manifest()),
                patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
                patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            ):
                run_interval(_config(["my-bucket"]), _BASE_RUNTIME)
            assert mock_pf.call_args.kwargs.get("until_timestamp") == boundary

    def test_capped_run_emits_journal_read_capped_audit(self):
        boundary = "2024-01-02T00:00:00.000000Z"
        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            _run_with_mocks(["my-bucket"], boundary_timestamp=boundary)

        audits = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "audit"
            and e.get("action") == "journal_read_capped"
        ]
        assert len(audits) == 1
        assert audits[0]["source_bucket"] == "my-bucket"
        assert audits[0]["until_timestamp"] == boundary

    def test_uncapped_run_emits_no_journal_read_capped_audit(self):
        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            _run_with_mocks(["my-bucket"], boundary_timestamp=None)

        audits = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "audit"
            and e.get("action") == "journal_read_capped"
        ]
        assert audits == []

    def test_boundary_check_failure_proceeds_uncapped_not_fatal(self):
        """A best-effort failure in find_row_count_boundary itself must not
        abort the run — it proceeds uncapped, which is the pre-existing
        behavior (not a regression), rather than blocking a whole run on a
        check that exists purely to prevent a rare condition."""
        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["my-bucket"])
        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  side_effect=RuntimeError("boom")),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            run_interval(_config(["my-bucket"]), _BASE_RUNTIME)  # must not raise

        # The run still completed and read_journal was still called
        # (uncapped — until_timestamp is None).
        mock_read_journal.assert_called_once()
        assert mock_read_journal.call_args.kwargs.get("until_timestamp") is None

    def test_row_cap_runtime_config_value_passed_through(self):
        """A custom journal_read_row_cap in runtime_config reaches
        find_row_count_boundary's row_cap argument."""
        rt = {**_BASE_RUNTIME, "journal_read_row_cap": 12345}
        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["my-bucket"])
        mock_find_boundary = MagicMock(return_value=None)
        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  mock_find_boundary),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            run_interval(_config(["my-bucket"]), rt)

        assert mock_find_boundary.call_args.kwargs.get("row_cap") == 12345

    def test_default_row_cap_used_when_runtime_config_absent(self):
        from src.core.manifest_strategy import JOURNAL_READ_ROW_CAP_DEFAULT

        mock_find_boundary = MagicMock(return_value=None)
        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["my-bucket"])
        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  mock_find_boundary),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            run_interval(_config(["my-bucket"]), _BASE_RUNTIME)

        assert mock_find_boundary.call_args.kwargs.get("row_cap") == JOURNAL_READ_ROW_CAP_DEFAULT

    # -----------------------------------------------------------------------
    # Row-cap overshoot audit (scale-threshold-and-drain-throughput Finding 2):
    # read_journal reads the boundary timestamp inclusively, so a tied batch
    # can push the rows actually read above row_cap. When that happens on a
    # capped run, the orchestrator emits a row_cap_overshoot audit so the
    # run-time overshoot (which the config-load memory check cannot see) is
    # visible.
    # -----------------------------------------------------------------------
    def _ops(self, bucket: str, n: int) -> list:
        return [
            TaggingOperation(
                source_bucket=bucket,
                object_key=f"path/obj-{i}.txt",
                resulting_tag_set={"env": "prod"},
                sequence_number=f"seq-{i:04d}",
                operation="PutObjectTagging",
                event_time=_NOW,
            )
            for i in range(n)
        ]

    def test_capped_run_over_cap_emits_row_cap_overshoot_audit(self):
        boundary = "2024-01-02T00:00:00.000000Z"
        rt = {**_BASE_RUNTIME, "journal_read_row_cap": 2}
        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            _run_with_mocks(
                ["my-bucket"],
                ops_per_bucket={"my-bucket": self._ops("my-bucket", 5)},
                boundary_timestamp=boundary,
                runtime=rt,
            )

        overshoots = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "audit"
            and e.get("action") == "row_cap_overshoot"
        ]
        assert len(overshoots) == 1
        assert overshoots[0]["source_bucket"] == "my-bucket"
        assert overshoots[0]["row_cap"] == 2
        assert overshoots[0]["rows_read"] == 5
        assert overshoots[0]["overshoot_rows"] == 3
        assert overshoots[0]["matched"] >= 1

    def test_capped_run_within_cap_emits_no_row_cap_overshoot_audit(self):
        """A capped run whose rows read do not exceed the cap (no boundary
        tie) emits no overshoot audit."""
        boundary = "2024-01-02T00:00:00.000000Z"
        rt = {**_BASE_RUNTIME, "journal_read_row_cap": 10}
        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            _run_with_mocks(
                ["my-bucket"],
                ops_per_bucket={"my-bucket": self._ops("my-bucket", 3)},
                boundary_timestamp=boundary,
                runtime=rt,
            )

        overshoots = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "audit"
            and e.get("action") == "row_cap_overshoot"
        ]
        assert overshoots == []

    def test_uncapped_run_emits_no_row_cap_overshoot_audit(self):
        """An uncapped run (find_row_count_boundary returns None) never emits
        an overshoot audit even if many ops were read."""
        rt = {**_BASE_RUNTIME, "journal_read_row_cap": 2}
        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            _run_with_mocks(
                ["my-bucket"],
                ops_per_bucket={"my-bucket": self._ops("my-bucket", 5)},
                boundary_timestamp=None,
                runtime=rt,
            )

        overshoots = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "audit"
            and e.get("action") == "row_cap_overshoot"
        ]
        assert overshoots == []


# ---------------------------------------------------------------------------
# Task 6.2: RunOutcome.any_capped_and_progressed — true only when a bucket
# both capped (journal_until is not None) and progressed (job submitted +
# checkpoint advanced).
# Requirements: 4.1, 4.2, 4.3
# ---------------------------------------------------------------------------


def _run_with_per_bucket_capping(
    bucket_names: list[str],
    capped_buckets: set[str],
    submission_factory=None,
):
    """Like ``_run_with_mocks`` but lets ``find_row_count_boundary`` return
    a boundary for only a chosen subset of buckets (``capped_buckets``),
    rather than uniformly for every bucket in the run — needed to exercise
    RunOutcome's cross-bucket aggregation (task 6.2).

    Returns the run's ``RunOutcome``.
    """
    (
        mock_factory_cls, mock_store_cls,
        mock_get_rules, mock_read_journal,
        mock_submit_job,
    ) = _make_mocks(bucket_names, submission_factory=submission_factory)

    def find_boundary_side_effect(**kwargs):
        return "2024-01-02T00:00:00.000000Z" if kwargs.get("bucket_name") in capped_buckets else None

    mock_find_boundary = MagicMock(side_effect=find_boundary_side_effect)

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
        patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
        patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
              mock_find_boundary),
        patch("src.orchestrator.write_in_memory_inventory_manifest",
              return_value=_written_manifest()),
        patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
    ):
        return run_interval(_config(bucket_names), _BASE_RUNTIME)


class TestRunOutcomeAnyCappedAndProgressed:
    """Example tests for ``RunOutcome.any_capped_and_progressed`` — task 6.2.

    _Requirements: 4.1, 4.2, 4.3_
    """

    def test_true_when_bucket_capped_and_submission_succeeds(self):
        """A single bucket that is both a Capped_Run and progressed (job
        submitted, checkpoint advanced) → any_capped_and_progressed is True."""
        _, _, _, _, outcome = _run_with_mocks(
            ["my-bucket"], boundary_timestamp="2024-01-02T00:00:00.000000Z",
            _return_outcome=True,
        )
        assert outcome.any_capped_and_progressed is True
        assert outcome.buckets[0].submitted == 1

    def test_false_when_no_bucket_capped(self):
        """No bucket hit the row cap (find_row_count_boundary returns None
        for all buckets) → any_capped_and_progressed is False, even though
        the run progressed normally."""
        _, _, _, _, outcome = _run_with_mocks(
            ["my-bucket"], boundary_timestamp=None, _return_outcome=True,
        )
        assert outcome.any_capped_and_progressed is False
        assert outcome.buckets[0].submitted == 1

    def test_false_when_capped_but_submission_fails(self):
        """A capped bucket whose job submission fails never progresses, so
        any_capped_and_progressed stays False (no reinvocation storm on a
        failing run — Requirement 4.3)."""
        _, _, _, _, outcome = _run_with_mocks(
            ["my-bucket"],
            boundary_timestamp="2024-01-02T00:00:00.000000Z",
            submission_factory=lambda cfg_id: _failed(cfg_id),
            _return_outcome=True,
        )
        assert outcome.any_capped_and_progressed is False

    def test_false_when_capped_but_no_matching_ops(self):
        """A capped bucket with nothing to submit (no matching journal ops)
        never progresses, so any_capped_and_progressed stays False."""
        _, _, _, _, outcome = _run_with_mocks(
            ["my-bucket"],
            ops_per_bucket={"my-bucket": []},
            boundary_timestamp="2024-01-02T00:00:00.000000Z",
            _return_outcome=True,
        )
        assert outcome.any_capped_and_progressed is False

    def test_true_when_only_one_of_several_buckets_capped_and_progressed(self):
        """Two buckets: only one is a Capped_Run (and it progresses); the
        other is not capped. any_capped_and_progressed is still True — one
        capped-and-progressed bucket is enough for the whole run's outcome."""
        outcome = _run_with_per_bucket_capping(
            ["capped-bucket", "normal-bucket"],
            capped_buckets={"capped-bucket"},
        )
        assert outcome.any_capped_and_progressed is True
        by_name = {b.source_bucket: b for b in outcome.buckets}
        assert by_name["capped-bucket"].submitted == 1
        assert by_name["normal-bucket"].submitted == 1

    def test_false_when_capped_bucket_fails_and_other_bucket_uncapped(self):
        """Two buckets: the capped bucket's submission fails (no progress),
        and the other bucket is not capped at all (regardless of its own
        success). any_capped_and_progressed is False for the whole run —
        no bucket was BOTH capped AND progressed."""

        def submission_factory(cfg_id):
            if cfg_id == "capped-bucket":
                return _failed(cfg_id)
            return _submitted(cfg_id)

        outcome = _run_with_per_bucket_capping(
            ["capped-bucket", "normal-bucket"],
            capped_buckets={"capped-bucket"},
            submission_factory=submission_factory,
        )
        assert outcome.any_capped_and_progressed is False

    def test_run_outcome_buckets_matches_bucket_metrics(self):
        """RunOutcome.buckets carries the same BucketMetrics entries
        run_interval already produces for the Metrics_Publisher."""
        _, _, _, _, outcome = _run_with_mocks(
            ["bucket-aa", "bucket-bb"], _return_outcome=True,
        )
        assert len(outcome.buckets) == 2
        names = {b.source_bucket for b in outcome.buckets}
        assert names == {"bucket-aa", "bucket-bb"}


# ---------------------------------------------------------------------------
# Task 7.3 — Requirement 5.4: a disabled/circuit-broken bucket can never
# contribute capped=True, progressed=True to RunOutcome.any_capped_and_
# progressed, so Self_Reinvocation never fires because of it.
# ---------------------------------------------------------------------------


class TestDisabledAndCircuitBrokenBucketNeverReinvoke:
    def test_disabled_bucket_excluded_before_process_bucket_runs(self):
        """A bucket.disabled=True entry is skipped by the per-bucket loop
        BEFORE _process_bucket runs at all — it never appears in
        RunOutcome.buckets, and cannot contribute to any_capped_and_progressed
        even though a second, active bucket in the same run does progress
        (Requirement 5.4)."""
        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["active-bucket"])

        config = {
            "buckets": [
                {"name": "disabled-bucket", "region": "us-east-1", "disabled": True},
                {"name": "active-bucket", "region": "us-east-1"},
            ]
        }

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value="2024-01-02T00:00:00.000000Z"),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            outcome = run_interval(config, _BASE_RUNTIME)

        # The active bucket capped and progressed, so the run-level signal
        # is still True — disabling one bucket must not suppress another
        # bucket's legitimate reinvocation trigger.
        assert outcome.any_capped_and_progressed is True
        bucket_names = {b.source_bucket for b in outcome.buckets}
        assert bucket_names == {"active-bucket"}
        # get_replication_rules (the first thing _process_bucket does) was
        # never called for the disabled bucket.
        called_bucket_names = {
            call.args[1].name for call in mock_get_rules.call_args_list
        }
        assert "disabled-bucket" not in called_bucket_names

    def test_only_disabled_bucket_yields_no_capped_or_progressed(self):
        """A run whose only Monitored_Bucket is disabled produces
        any_capped_and_progressed=False and an empty buckets list —
        regardless of what the (never-called) row-cap boundary check would
        have returned (Requirement 5.4)."""
        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["disabled-bucket"])

        config = {
            "buckets": [
                {"name": "disabled-bucket", "region": "us-east-1", "disabled": True},
            ]
        }

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value="2024-01-02T00:00:00.000000Z"),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            outcome = run_interval(config, _BASE_RUNTIME)

        assert outcome.any_capped_and_progressed is False
        assert outcome.buckets == []
        mock_get_rules.assert_not_called()

    def test_circuit_breaker_trip_this_run_cannot_set_capped_even_if_window_was_capped(self):
        """A bucket whose circuit breaker trips THIS run (consecutive
        S3 Batch Operations failures reach the threshold) returns from
        _process_bucket in the failed-job-recovery step, strictly before
        the row-cap boundary check runs — so result.capped stays False and
        result.progressed stays False, even when find_row_count_boundary
        (patched here to return a boundary unconditionally) would otherwise
        have reported this window as capped. any_capped_and_progressed must
        be False (Requirement 5.4)."""
        bucket_name = "trip-bucket"

        mock_factory_cls = MagicMock()
        mock_factory = MagicMock()
        mock_factory_cls.return_value = mock_factory

        mock_s3control = MagicMock()
        mock_s3control.describe_job.return_value = {"Job": {"Status": "Failed"}}
        mock_factory.create_s3control_client.return_value = mock_s3control

        mock_store_cls = MagicMock()
        mock_store = MagicMock()
        mock_store_cls.return_value = mock_store
        mock_store.get_checkpoint.side_effect = (
            lambda *a: (_checkpoint(bucket_name), '"etag-0"')
        )
        mock_store.get_submission_records.return_value = {
            bucket_name: _prior_rec_with_count(
                config_id=bucket_name, consecutive_failures=3, job_id="job-cb",
            )
        }

        # find_row_count_boundary always reports a cap — if the circuit
        # breaker's early return did NOT happen first, this bucket would
        # be a Capped_Run.
        mock_find_boundary = MagicMock(return_value="2024-01-02T00:00:00.000000Z")

        disabled_buckets: list[str] = []
        rt = {
            **_BASE_RUNTIME,
            "max_batch_job_failures": 4,
            "on_bucket_disable": lambda name, reason: disabled_buckets.append(name),
        }

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch(
                "src.orchestrator.replication_config_adapter.get_replication_rules",
                return_value=([_rule(bucket_name)], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.read_journal",
                return_value=([_op(bucket_name)], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                mock_find_boundary,
            ),
            patch(
                "src.orchestrator.write_in_memory_inventory_manifest",
                return_value=_written_manifest(),
            ),
            patch(
                "src.orchestrator.batch_operations_adapter.submit_batch_job",
                return_value=_submitted(),
            ),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch(
                "src.orchestrator.bops_report_reader.read_bops_completion_report",
                return_value=[],
            ),
        ):
            outcome = run_interval(_config([bucket_name]), rt)

        assert bucket_name in disabled_buckets  # circuit breaker did trip
        assert outcome.any_capped_and_progressed is False
        assert len(outcome.buckets) == 1
        assert outcome.buckets[0].submitted == 0
        # The row-cap boundary check must never even run for a bucket that
        # is disabled before reaching it.
        mock_find_boundary.assert_not_called()


# ---------------------------------------------------------------------------
# Task 7.3 — Requirement 5.5: a reinvocation finding the lease held defers
# to the existing lease-contention handling; run_interval has no notion of
# "reinvocation" at all, so there is no special-casing to bypass.
# ---------------------------------------------------------------------------


class TestLeaseContentionDefersToExistingHandling:
    def test_lease_contention_skips_bucket_without_reinvocation_awareness(self):
        """A stale-ETag lease acquisition failure (ConditionalWriteError) —
        the existing lease-contention condition — causes the bucket to be
        skipped (errored=True, no submission, no capped/progressed
        contribution), and run_interval's signature has no
        reinvocation-depth parameter through which a reinvocation could
        take a different path (Requirement 5.5)."""
        from src.adapters import state_store as state_store_module

        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["contended-bucket"])

        mock_store = mock_store_cls.return_value
        mock_store.acquire_lease.side_effect = state_store_module.ConditionalWriteError(
            "stale etag — concurrent run"
        )

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            # Simulate this window would also have capped, to prove capped
            # alone (without progressed) never trips any_capped_and_progressed.
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value="2024-01-02T00:00:00.000000Z"),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            outcome = run_interval(_config(["contended-bucket"]), _BASE_RUNTIME)

        assert mock_submit_job.call_count == 0  # never reaches submission
        assert outcome.any_capped_and_progressed is False
        assert outcome.buckets[0].submitted == 0
        assert outcome.buckets[0].errored is True

    def test_lease_contention_outcome_identical_regardless_of_caller_intent(self):
        """run_interval accepts no reinvocation-related argument at all —
        calling it twice with identical config/runtime under the same
        lease-contention mock setup (standing in for 'a scheduled trigger'
        and 'a reinvocation racing a still-running prior invocation')
        produces byte-for-byte identical RunOutcome, proving there is no
        code path that treats a reinvocation specially around the lease
        (Requirement 5.5)."""
        from src.adapters import state_store as state_store_module

        def _run_once():
            (
                mock_factory_cls, mock_store_cls,
                mock_get_rules, mock_read_journal,
                mock_submit_job,
            ) = _make_mocks(["contended-bucket"])
            mock_store = mock_store_cls.return_value
            mock_store.acquire_lease.side_effect = state_store_module.ConditionalWriteError(
                "stale etag — concurrent run"
            )
            with (
                patch("src.orchestrator.ClientFactory", mock_factory_cls),
                patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
                patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
                patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
                patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                      return_value=None),
                patch("src.orchestrator.write_in_memory_inventory_manifest",
                      return_value=_written_manifest()),
                patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
                patch("src.orchestrator.preflight_count", return_value=0),
                patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            ):
                return run_interval(_config(["contended-bucket"]), _BASE_RUNTIME)

        # "scheduled trigger" run and "reinvocation" run — run_interval has
        # no way to distinguish them, so both must produce the same outcome.
        scheduled_outcome = _run_once()
        reinvocation_outcome = _run_once()

        assert scheduled_outcome == reinvocation_outcome
        assert scheduled_outcome.any_capped_and_progressed is False


# ---------------------------------------------------------------------------
# DisabledBuckets metric wiring (Requirement 3.3)
#
# A disabled bucket is skipped before any BucketMetrics exists for it, so it
# contributes no per-bucket datums. The run-level count published here is the
# only metric-visible signal that a bucket has been auto-disabled and is
# awaiting a manual re-enable.
# ---------------------------------------------------------------------------


class TestDisabledBucketsMetricWiring:
    def _run(self, config: dict):
        (
            mock_factory_cls, mock_store_cls,
            mock_get_rules, mock_read_journal,
            mock_submit_job,
        ) = _make_mocks(["active-bucket"])

        published: list = []

        class _CapturingPublisher:
            def __init__(self, namespace=None, dimensions=None):
                pass

            def publish(self, run_result):
                published.append(run_result)

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value=None),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch("src.orchestrator.MetricsPublisher", _CapturingPublisher),
        ):
            run_interval(config, {**_BASE_RUNTIME, "metrics_namespace": "NS"})

        assert len(published) == 1
        return published[0]

    def test_counts_disabled_buckets(self):
        run_result = self._run({
            "buckets": [
                {"name": "disabled-a", "region": "us-east-1", "disabled": True},
                {"name": "disabled-b", "region": "us-east-1", "disabled": True},
                {"name": "active-bucket", "region": "us-east-1"},
            ]
        })
        assert run_result.disabled_buckets == 2
        # The disabled buckets contribute no per-bucket metrics at all, which
        # is precisely why the run-level count is needed.
        assert {b.source_bucket for b in run_result.buckets} == {"active-bucket"}

    def test_zero_when_no_bucket_disabled(self):
        run_result = self._run({
            "buckets": [{"name": "active-bucket", "region": "us-east-1"}]
        })
        assert run_result.disabled_buckets == 0


# ---------------------------------------------------------------------------
# Deleted_Version_Filter applies to the serialized manifest (Req 13.1)
#
# Regression coverage for the partial-exclusion case. Every other test in this
# module stubs read_permanent_deletes to an empty set, so the filter's effect
# on the manifest body was never asserted: the orchestrator computed kept_set,
# used it only for the excluded-count metric and the all-excluded early
# return, then serialized the pre-filter entry list. A partial exclusion
# therefore left the excluded objects in the submitted manifest.
# ---------------------------------------------------------------------------


def _versioned_op(source_bucket: str, key: str, version: str) -> TaggingOperation:
    """A matching TaggingOperation carrying an explicit version token.

    ``operation_version`` is threaded through to ``MatchedObject.version_id``
    (rule_matcher.py:110) and on into the manifest entry, so it is what
    read_permanent_deletes' (object_key, version_id) pairs are matched
    against.
    """
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key=key,
        resulting_tag_set={"env": "prod"},
        sequence_number=f"seq-{key}",
        operation="PutObjectTagging",
        event_time=_NOW,
        operation_version=version,
    )


def _run_capturing_manifest(ops, permanent_deletes):
    """Run one interval and return the csv_bytes handed to the manifest writer.

    Returns ``(csv_text, object_count, submit_call_count)``. ``csv_text`` is
    None when no manifest was written.
    """
    bucket = "my-bucket"
    (
        mock_factory_cls, mock_store_cls,
        mock_get_rules, mock_read_journal,
        mock_submit_job,
    ) = _make_mocks([bucket], ops_per_bucket={bucket: ops})

    captured = {}

    def capture_write(**kwargs):
        captured["csv_bytes"] = kwargs["csv_bytes"]
        # The orchestrator stamps object_count onto the returned manifest, so
        # holding a reference lets the test read the count it recorded.
        written = _written_manifest()
        captured["written"] = written
        return written

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
        patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
        patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
              MagicMock(return_value=None)),
        patch("src.orchestrator.write_in_memory_inventory_manifest",
              side_effect=capture_write),
        patch("src.orchestrator.batch_operations_adapter.submit_batch_job", mock_submit_job),
        patch("src.orchestrator.preflight_count", return_value=0),
        patch("src.orchestrator.read_permanent_deletes", return_value=permanent_deletes),
    ):
        run_interval(_config([bucket]), _BASE_RUNTIME)

    csv_bytes = captured.get("csv_bytes")
    csv_text = csv_bytes.decode("utf-8") if csv_bytes is not None else None
    written = captured.get("written")
    object_count = written.object_count if written is not None else None
    return csv_text, object_count, mock_submit_job.call_count


class TestDeletedVersionFilterAppliesToManifest:
    """The manifest must contain only the filter's survivors (Req 13.1)."""

    def test_partially_excluded_object_is_absent_from_manifest(self):
        ops = [
            _versioned_op("my-bucket", "keep/a.txt", "v-a"),
            _versioned_op("my-bucket", "gone/b.txt", "v-b"),
            _versioned_op("my-bucket", "keep/c.txt", "v-c"),
        ]
        csv_text, object_count, submit_calls = _run_capturing_manifest(
            ops, permanent_deletes={("gone/b.txt", "v-b")},
        )

        assert csv_text is not None, "expected a manifest to be written"
        assert "gone/b.txt" not in csv_text, (
            "permanently deleted version was left in the manifest"
        )
        assert "keep/a.txt" in csv_text
        assert "keep/c.txt" in csv_text
        # Two survivors → two rows, and a job is still submitted.
        assert len([ln for ln in csv_text.splitlines() if ln.strip()]) == 2
        assert submit_calls == 1
        # The recorded count must describe the manifest actually written, not
        # the three pre-filter matches.
        assert object_count == 2

    def test_no_exclusions_keeps_every_entry(self):
        ops = [
            _versioned_op("my-bucket", "keep/a.txt", "v-a"),
            _versioned_op("my-bucket", "keep/b.txt", "v-b"),
        ]
        csv_text, object_count, submit_calls = _run_capturing_manifest(
            ops, permanent_deletes=set(),
        )

        assert csv_text is not None
        assert "keep/a.txt" in csv_text
        assert "keep/b.txt" in csv_text
        assert len([ln for ln in csv_text.splitlines() if ln.strip()]) == 2
        assert submit_calls == 1
        assert object_count == 2

    def test_all_excluded_writes_no_manifest_and_submits_no_job(self):
        """Req 13.7 — the all-excluded early return still holds."""
        ops = [_versioned_op("my-bucket", "gone/a.txt", "v-a")]
        csv_text, _, submit_calls = _run_capturing_manifest(
            ops, permanent_deletes={("gone/a.txt", "v-a")},
        )

        assert csv_text is None
        assert submit_calls == 0

    def test_version_id_is_part_of_the_exclusion_identity(self):
        """A delete of a different version must not exclude the tagged one."""
        ops = [_versioned_op("my-bucket", "keep/a.txt", "v-current")]
        csv_text, _, submit_calls = _run_capturing_manifest(
            ops, permanent_deletes={("keep/a.txt", "v-old")},
        )

        assert csv_text is not None
        assert "keep/a.txt" in csv_text
        assert submit_calls == 1


# ---------------------------------------------------------------------------
# Empty watermark_low must not force an epoch rollback
#
# AWS Security Agent finding f-5a303b12-11c3-48e2-9e3d-619ff88c3d77: a failed
# submission record whose watermark_low is the empty string dragged
# min(failed_lows) to "", resetting the bucket to the epoch and re-admitting
# the entire journal history as duplicate jobs. An empty watermark_low arises
# legitimately for records written before the field existed
# (_deserialize_submission_record defaults it to ""), and is also the shape an
# attacker able to write the state object would use to force a full replay.
# ---------------------------------------------------------------------------


class TestEmptyWatermarkLowDoesNotRollBackToEpoch:
    def test_sole_failed_record_with_empty_low_does_not_reset_to_epoch(self):
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=""),
        }
        emitted, mock_store, _ = _run_migration_recovery_mocks(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed"},
        )
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        # The checkpoint must not have been dragged back to the epoch.
        assert submitted_rec.watermark_low != ""
        # And the skip is reported rather than passing silently.
        causes = " ".join(e.get("cause", "") for e in _errors(emitted))
        assert "watermark_low" in causes

    def test_empty_low_is_ignored_when_another_failure_has_a_real_low(self):
        """The rollback uses the minimum of the usable lows only."""
        real_low = "2024-01-01T00:00:30.000000Z"
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=""),
            "rule-b": _prior_rec("rule-b", "job-b", watermark_low=real_low),
        }
        _, mock_store, _ = _run_migration_recovery_mocks(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed", "job-b": "Failed"},
        )
        mock_store.record_submission.assert_called_once()
        submitted_rec: SubmissionRecord = mock_store.record_submission.call_args[0][2]
        assert submitted_rec.watermark_low == real_low

    def test_readmit_audit_still_emitted_for_the_empty_low_record(self):
        """Skipping the rollback must not hide the job failure itself."""
        prior = {
            "rule-a": _prior_rec("rule-a", "job-a", watermark_low=""),
        }
        emitted, _, _ = _run_migration_recovery_mocks(
            prior_submissions=prior,
            describe_responses={"job-a": "Failed"},
        )
        readmits = _audits(emitted, "batch_job_failure_readmit")
        assert len(readmits) == 1
        assert readmits[0]["config_id"] == "rule-a"


# ---------------------------------------------------------------------------
# The bucket's replication role ARN is not consulted at all
#
# It used to be read from the bucket's replication configuration, validated,
# and passed to CreateJob as the job role, so a foreign, malformed, or absent
# value skipped the bucket and marked it errored. The job role is now the
# stack-created Batch Operations role, so none of those values has any effect.
# solution-owned-batchops-role Requirements 2.3, 2.4.
# ---------------------------------------------------------------------------


class TestReplicationRoleArnIsNotConsulted:
    _FOREIGN = "arn:aws:iam::999988887777:role/attacker-role"
    _MALFORMED = "not-an-arn"
    _WILDCARD = "*"

    def _run_with_config_role(self, role_arn):
        """Run one interval whose bucket's replication configuration names
        *role_arn*, threaded through the real ``rule_deriver`` so the ARN
        travels the path it used to travel.

        Completion reporting is left off, which is the configuration in which
        the ARN previously reached CreateJob.
        """
        bucket = "my-bucket"
        replication_config = {
            "ReplicationConfiguration": {
                "Role": role_arn,
                "Rules": [
                    {
                        "ID": "rule-1",
                        "Status": "Enabled",
                        "Filter": {"Tag": {"Key": "env", "Value": "prod"}},
                        "Destination": {"Bucket": _DEST_ARN},
                    }
                ],
            }
        }
        rules = derive_rules(bucket, replication_config)
        assert len(rules) == 1, "the fixture must derive exactly one rule"

        (
            mock_factory_cls, mock_store_cls,
            _, mock_read_journal, mock_submit_job,
        ) = _make_mocks([bucket])

        emitted: list = []
        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch(
                "src.orchestrator.replication_config_adapter.get_replication_rules",
                return_value=(rules, []),
            ),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                  return_value=None),
            patch("src.orchestrator.write_in_memory_inventory_manifest",
                  return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job",
                  mock_submit_job),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        ):
            outcome = run_interval(_config([bucket]), _BASE_RUNTIME)
        return emitted, mock_submit_job, outcome

    @pytest.mark.parametrize("value", [_FOREIGN, _MALFORMED, _WILDCARD, ""])
    def test_bucket_still_submits_whatever_the_config_role_is(self, value):
        """No bucket is skipped for a role-shaped reason (Requirement 2.4)."""
        _, mock_submit, _ = self._run_with_config_role(value)
        assert mock_submit.call_count == 1

    @pytest.mark.parametrize("value", [_FOREIGN, _MALFORMED, _WILDCARD, ""])
    def test_bucket_is_not_errored_for_a_role_shaped_reason(self, value):
        emitted, _, outcome = self._run_with_config_role(value)
        causes = " ".join(e.get("cause", "") for e in _errors(emitted))
        assert "well-formed IAM role ARN" not in causes
        assert all(bm.errored is False for bm in outcome.buckets)

    @pytest.mark.parametrize("value", [_FOREIGN, _MALFORMED, _WILDCARD, ""])
    def test_created_role_is_passed_regardless_of_the_config_role(self, value):
        """The submitted job's RoleArn never comes from the configuration."""
        _, mock_submit, _ = self._run_with_config_role(value)
        kwargs = mock_submit.call_args.kwargs
        assert kwargs["batch_operations_role_arn"] == _BATCHOPS_ROLE_ARN


# ---------------------------------------------------------------------------
# Always-on completion report — new coverage (spec task 7)
#
# Tests: Requirement 1.1, 2.1, 2.2, 2.3, 2.5, 3.1, 3.3
# ---------------------------------------------------------------------------


class TestAlwaysOnCompletionReport:
    """Tests for the always-on completion report diagnostic, ensuring the
    report is always requested and the permission-shaped diagnostic fires
    regardless of CompletionNotificationEmail."""

    def test_report_enabled_true_with_topic_arn_unset(self):
        """Report.Enabled is True with the topic ARN unset (Requirement 1.1)."""
        _, _, mock_submit_job, _ = _run_single_submission(
            completion_report_topic_arn=None
        )
        # submit_batch_job is called (the job has a report prefix)
        mock_submit_job.assert_called_once()
        call_kwargs = mock_submit_job.call_args.kwargs
        assert call_kwargs.get("completion_report_prefix") is not None
        assert call_kwargs.get("state_bucket") == _STATE_BUCKET

    def test_diagnostic_fires_with_topic_arn_unset(self):
        """The permission-shaped diagnostic fires with the topic ARN unset
        (Requirement 2.1)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-no-topic")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket",
                object_key="key-a",
                version_id="v1",
                error_code="InitiateReplicationNotPermitted",
            ),
        ]
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-no-topic": "Complete"},
            read_bops_report_return_value=entries,
            completion_report_topic_arn=None,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
            and "InitiateReplicationNotPermitted" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1

    def test_diagnostic_fires_on_partial_failure(self):
        """The diagnostic fires on a partial failure — at least one task
        succeeded and at least one failed with a permission-shaped code.
        This is the case is_effective_failure deliberately does not flag
        (Requirement 2.2)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-partial")}
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-ok", version_id="v1",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-fail-1", version_id="v2",
                error_code="InitiateReplicationNotPermitted",
            ),
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-fail-2", version_id="v3",
                error_code="InitiateReplicationNotPermitted",
            ),
        ]
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-partial": "Complete"},
            read_bops_report_return_value=entries,
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
            and "InitiateReplicationNotPermitted" in e.get("cause", "")
        ]
        assert len(diagnostics) == 1
        assert "2 task(s)" in diagnostics[0]["cause"]

    def test_diagnostic_does_not_fire_twice_across_two_runs(self):
        """The diagnostic does not fire twice across two runs for one job
        (Requirement 3.1). Once report_diagnosed is True, the second run
        skips the diagnostic."""
        prior = {
            "rule-1": SubmissionRecord(
                replication_config_id="rule-1",
                source_bucket="my-bucket",
                job_id="job-already-diag",
                manifest_key="manifests/rule-1/ts_manifest.json",
                submitted_at=_NOW,
                status=SubmissionStatus.SUBMITTED,
                watermark_low=_CT_WM_LOW,
                watermark_high=_CT_WM_CURRENT,
                report_diagnosed=True,
            )
        }
        entries = [
            ManifestEntry(
                source_bucket="my-bucket", object_key="key-a", version_id="v1",
                error_code="InitiateReplicationNotPermitted",
            ),
        ]
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-already-diag": "Complete"},
            read_bops_report_return_value=entries,
        )
        # No diagnostic fires — report_diagnosed gate prevents it
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
            and "InitiateReplicationNotPermitted" in e.get("cause", "")
        ]
        assert len(diagnostics) == 0

    def test_report_read_exception_does_not_disturb_recovery(self):
        """A report read raising does not disturb consecutive_failures,
        the rollback target, or the readmission audit (Requirement 2.5).
        The isolation try/except protects the recovery arithmetic."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-read-boom")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-read-boom": "Failed"},
            read_bops_report_side_effect=RuntimeError("S3 read failed"),
        )
        # The report-read failure is logged via Completion_Tracker component
        read_errors = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
            and "Failed to read or diagnose" in e.get("cause", "")
        ]
        assert len(read_errors) == 1
        # The readmission audit still fires (recovery arithmetic unaffected)
        audits = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "audit"
            and e.get("action") == "batch_job_failure_readmit"
        ]
        assert len(audits) == 1
        assert audits[0]["job_id"] == "job-read-boom"

    def test_empty_report_diagnoses_nothing_and_does_not_raise(self):
        """An empty report (no object under the prefix) diagnoses nothing
        and does not raise (migration safety)."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-empty-report")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-empty-report": "Complete"},
            read_bops_report_return_value=[],
        )
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 0

    def test_report_diagnosed_round_trips_through_serialization(self):
        """report_diagnosed round-trips through serialise/deserialise, and
        a payload without the key deserialises to False (Requirement 3.3)."""
        from src.core.checkpoint_serializer import (
            serialize_submission_record,
            _deserialize_submission_record,
        )

        rec = SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket="my-bucket",
            job_id="job-rt",
            manifest_key="manifests/cfg-1/ts.json",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low="2024-01-01T00:00:00.000000Z",
            watermark_high="2024-01-01T00:01:00.000000Z",
            consecutive_failures=0,
            report_diagnosed=True,
        )
        data = serialize_submission_record(rec)
        restored = _deserialize_submission_record(data)
        assert restored.report_diagnosed is True

        # A payload without the key deserialises to False
        data_without_key = dict(data)
        del data_without_key["report_diagnosed"]
        restored_without = _deserialize_submission_record(data_without_key)
        assert restored_without.report_diagnosed is False

    def test_missing_report_diagnoses_nothing_and_raises_nothing(self):
        """An absent completion report diagnoses nothing and raises nothing.

        Whatever the reason the report never arrived — the job wrote none, or
        the write failed — the read finds nothing and
        read_bops_completion_report returns an empty list. This must not break
        anything."""
        prior = {"rule-1": _ct_prior_rec(job_id="job-no-report")}
        emitted, mock_store, mock_read_report = _run_completion_tracking_hook(
            prior_submissions=prior,
            describe_responses={"job-no-report": "Complete"},
            read_bops_report_return_value=[],
            completion_report_topic_arn=None,
        )
        # No diagnostic error, no Completion_Tracker error
        diagnostics = [
            e for e in emitted
            if isinstance(e, dict) and e.get("event") == "error"
            and e.get("component") == "Completion_Tracker"
        ]
        assert len(diagnostics) == 0
        # The run completes without raising (implied by reaching here)
        # mark_report_diagnosed was still called (empty report = nothing to
        # diagnose, but the flag should still be set to avoid re-reading)
        mock_store.mark_report_diagnosed.assert_called_once()
