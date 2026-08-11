"""Tests for lease release on every post-acquire exit path (Task 15).

Validates Requirements 5.1, 5.2, 5.3, 5.5 from the orchestrator-decomposition spec.

Each test exercises one post-acquire early-return path and asserts:
  - release_lease was called (lease cleared)
  - submitted_refs=None (watermark not advanced)

One test covers release itself failing: the failure is logged and the original
outcome (errored/result) is preserved.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.adapters.batch_operations_adapter import SubmissionResult
from src.adapters.inventory_manifest_writer import (
    InventoryManifestWriteError,
    WrittenManifest,
)
from src.core.models import (
    CheckpointState,
    DestinationRef,
    DerivedReplicationRule,
    FailureClass,
    S3Location,
    SubmissionStatus,
    TaggingOperation,
)
from src.orchestrator import run_interval
from tests.support import mock_state_store

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)
_DEST_ARN = "arn:aws:s3:::dest-bucket"
_ACCOUNT_ID = "123456789012"
_STATE_BUCKET = "scratch-state-bucket"

_BASE_RUNTIME = {
    "state_bucket": _STATE_BUCKET,
    "athena_workgroup": "primary",
    "athena_output_location": f"s3://{_STATE_BUCKET}/athena/",
    "account_id": _ACCOUNT_ID,
    "region": "us-east-1",
}


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


def _permanent_client_failure(config_id: str = "rule-1") -> SubmissionResult:
    return SubmissionResult(
        status=SubmissionStatus.CREATE_FAILED,
        config_id=config_id,
        object_count=1,
        error_reason="ParamValidationError: Invalid param",
        failure_class=FailureClass.PERMANENT_CLIENT,
    )


def _make_base_mocks(bucket_name: str = "my-bucket"):
    """Create the standard set of mocks for a single-bucket run."""
    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_store_cls = MagicMock()
    mock_store = mock_state_store()
    mock_store_cls.return_value = mock_store

    def get_checkpoint_side_effect(s3_client, state_bucket, source_bucket):
        return (_checkpoint(source_bucket), '"etag-0"')

    mock_store.get_checkpoint.side_effect = get_checkpoint_side_effect
    mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    # increment_submission_failure_streak returns (new_value, new_etag)
    mock_store.increment_submission_failure_streak.return_value = (99, '"etag-streak"')

    mock_get_rules = MagicMock(
        return_value=([_rule(bucket_name)], [])
    )

    mock_read_journal = MagicMock(
        return_value=([_op(bucket_name)], [])
    )

    return mock_factory_cls, mock_store_cls, mock_get_rules, mock_read_journal, mock_store


def _run_lease_test(
    bucket_name: str = "my-bucket",
    preflight_side_effect=None,
    manifest_write_side_effect=None,
    read_permanent_deletes_return=None,
    submit_side_effect=None,
    runtime_overrides: dict | None = None,
):
    """Run a single-bucket interval with configurable failure injection.

    Returns the mock_store so callers can assert on release_lease.
    """
    (
        mock_factory_cls, mock_store_cls, mock_get_rules,
        mock_read_journal, mock_store,
    ) = _make_base_mocks(bucket_name)

    preflight_mock = MagicMock(return_value=0)
    if preflight_side_effect is not None:
        preflight_mock.side_effect = preflight_side_effect

    manifest_write_mock = MagicMock(return_value=_written_manifest())
    if manifest_write_side_effect is not None:
        manifest_write_mock.side_effect = manifest_write_side_effect

    perm_deletes_return = read_permanent_deletes_return if read_permanent_deletes_return is not None else set()

    submit_mock = MagicMock(return_value=_submitted())
    if submit_side_effect is not None:
        submit_mock.side_effect = submit_side_effect

    rt = {**_BASE_RUNTIME, **(runtime_overrides or {})}

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
        patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
        patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary", return_value=None),
        patch("src.orchestrator.write_in_memory_inventory_manifest", manifest_write_mock),
        patch("src.orchestrator.batch_operations_adapter.submit_batch_job", submit_mock),
        patch("src.orchestrator.preflight_count", preflight_mock),
        patch("src.orchestrator.read_permanent_deletes", return_value=perm_deletes_return),
    ):
        outcome = run_interval(_config([bucket_name]), rt)

    return mock_store, outcome


# ---------------------------------------------------------------------------
# Test: Preflight failure -> lease released, watermark unchanged
# Validates: Requirements 5.1, 5.2
# ---------------------------------------------------------------------------

class TestLeaseReleaseOnPreflightFailure:
    def test_lease_released_on_preflight_failure(self):
        """When preflight_count raises, the lease is released with submitted_refs=None."""
        mock_store, _ = _run_lease_test(
            preflight_side_effect=RuntimeError("Athena timeout"),
        )
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None


# ---------------------------------------------------------------------------
# Test: No matches (gen.finalize has_matches=False) -> lease released
# Validates: Requirements 5.1, 5.2
# ---------------------------------------------------------------------------

class TestLeaseReleaseOnNoMatches:
    def test_lease_released_when_no_matches(self):
        """When no operations match any rule, the lease is released with submitted_refs=None.

        We inject ops that pass dedup but fail to match any rule by providing
        a rule that won't match the operation's tags. Instead, we use the
        simpler approach: provide ops with tags that DO match, but make the
        manifest generator finalize with has_matches=False by having
        read_permanent_deletes exclude everything.

        Actually the cleaner path: supply ops whose resulting_tag_set does NOT
        match the rule's tag_filter, so gen.finalize reports no matches.
        """
        # The simplest way to get no matches: supply ops that do match the rule,
        # so the lease IS acquired (candidate_hwm is not None), then make the
        # gen.finalize return has_matches=False. We can achieve this by
        # patching the ManifestGenerator... but that's complex. Instead let's use
        # a simpler path: supply an op with tags that DO match the filter so
        # candidate_hwm is computed and the lease is acquired, but then have
        # preflight + finalize show no matches.
        #
        # Actually, looking at the code flow more carefully:
        # 1. _select_eligible returns candidate_hwm based on deduped ops
        # 2. Lease is acquired since candidate_hwm is not None
        # 3. The "for op in deduped_ops" loop matches and accumulates
        # 4. gen.finalize(bucket_name) checks has_matches
        #
        # To get has_matches=False, we need ops that DON'T match rules.
        # But then _select_eligible would still produce candidate_hwm (it's
        # based on deduped ops, not matched ones).
        #
        # Let's use a different approach: provide a rule whose tag_filter
        # won't match the op's resulting_tag_set. The op will still be
        # deduplicated and candidate_hwm will still be set, but matching
        # will produce zero matches.
        (
            mock_factory_cls, mock_store_cls, mock_get_rules,
            mock_read_journal, mock_store,
        ) = _make_base_mocks("my-bucket")

        # Rule with a tag filter that won't match the operation's tags
        non_matching_rule = DerivedReplicationRule(
            source_bucket="my-bucket",
            replication_config_id="rule-1",
            rule_id="rule-1",
            tag_filter={"env": "staging"},  # won't match {"env": "prod"}
            destination=DestinationRef(bucket_arn=_DEST_ARN),
        )
        mock_get_rules.return_value = ([non_matching_rule], [])

        rt = _BASE_RUNTIME

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary", return_value=None),
            patch("src.orchestrator.write_in_memory_inventory_manifest", return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", return_value=_submitted()),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        ):
            run_interval(_config(["my-bucket"]), rt)

        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None


# ---------------------------------------------------------------------------
# Test: All candidates excluded by filter_deleted_versions -> lease released
# Validates: Requirements 5.1, 5.2
# ---------------------------------------------------------------------------

class TestLeaseReleaseOnAllCandidatesExcluded:
    def test_lease_released_when_all_candidates_excluded(self):
        """When filter_deleted_versions excludes all entries, lease released with submitted_refs=None."""
        # filter_deleted_versions checks (object_key, version_id) tuples.
        # Our test op has object_key="path/obj.txt" and version_id=None.
        perm_deleted = {("path/obj.txt", None)}

        mock_store, _ = _run_lease_test(
            read_permanent_deletes_return=perm_deleted,
        )
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None


# ---------------------------------------------------------------------------
# Test: Manifest write failure (InventoryManifestWriteError) -> lease released
# Validates: Requirements 5.1, 5.2
# ---------------------------------------------------------------------------

class TestLeaseReleaseOnManifestWriteFailure:
    def test_lease_released_on_manifest_write_error(self):
        """When write_in_memory_inventory_manifest raises InventoryManifestWriteError,
        lease released with submitted_refs=None."""
        mock_store, _ = _run_lease_test(
            manifest_write_side_effect=InventoryManifestWriteError("my-bucket", "S3 put failed"),
        )
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None


# ---------------------------------------------------------------------------
# Test: Submission-streak disable (bucket disabled) -> lease released
# Validates: Requirements 5.1, 5.2
# ---------------------------------------------------------------------------

class TestLeaseReleaseOnSubmissionStreakDisable:
    def test_lease_released_when_bucket_disabled_by_streak(self):
        """When the submission-failure streak threshold disables a bucket,
        the lease is released with submitted_refs=None."""
        disabled_buckets = []

        mock_store, _ = _run_lease_test(
            submit_side_effect=lambda **kwargs: _permanent_client_failure(),
            runtime_overrides={
                "max_batch_job_failures": 1,  # threshold=1 so first failure disables
                "on_bucket_disabled": lambda name, reason: disabled_buckets.append(name),
            },
        )
        # Bucket was indeed disabled
        assert "my-bucket" in disabled_buckets
        # Lease was still released
        assert mock_store.release_lease.call_count == 1
        kwargs = mock_store.release_lease.call_args.kwargs
        assert kwargs["submitted_refs"] is None


# ---------------------------------------------------------------------------
# Test: Release itself failing -> logged, original outcome preserved
# Validates: Requirement 5.3
# ---------------------------------------------------------------------------

class TestLeaseReleaseFailureHandled:
    def test_release_failure_logged_and_original_outcome_preserved(self, caplog):
        """When release_lease raises, the failure is logged and the original
        outcome is preserved (not masked by the release exception).

        Requirement 5.3: release failure SHALL be logged and SHALL NOT mask
        the original error that caused the early return.
        """
        (
            mock_factory_cls, mock_store_cls, mock_get_rules,
            mock_read_journal, mock_store,
        ) = _make_base_mocks("my-bucket")

        # Make release_lease raise
        mock_store.release_lease.side_effect = RuntimeError("DynamoDB timeout on release")

        # Trigger via preflight failure so the path hits the release in __exit__
        preflight_mock = MagicMock(side_effect=RuntimeError("Athena broke"))

        rt = _BASE_RUNTIME

        with caplog.at_level(logging.ERROR):
            with (
                patch("src.orchestrator.ClientFactory", mock_factory_cls),
                patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
                patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
                patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
                patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary", return_value=None),
                patch("src.orchestrator.write_in_memory_inventory_manifest", return_value=_written_manifest()),
                patch("src.orchestrator.batch_operations_adapter.submit_batch_job", return_value=_submitted()),
                patch("src.orchestrator.preflight_count", preflight_mock),
                patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            ):
                outcome = run_interval(_config(["my-bucket"]), rt)

        # Release was attempted (and failed)
        assert mock_store.release_lease.call_count == 1

        # The original outcome is preserved — run_interval returns normally,
        # not raising the release exception. The bucket was not errored by the
        # preflight failure (that path is a skip, not an error).
        assert outcome is not None
        assert len(outcome.buckets) == 1
        # Crucially: the release failure did NOT propagate as an unhandled exception.
        # The run completed and produced a result (Requirement 5.3).

        # Verify the lease release failure was logged via observability.emit
        # The caplog captured the error log containing the release failure message.
        log_messages = [r.message for r in caplog.records]
        assert any("Failed to release lease" in msg for msg in log_messages), (
            f"Expected 'Failed to release lease' in logs, got: {log_messages}"
        )


# ---------------------------------------------------------------------------
# Non-vacuousness verification: when _lease_scope does NOT release,
# these tests should fail. We verify by temporarily making release_lease
# not get called (simulating the absence of the context manager's finally block).
# ---------------------------------------------------------------------------

class TestNonVacuous:
    def test_without_lease_release_preflight_failure_would_not_release(self):
        """Non-vacuousness: if _lease_scope's finally block is skipped (no-op),
        release_lease would NOT be called on the preflight failure path.

        We verify by patching _lease_scope to skip the release in its finally block.
        """
        import contextlib
        from src.orchestrator import _LeaseHolder

        @contextlib.contextmanager
        def noop_lease_scope(writer, ctx, candidate_hwm, lookback):
            """A _lease_scope that acquires but does NOT release."""
            from src.core.models import Lease, LeaseStatus
            import uuid
            lease = Lease(
                lease_id=str(uuid.uuid4()),
                candidate_max_watermark=candidate_hwm,
                acquired_at=datetime.now(tz=timezone.utc),
                status=LeaseStatus.IN_FLIGHT,
            )
            writer.acquire_lease(lease)
            yield _LeaseHolder()
            # Deliberately no release in finally

        (
            mock_factory_cls, mock_store_cls, mock_get_rules,
            mock_read_journal, mock_store,
        ) = _make_base_mocks("my-bucket")

        preflight_mock = MagicMock(side_effect=RuntimeError("Athena timeout"))

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch("src.orchestrator.replication_config_adapter.get_replication_rules", mock_get_rules),
            patch("src.orchestrator.athena_journal_adapter.read_journal", mock_read_journal),
            patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary", return_value=None),
            patch("src.orchestrator.write_in_memory_inventory_manifest", return_value=_written_manifest()),
            patch("src.orchestrator.batch_operations_adapter.submit_batch_job", return_value=_submitted()),
            patch("src.orchestrator.preflight_count", preflight_mock),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch("src.orchestrator._lease_scope", noop_lease_scope),
        ):
            run_interval(_config(["my-bucket"]), _BASE_RUNTIME)

        # With the no-op lease scope, release_lease should NOT be called
        assert mock_store.release_lease.call_count == 0, (
            "Non-vacuousness check: with _lease_scope not releasing, "
            "release_lease should not be called"
        )
