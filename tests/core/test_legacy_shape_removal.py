"""Tests asserting the behaviour when old-format data is encountered after
the legacy compatibility paths have been removed (Requirement 3.8).

Each test documents what the Solution does when handed data in a shape it no
longer actively migrates — the acceptable outcome is that stale keys are
ignored and no unhandled exception is raised.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.core.checkpoint_serializer import deserialize_submission_records
from src.core.manifest_generator import deserialize


# ---------------------------------------------------------------------------
# 1. Singular submission_record key in payload -> ignored (empty dict returned)
# ---------------------------------------------------------------------------


class TestSingularSubmissionRecordIgnored:
    """After removal of the singular ``submission_record`` migration branch,
    a payload carrying only the old singular key (and no ``submission_records``
    dict) is treated as having no submission records."""

    def test_singular_submission_record_key_returns_empty_dict(self):
        payload = {
            "submission_record": {
                "replication_config_id": "cfg-old",
                "source_bucket": "my-bucket",
                "job_id": "job-legacy-001",
                "manifest_key": "manifests/legacy.csv",
                "submitted_at": "2024-01-01T00:00:00+00:00",
                "status": "SUBMITTED",
                "watermark_low": "",
                "watermark_high": "",
            }
        }
        result = deserialize_submission_records(payload)
        assert result == {}

    def test_both_absent_still_returns_empty_dict(self):
        """Neither key present — same outcome as before."""
        result = deserialize_submission_records({})
        assert result == {}

    def test_submission_records_dict_still_works(self):
        """The current format (submission_records dict) still deserializes."""
        payload = {
            "submission_records": {
                "my-bucket": {
                    "replication_config_id": "cfg-1",
                    "source_bucket": "my-bucket",
                    "job_id": "job-001",
                    "manifest_key": "manifests/m.csv",
                    "submitted_at": "2024-06-01T12:00:00+00:00",
                    "status": "SUBMITTED",
                    "watermark_low": "2024-01-01T00:00:00.000000Z",
                    "watermark_high": "2024-06-01T00:00:00.000000Z",
                }
            }
        }
        result = deserialize_submission_records(payload)
        assert len(result) == 1
        assert "my-bucket" in result


# ---------------------------------------------------------------------------
# 2. 2-column CSV -> deserialize raises (round-trip oracle expects 3 columns)
# ---------------------------------------------------------------------------


class TestTwoColumnCsvHandling:
    """After removal of the 2-column detection branch, ``deserialize`` always
    uses the 3-column parser. A 2-column row is parsed by rpartition, which
    splits off the last comma-delimited segment as ``version_id`` and the
    remainder (partitioned on the first comma) as ``bucket,key`` — for a
    genuinely 2-column row this misassigns the key's own text to
    ``version_id`` and leaves ``object_key`` empty. The result is
    structurally wrong but does not raise an unhandled exception.

    This is the load-bearing regression test for Requirement 3.2: the old
    auto-detection branch would have counted the single comma in this row,
    classified it as 2-column, and returned ``object_key="path/to/file.txt"``
    with ``version_id=None`` — the opposite of what is asserted below. If the
    auto-detection branch is restored, this test fails."""

    def test_two_column_row_does_not_raise(self):
        """A 2-column row is parsed without exception, via the 3-column
        parser's rpartition — object_key ends up empty and version_id
        absorbs the key's own text, rather than raising."""
        csv = "my-bucket,path/to/file.txt"
        result = deserialize(csv)
        assert len(result) == 1
        entry = result[0]
        assert entry.source_bucket == "my-bucket"
        assert entry.object_key == ""
        assert entry.version_id == "path/to/file.txt"


# ---------------------------------------------------------------------------
# 3. Per-config_id keyed submission records (no sentinel key) -> bucket skipped
# ---------------------------------------------------------------------------


class TestPerConfigIdKeyingSkipped:
    """After removal of the per-config_id fallback in check_report_handler,
    a submission_records dict that has only legacy config_id keys (no
    bucket_name sentinel key) results in the bucket being skipped — the
    get(bucket_name) lookup returns None.

    This test only validates the deserialize layer, which is key-agnostic
    and was not itself changed by Requirement 3.4 — it does not exercise
    check_report_handler and cannot fail if the removed fallback is
    restored. The load-bearing regression test for 3.4 is
    ``tests/test_lambda_handler.py::TestCheckReportHandler::
    test_legacy_config_id_keyed_record_is_skipped_not_iterated``, which
    calls the handler itself and asserts ``describe_job`` is never called
    for the legacy-keyed record."""

    def test_per_config_id_keyed_records_have_no_bucket_sentinel(self):
        """When submission_records is keyed by config_id rather than
        bucket_name, looking up bucket_name returns None — the bucket is
        skipped in check_report_handler."""
        payload = {
            "submission_records": {
                "cfg-rule-1": {
                    "replication_config_id": "cfg-rule-1",
                    "source_bucket": "my-bucket",
                    "job_id": "job-legacy-002",
                    "manifest_key": "manifests/legacy2.csv",
                    "submitted_at": "2024-03-01T00:00:00+00:00",
                    "status": "SUBMITTED",
                    "watermark_low": "",
                    "watermark_high": "",
                },
                "cfg-rule-2": {
                    "replication_config_id": "cfg-rule-2",
                    "source_bucket": "my-bucket",
                    "job_id": "job-legacy-003",
                    "manifest_key": "manifests/legacy3.csv",
                    "submitted_at": "2024-03-01T00:00:00+00:00",
                    "status": "SUBMITTED",
                    "watermark_low": "",
                    "watermark_high": "",
                },
            }
        }
        records = deserialize_submission_records(payload)
        # Records deserialize fine (the function is key-agnostic)
        assert len(records) == 2
        # But the bucket_name sentinel lookup that check_report_handler uses
        # will not find a match
        bucket_name = "my-bucket"
        assert records.get(bucket_name) is None


# ---------------------------------------------------------------------------
# 4. Pre-migration TrackedObject handling -> no exception
# ---------------------------------------------------------------------------


class TestPreMigrationTrackedObjectHandling:
    """Requirement 3.5's "pre-migration TrackedObject handling" in
    completion_tracker.py turned out, on inspection, to be narration only —
    ``build_completion_report`` and ``should_publish`` never contained a
    conditional branch keyed on ``len(obj.configs)``; both always iterated
    ``obj.configs`` generically. There was no behavioural branch to delete,
    so these are not regression tests for a removed branch — task 7 removed
    the "pre-migration"/"legacy" narration describing that generic loop
    (Requirement 3.6), and these tests are the coverage that the generic
    loop still correctly handles a multi-config object (the shape a
    pre-migration state object would produce) after that narration is gone.
    """

    def test_build_completion_report_multi_config_no_exception(self):
        """build_completion_report handles a TrackedObject with multiple
        configs (the pre-migration shape) without raising."""
        from src.core.completion_tracker import build_completion_report
        from src.core.models import CompletionState, ConfigContext, TrackedObject

        obj = TrackedObject(
            source_bucket="my-bucket",
            object_key="path/to/obj.txt",
            version_id="v1",
            configs={
                "cfg-rule-1": ConfigContext(
                    replication_config_id="cfg-rule-1",
                    job_id="job-1",
                    manifest_generated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
                    bops_confirmed=True,
                ),
                "cfg-rule-2": ConfigContext(
                    replication_config_id="cfg-rule-2",
                    job_id="job-2",
                    manifest_generated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
                    bops_confirmed=True,
                ),
            },
            state=CompletionState.RESOLVED,
            resolved_at=datetime(2024, 6, 2, tzinfo=timezone.utc),
            resolution_method="source_status_header",
            replication_outcome="COMPLETE",
        )
        # Should not raise
        report = build_completion_report("my-bucket", [obj])
        assert report["item_count"] == 1
        assert report["items"][0]["destinations"] == ["cfg-rule-1", "cfg-rule-2"]

    def test_should_publish_multi_config_no_exception(self):
        """should_publish handles a TrackedObject with multiple configs
        (the pre-migration shape) without raising."""
        from src.core.completion_tracker import should_publish
        from src.core.models import (
            CompletionState,
            ConfigContext,
            ScanState,
            TrackedObject,
        )

        obj = TrackedObject(
            source_bucket="my-bucket",
            object_key="path/to/obj.txt",
            version_id=None,
            configs={
                "cfg-rule-1": ConfigContext(
                    replication_config_id="cfg-rule-1",
                    job_id="job-1",
                    manifest_generated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
                    bops_confirmed=True,
                ),
                "cfg-rule-2": ConfigContext(
                    replication_config_id="cfg-rule-2",
                    job_id="job-2",
                    manifest_generated_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
                    bops_confirmed=True,
                ),
            },
            state=CompletionState.RESOLVED,
            resolved_at=datetime(2024, 6, 2, tzinfo=timezone.utc),
            resolution_method="source_status_header",
            replication_outcome="COMPLETE",
        )
        scan_state_by_config = {
            "cfg-rule-1": ScanState(
                last_scan_at=datetime(2024, 6, 2, tzinfo=timezone.utc),
                last_scan_match_count=0,
            ),
            "cfg-rule-2": ScanState(
                last_scan_at=datetime(2024, 6, 2, tzinfo=timezone.utc),
                last_scan_match_count=0,
            ),
        }
        # Should not raise — returns True because all configs are quiescent
        result = should_publish(obj, scan_state_by_config)
        assert result is True
