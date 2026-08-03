"""Tests for src/core/observability.py — tasks 10.2 and 10.3.

Property test:
  - Property 9: Summary counts and log timestamps — the summary entry's counts
    equal the interval's actual values (zero where none), and every emitted
    log entry includes a timestamp.

Unit tests:
  - Submission log carries job id + bucket (Req 11.3).
  - Error log carries component, bucket, and cause (Req 11.4).
  - All log entries include a timestamp (Req 11.5).

Requirements: 11.1, 11.2, 11.3, 11.4, 11.5
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.observability import (
    emit,
    log_audit,
    log_error,
    log_reinvocation_chain_limit_reached,
    log_reinvocation_triggered,
    log_submission,
    log_summary,
    logger as observability_logger,
    redact_object_key,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Property 9: Summary counts and log timestamps (task 10.2)
# Feature: tag-based-s3-replication, Property 9: Summary counts and log timestamps
# ---------------------------------------------------------------------------


class TestProperty9SummaryCountsAndTimestamps:
    @given(
        ops_read=st.integers(min_value=0, max_value=10_000),
        matched_objects=st.integers(min_value=0, max_value=10_000),
        jobs_submitted=st.integers(min_value=0, max_value=100),
        dup_discarded=st.integers(min_value=0, max_value=5_000),
    )
    @settings(max_examples=100)
    def test_summary_counts_match_inputs(
        self,
        ops_read: int,
        matched_objects: int,
        jobs_submitted: int,
        dup_discarded: int,
    ) -> None:
        """Summary entry counts equal the provided interval values.

        # Feature: tag-based-s3-replication, Property 9: Summary counts and log timestamps
        """
        entry = log_summary(
            ops_read=ops_read,
            matched_objects=matched_objects,
            jobs_submitted=jobs_submitted,
            duplicate_records_discarded=dup_discarded,
        )
        assert entry["Tagging_Operations"] == ops_read
        assert entry["Matched_Objects"] == matched_objects
        assert entry["Batch_Replication_Job_submissions"] == jobs_submitted
        assert entry["duplicate_records_discarded"] == dup_discarded

    @given(
        ops_read=st.integers(min_value=0, max_value=1000),
        matched_objects=st.integers(min_value=0, max_value=1000),
        jobs_submitted=st.integers(min_value=0, max_value=100),
        dup_discarded=st.integers(min_value=0, max_value=500),
    )
    @settings(max_examples=100)
    def test_summary_entry_includes_timestamp(
        self,
        ops_read: int,
        matched_objects: int,
        jobs_submitted: int,
        dup_discarded: int,
    ) -> None:
        """Every summary entry includes a timestamp (Req 11.5).

        # Feature: tag-based-s3-replication, Property 9: Summary counts and log timestamps
        """
        entry = log_summary(
            ops_read=ops_read,
            matched_objects=matched_objects,
            jobs_submitted=jobs_submitted,
            duplicate_records_discarded=dup_discarded,
        )
        assert "timestamp" in entry
        assert isinstance(entry["timestamp"], str)
        assert len(entry["timestamp"]) > 0
        # Must be parseable as ISO 8601.
        datetime.fromisoformat(entry["timestamp"])

    @given(
        ops_read=st.just(0),
        matched_objects=st.just(0),
        jobs_submitted=st.just(0),
        dup_discarded=st.just(0),
    )
    @settings(max_examples=10)
    def test_zero_counts_recorded_when_interval_is_empty(
        self,
        ops_read: int,
        matched_objects: int,
        jobs_submitted: int,
        dup_discarded: int,
    ) -> None:
        """Zero values are recorded explicitly, not omitted (Req 11.2).

        # Feature: tag-based-s3-replication, Property 9: Summary counts and log timestamps
        """
        entry = log_summary(
            ops_read=ops_read,
            matched_objects=matched_objects,
            jobs_submitted=jobs_submitted,
            duplicate_records_discarded=dup_discarded,
        )
        assert entry["Tagging_Operations"] == 0
        assert entry["Matched_Objects"] == 0
        assert entry["Batch_Replication_Job_submissions"] == 0

    @given(
        job_id=st.text(min_size=1, max_size=50),
        bucket=st.from_regex(r"^[a-z][a-z0-9\-]{3,20}$", fullmatch=True),
    )
    @settings(max_examples=100)
    def test_submission_entry_includes_timestamp(
        self, job_id: str, bucket: str
    ) -> None:
        """Every submission log entry includes a timestamp (Req 11.5).

        # Feature: tag-based-s3-replication, Property 9: Summary counts and log timestamps
        """
        entry = log_submission(job_id=job_id, source_bucket=bucket)
        assert "timestamp" in entry
        datetime.fromisoformat(entry["timestamp"])

    @given(
        component=st.text(min_size=1, max_size=50),
        bucket=st.from_regex(r"^[a-z][a-z0-9\-]{3,20}$", fullmatch=True),
        cause=st.text(min_size=1, max_size=200),
    )
    @settings(max_examples=100)
    def test_error_entry_includes_timestamp(
        self, component: str, bucket: str, cause: str
    ) -> None:
        """Every error log entry includes a timestamp (Req 11.5).

        # Feature: tag-based-s3-replication, Property 9: Summary counts and log timestamps
        """
        entry = log_error(component=component, bucket=bucket, cause=cause)
        assert "timestamp" in entry
        datetime.fromisoformat(entry["timestamp"])


# ---------------------------------------------------------------------------
# Unit tests: submission and error log content (task 10.3)
# ---------------------------------------------------------------------------


class TestSubmissionLogContent:
    """Submission log carries job id + source bucket (Req 11.3)."""

    def test_submission_log_contains_job_id(self):
        entry = log_submission(job_id="job-abc-123", source_bucket="my-bucket")
        assert entry["job_id"] == "job-abc-123"

    def test_submission_log_contains_source_bucket(self):
        entry = log_submission(job_id="job-abc-123", source_bucket="my-bucket")
        assert entry["source_bucket"] == "my-bucket"

    def test_submission_log_event_type_is_job_submitted(self):
        entry = log_submission(job_id="j", source_bucket="b")
        assert entry["event"] == "job_submitted"

    def test_submission_log_accepts_explicit_timestamp(self):
        entry = log_submission("job-1", "bucket-a", timestamp=_NOW)
        assert entry["timestamp"] == _NOW.isoformat()

    def test_submission_log_is_json_serializable(self):
        entry = log_submission("job-xyz", "bucket-xyz")
        json.dumps(entry)  # must not raise

    def test_summary_event_type_is_interval_summary(self):
        entry = log_summary(0, 0, 0, 0)
        assert entry["event"] == "interval_summary"


class TestErrorLogContent:
    """Error log carries component, bucket, and cause (Req 11.4)."""

    def test_error_log_contains_component(self):
        entry = log_error("Journal_Monitor", "my-bucket", "access denied")
        assert entry["component"] == "Journal_Monitor"

    def test_error_log_contains_bucket(self):
        entry = log_error("Journal_Monitor", "my-bucket", "access denied")
        assert entry["bucket"] == "my-bucket"

    def test_error_log_contains_cause(self):
        entry = log_error("Journal_Monitor", "my-bucket", "access denied")
        assert entry["cause"] == "access denied"

    def test_error_log_event_type_is_error(self):
        entry = log_error("comp", "bucket", "cause")
        assert entry["event"] == "error"

    def test_error_log_accepts_explicit_timestamp(self):
        entry = log_error("C", "B", "cause", timestamp=_NOW)
        assert entry["timestamp"] == _NOW.isoformat()

    def test_error_log_is_json_serializable(self):
        entry = log_error("Rule_Matcher", "bucket-a", "tag set indeterminate")
        json.dumps(entry)  # must not raise

    def test_all_three_required_fields_non_empty(self):
        entry = log_error("Component", "bucket-name", "failure reason")
        assert entry["component"]
        assert entry["bucket"]
        assert entry["cause"]


class TestTimestampHandling:
    """Timestamp included in every log entry (Req 11.5)."""

    def test_summary_timestamp_is_iso_parseable(self):
        entry = log_summary(1, 1, 1, 0)
        datetime.fromisoformat(entry["timestamp"])

    def test_submission_timestamp_is_iso_parseable(self):
        entry = log_submission("j", "b")
        datetime.fromisoformat(entry["timestamp"])

    def test_error_timestamp_is_iso_parseable(self):
        entry = log_error("C", "B", "cause")
        datetime.fromisoformat(entry["timestamp"])

    def test_explicit_utc_timestamp_preserved(self):
        ts = datetime(2024, 3, 15, 9, 30, 0, tzinfo=timezone.utc)
        entry = log_summary(0, 0, 0, 0, timestamp=ts)
        restored = datetime.fromisoformat(entry["timestamp"])
        assert restored == ts

    def test_audit_log_has_timestamp(self):
        entry = log_audit("checkpoint_advanced", "my-bucket")
        assert "timestamp" in entry
        datetime.fromisoformat(entry["timestamp"])


class TestAuditLogContent:
    def test_audit_log_contains_action(self):
        entry = log_audit("lease_acquired", "bucket-a")
        assert entry["action"] == "lease_acquired"

    def test_audit_log_contains_source_bucket(self):
        entry = log_audit("lease_released", "my-bucket")
        assert entry["source_bucket"] == "my-bucket"

    def test_audit_log_event_type_is_audit(self):
        entry = log_audit("any_action", "b")
        assert entry["event"] == "audit"

    def test_audit_log_merges_details(self):
        entry = log_audit("batch_job_created", "b", details={"job_id": "j123"})
        assert entry["job_id"] == "j123"


class TestEmitFunction:
    """emit() routes entries to the Python logging infrastructure."""

    def test_emit_info_for_summary(self, caplog):
        with caplog.at_level(logging.INFO):
            emit(log_summary(0, 0, 0, 0))
        assert any("interval_summary" in r.message for r in caplog.records)

    def test_emit_error_for_error_event(self, caplog):
        with caplog.at_level(logging.ERROR):
            emit(log_error("C", "B", "fail"))
        assert any(r.levelno == logging.ERROR for r in caplog.records)

    def test_emit_produces_json_parseable_line(self, caplog):
        with caplog.at_level(logging.INFO):
            emit(log_submission("j", "b"))
        # The last info record should be JSON-parseable.
        info_records = [r for r in caplog.records if r.levelno == logging.INFO]
        assert info_records, "No INFO records emitted"
        json.loads(info_records[-1].message)


class TestJournalReadCappedVisibility:
    """Requirement 6.1 — `journal_read_capped` (an audit entry) is emitted at
    a level visible in the deployed Lambda's default logging configuration.

    AWS Lambda's Python runtime defaults the root logger to WARNING for
    plain-text log format, so an entry emitted only at INFO on a logger whose
    effective level resolves to the root's WARNING default would be
    silently dropped. This module's logger explicitly sets its own level to
    INFO so `emit()`'s `logger.info(...)` call is visible regardless of the
    root logger's configuration.
    """

    def test_observability_logger_level_is_info_or_more_visible(self):
        assert observability_logger.getEffectiveLevel() <= logging.INFO

    def test_audit_entry_visible_even_when_root_defaults_to_warning(self, caplog):
        # Simulate the Lambda Python runtime's plain-text-format default: the
        # root logger's level is WARNING, higher (less visible) than INFO.
        root_logger = logging.getLogger()
        original_root_level = root_logger.level
        root_logger.setLevel(logging.WARNING)
        try:
            with caplog.at_level(logging.INFO, logger="src.core.observability"):
                emit(log_audit(
                    action="journal_read_capped",
                    source_bucket="my-bucket",
                    details={"row_cap": 500_000, "until_timestamp": "2024-01-02T00:00:00Z"},
                ))
            assert any(
                "journal_read_capped" in r.message for r in caplog.records
            )
        finally:
            root_logger.setLevel(original_root_level)


class TestReinvocationTriggeredLogContent:
    """`reinvocation_triggered` records the Reinvocation_Chain position
    (Requirement 6.2) and includes a timestamp (Requirement 6.4).
    """

    def test_event_type_is_reinvocation_triggered(self):
        entry = log_reinvocation_triggered(chain_position=1)
        assert entry["event"] == "reinvocation_triggered"

    def test_chain_position_recorded(self):
        entry = log_reinvocation_triggered(chain_position=5)
        assert entry["chain_position"] == 5

    def test_includes_timestamp(self):
        entry = log_reinvocation_triggered(chain_position=1)
        assert "timestamp" in entry
        datetime.fromisoformat(entry["timestamp"])

    def test_accepts_explicit_timestamp(self):
        entry = log_reinvocation_triggered(chain_position=2, timestamp=_NOW)
        assert entry["timestamp"] == _NOW.isoformat()

    def test_is_json_serializable(self):
        json.dumps(log_reinvocation_triggered(chain_position=1))


class TestReinvocationChainLimitReachedLogContent:
    """`reinvocation_chain_limit_reached` records that the limit stopped
    further reinvocation and backlog remains (Requirement 6.3), with a
    timestamp (Requirement 6.4).
    """

    def test_event_type_is_reinvocation_chain_limit_reached(self):
        entry = log_reinvocation_chain_limit_reached(chain_limit=20, depth=20)
        assert entry["event"] == "reinvocation_chain_limit_reached"

    def test_chain_limit_and_depth_recorded(self):
        entry = log_reinvocation_chain_limit_reached(chain_limit=20, depth=20)
        assert entry["chain_limit"] == 20
        assert entry["depth"] == 20

    def test_includes_timestamp(self):
        entry = log_reinvocation_chain_limit_reached(chain_limit=20, depth=20)
        assert "timestamp" in entry
        datetime.fromisoformat(entry["timestamp"])

    def test_is_json_serializable(self):
        json.dumps(log_reinvocation_chain_limit_reached(chain_limit=20, depth=20))


class TestRedactObjectKey:
    def test_non_empty_key_returns_redacted_form(self):
        result = redact_object_key("path/to/sensitive-file.txt")
        assert result.startswith("<redacted-key sha256:")
        assert "len=" in result

    def test_none_returns_empty_indicator(self):
        assert redact_object_key(None) == "<redacted-key empty>"

    def test_empty_string_returns_empty_indicator(self):
        assert redact_object_key("") == "<redacted-key empty>"

    def test_same_key_produces_same_fingerprint(self):
        key = "some/sensitive/path.txt"
        assert redact_object_key(key) == redact_object_key(key)

    def test_different_keys_produce_different_fingerprints(self):
        a = redact_object_key("key-a.txt")
        b = redact_object_key("key-b.txt")
        assert a != b
