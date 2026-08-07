"""Property test for ``is_report_overdue`` — task 23.2.

Feature: source-status-completion-tracking.

Property 17 also covers ``check_report_handler``'s escalation decision
(the report-does-not-exist AND is_report_overdue conjunction); that half is
covered by ``tests/test_lambda_handler.py::TestCheckReportHandler`` via
mocked boto3 clients, since it requires a full handler invocation. This
module covers the pure ``is_report_overdue`` function directly.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import is_report_overdue

_BASE = datetime(2024, 1, 1, tzinfo=timezone.utc)


class TestIsReportOverdueUnit:
    def test_exactly_one_hour_is_not_overdue(self):
        """Strictly greater than 1 hour — exactly 1 hour is NOT overdue."""
        now = _BASE + timedelta(hours=1)
        assert is_report_overdue(_BASE, now) is False

    def test_just_over_one_hour_is_overdue(self):
        now = _BASE + timedelta(hours=1, seconds=1)
        assert is_report_overdue(_BASE, now) is True

    def test_well_under_one_hour_is_not_overdue(self):
        now = _BASE + timedelta(minutes=30)
        assert is_report_overdue(_BASE, now) is False

    def test_well_over_one_hour_is_overdue(self):
        now = _BASE + timedelta(hours=5)
        assert is_report_overdue(_BASE, now) is True

    def test_now_equal_to_terminal_at_is_not_overdue(self):
        assert is_report_overdue(_BASE, _BASE) is False


# ---------------------------------------------------------------------------
# Property 17: A report-missing escalation fires if and only if more than
# 1 hour has elapsed since terminal status and the report is still absent
# Feature: source-status-completion-tracking, Property 17: A report-missing escalation fires if and only if more than 1 hour has elapsed since terminal status and the report is still absent
# Validates: Requirements 8.1, 8.2
# ---------------------------------------------------------------------------


class TestProperty17ReportOverdueBoundary:
    """# Feature: source-status-completion-tracking, Property 17: A report-missing escalation fires if and only if more than 1 hour has elapsed since terminal status and the report is still absent

    Validates: Requirements 8.1, 8.2
    """

    @given(
        elapsed_seconds=st.integers(min_value=-3600, max_value=3600 * 6),
    )
    @settings(max_examples=100)
    def test_overdue_iff_elapsed_exceeds_one_hour(self, elapsed_seconds: int) -> None:
        """# Feature: source-status-completion-tracking, Property 17: A report-missing escalation fires if and only if more than 1 hour has elapsed since terminal status and the report is still absent"""
        now = _BASE + timedelta(seconds=elapsed_seconds)
        result = is_report_overdue(_BASE, now)
        expected = elapsed_seconds > 3600
        assert result is expected

    @given(
        elapsed_seconds=st.integers(min_value=3601, max_value=3600 * 24),
        report_exists=st.booleans(),
    )
    @settings(max_examples=100)
    def test_overdue_result_independent_of_report_existence(
        self, elapsed_seconds: int, report_exists: bool
    ) -> None:
        """is_report_overdue itself takes no report-existence parameter — its
        result is independent of whatever report-existence value the caller
        later combines it with (the AND is performed by the caller, not this
        function).

        # Feature: source-status-completion-tracking, Property 17: A report-missing escalation fires if and only if more than 1 hour has elapsed since terminal status and the report is still absent
        """
        now = _BASE + timedelta(seconds=elapsed_seconds)
        result = is_report_overdue(_BASE, now)
        # Overdue is deterministic solely from the elapsed time.
        assert result is True
        # The caller's escalation decision is report-absent AND overdue;
        # report_exists is exercised in test_lambda_handler.py's handler-level
        # test, not here — this asserts is_report_overdue's own purity.
        escalate = (not report_exists) and result
        assert escalate == (not report_exists)
