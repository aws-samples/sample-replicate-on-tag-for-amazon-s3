"""Direct unit tests for src.core.job_recovery.plan_recovery.

Tests each of the four consolidation rules and the empty-watermark_low skip,
each confirmed non-vacuous by checking that inverting the corresponding rule
would produce a different result.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.core.job_recovery import (
    JobOutcome,
    RecoveryPlan,
    is_effective_failure,
    plan_recovery,
)
from src.core.models import SubmissionRecord, SubmissionStatus


def _make_record(
    config_id: str = "cfg-1",
    consecutive_failures: int = 0,
    watermark_low: str = "2024-01-01T00:00:00Z",
    watermark_high: str = "2024-01-02T00:00:00Z",
) -> SubmissionRecord:
    """Helper to build a SubmissionRecord for testing."""
    return SubmissionRecord(
        replication_config_id=config_id,
        source_bucket="test-bucket",
        job_id="job-123",
        manifest_key="manifests/m.csv",
        submitted_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        status=SubmissionStatus.SUBMITTED,
        watermark_low=watermark_low,
        watermark_high=watermark_high,
        consecutive_failures=consecutive_failures,
    )


def _make_outcome(
    status: str = "Failed",
    watermark_low: str = "2024-01-01T00:00:00Z",
    config_id: str = "cfg-1",
    tasks_succeeded: int | None = None,
    tasks_failed: int | None = None,
) -> JobOutcome:
    """Helper to build a JobOutcome for testing."""
    return JobOutcome(
        config_id=config_id,
        job_id="job-456",
        status=status,
        watermark_low=watermark_low,
        watermark_high="2024-01-02T00:00:00Z",
        consecutive_failures=0,
        tasks_succeeded=tasks_succeeded,
        tasks_failed=tasks_failed,
    )


# ---------------------------------------------------------------------------
# Rule 1: Seed from the maximum prior consecutive_failures
# ---------------------------------------------------------------------------


class TestRule1SeedFromMaxPrior:
    """The counter is seeded from the maximum consecutive_failures in records."""

    def test_seeds_from_max_prior_failures(self):
        """When multiple records exist, seed takes the highest failure count."""
        records = {
            "cfg-1": _make_record(config_id="cfg-1", consecutive_failures=2),
            "cfg-2": _make_record(config_id="cfg-2", consecutive_failures=5),
            "cfg-3": _make_record(config_id="cfg-3", consecutive_failures=3),
        }
        # One failure outcome -> should increment from the seed of 5.
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 6  # seeded at 5, incremented by 1

    def test_non_vacuous_seed_differs_from_zero(self):
        """Non-vacuous: if we seeded from 0 instead of max, result would differ."""
        records = {
            "cfg-1": _make_record(config_id="cfg-1", consecutive_failures=4),
        }
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        # Actual: seeded at 4, incremented to 5.
        assert plan.consecutive_failures == 5
        # If the rule were inverted (seed from 0): would be 1, not 5.
        assert plan.consecutive_failures != 1

    def test_empty_records_seeds_at_zero(self):
        """When no records exist, seed defaults to 0."""
        records: dict[str, SubmissionRecord] = {}
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 1  # seeded at 0, incremented by 1


# ---------------------------------------------------------------------------
# Rule 2: Increment by exactly one when any check failed
# ---------------------------------------------------------------------------


class TestRule2IncrementOnFailure:
    """Failure increments the counter by exactly one, regardless of how many failed."""

    def test_single_failure_increments_by_one(self):
        """One failed outcome increments by one from the seed."""
        records = {"cfg-1": _make_record(consecutive_failures=3)}
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 4

    def test_multiple_failures_still_increment_by_one(self):
        """Multiple failed outcomes still increment by exactly one, not by count."""
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes = [
            _make_outcome(status="Failed", config_id="cfg-1"),
            _make_outcome(status="Cancelled", config_id="cfg-2"),
            _make_outcome(status="Failed", config_id="cfg-3"),
        ]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 3  # seeded at 2, +1

    def test_non_vacuous_increment_differs_from_hold(self):
        """Non-vacuous: if we held instead of incrementing, result would differ."""
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 3
        # If rule were inverted (hold at seed): would be 2.
        assert plan.consecutive_failures != 2


# ---------------------------------------------------------------------------
# Rule 3: Reset to zero when at least one check ran and none failed
# ---------------------------------------------------------------------------


class TestRule3ResetOnAllSuccess:
    """When checks ran and none failed, the counter resets to zero."""

    def test_all_complete_resets_to_zero(self):
        """All outcomes Complete -> counter resets regardless of prior seed."""
        records = {"cfg-1": _make_record(consecutive_failures=7)}
        outcomes = [_make_outcome(status="Complete")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 0

    def test_non_vacuous_reset_differs_from_seed(self):
        """Non-vacuous: if we held at seed instead of resetting, result would differ."""
        records = {"cfg-1": _make_record(consecutive_failures=5)}
        outcomes = [_make_outcome(status="Complete")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 0
        # If rule were inverted (hold at seed): would be 5.
        assert plan.consecutive_failures != 5

    def test_no_rollback_when_all_success(self):
        """No rollback occurs when all jobs succeeded."""
        records = {"cfg-1": _make_record(consecutive_failures=3)}
        outcomes = [_make_outcome(status="Complete")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.rollback_to is None


# ---------------------------------------------------------------------------
# Rule 4: Hold at seeded value when no check could be evaluated
# ---------------------------------------------------------------------------


class TestRule4HoldWhenNoEvaluation:
    """When outcomes is empty (all DescribeJob calls failed), hold at seed."""

    def test_empty_outcomes_holds_at_seed(self):
        """No outcomes -> counter stays at the seeded value."""
        records = {"cfg-1": _make_record(consecutive_failures=4)}
        outcomes: list[JobOutcome] = []
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 4

    def test_non_vacuous_hold_differs_from_reset(self):
        """Non-vacuous: if we reset to 0 instead of holding, result would differ."""
        records = {"cfg-1": _make_record(consecutive_failures=6)}
        outcomes: list[JobOutcome] = []
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 6
        # If rule were inverted (reset): would be 0.
        assert plan.consecutive_failures != 0

    def test_no_rollback_when_no_evaluation(self):
        """No rollback when nothing could be evaluated."""
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes: list[JobOutcome] = []
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.rollback_to is None
        assert plan.skipped_empty_lows == 0


# ---------------------------------------------------------------------------
# Empty-watermark_low skip: rollback is skipped, not collapsed to epoch
# ---------------------------------------------------------------------------


class TestEmptyWatermarkLowSkip:
    """An empty watermark_low is excluded from rollback, not treated as epoch."""

    def test_empty_low_skipped_not_epoch(self):
        """Empty watermark_low outcomes are excluded; rollback_to is None."""
        records = {"cfg-1": _make_record(consecutive_failures=1)}
        outcomes = [
            _make_outcome(status="Failed", watermark_low=""),
        ]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        # The rollback should be None (skipped), NOT "" (epoch).
        assert plan.rollback_to is None
        assert plan.skipped_empty_lows == 1

    def test_mixed_empty_and_usable_lows(self):
        """When some lows are empty and some are not, only usable ones drive rollback."""
        records = {"cfg-1": _make_record(consecutive_failures=0)}
        outcomes = [
            _make_outcome(status="Failed", watermark_low=""),
            _make_outcome(status="Failed", watermark_low="2024-01-05T00:00:00Z"),
            _make_outcome(status="Failed", watermark_low="2024-01-03T00:00:00Z"),
        ]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.rollback_to == "2024-01-03T00:00:00Z"
        assert plan.skipped_empty_lows == 1

    def test_all_empty_lows_no_rollback(self):
        """When ALL failed outcomes have empty watermark_low, no rollback happens."""
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes = [
            _make_outcome(status="Failed", watermark_low=""),
            _make_outcome(status="Cancelled", watermark_low=""),
        ]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.rollback_to is None
        assert plan.skipped_empty_lows == 2
        # Counter still increments.
        assert plan.consecutive_failures == 3


# ---------------------------------------------------------------------------
# Threshold / disable reason
# ---------------------------------------------------------------------------


class TestIsEffectiveFailure:
    """A Complete job where every task failed is an effective failure,
    matching Failed/Cancelled — but only when tasks_succeeded is exactly 0."""

    def test_failed_status_is_effective_failure(self):
        outcome = _make_outcome(status="Failed")
        assert is_effective_failure(outcome) is True

    def test_cancelled_status_is_effective_failure(self):
        outcome = _make_outcome(status="Cancelled")
        assert is_effective_failure(outcome) is True

    def test_complete_with_no_progress_summary_is_not_a_failure(self):
        """tasks_succeeded/tasks_failed both None (no ProgressSummary on the
        DescribeJob response) — absence is not treated as either verdict."""
        outcome = _make_outcome(status="Complete")
        assert is_effective_failure(outcome) is False

    def test_complete_with_all_tasks_succeeded_is_not_a_failure(self):
        outcome = _make_outcome(status="Complete", tasks_succeeded=5, tasks_failed=0)
        assert is_effective_failure(outcome) is False

    def test_complete_with_zero_succeeded_and_some_failed_is_a_failure(self):
        """The InitiateReplicationNotPermitted signature: job completes,
        every task fails."""
        outcome = _make_outcome(status="Complete", tasks_succeeded=0, tasks_failed=1)
        assert is_effective_failure(outcome) is True

    def test_complete_with_zero_succeeded_and_zero_failed_is_not_a_failure(self):
        """0 succeeded and 0 failed means 0 total tasks — not a failure
        signature (e.g. an empty manifest); tasks_failed must be > 0."""
        outcome = _make_outcome(status="Complete", tasks_succeeded=0, tasks_failed=0)
        assert is_effective_failure(outcome) is False

    def test_complete_with_partial_success_is_not_a_failure(self):
        """A nonzero tasks_succeeded is never flagged, however large
        tasks_failed is — NumberOfTasksFailed alone is not a reliable
        signal at scale (see docstring in job_recovery.py), so only the
        all-zero-succeeded case counts."""
        outcome = _make_outcome(status="Complete", tasks_succeeded=1, tasks_failed=100001)
        assert is_effective_failure(outcome) is False

    def test_non_vacuous_all_tasks_failed_differs_from_status_only_check(self):
        """Non-vacuous: a status-only check (the old behavior) would call
        this Complete job non-failing; the all-tasks-failed check correctly
        flags it."""
        outcome = _make_outcome(status="Complete", tasks_succeeded=0, tasks_failed=3)
        assert outcome.status not in ("Failed", "Cancelled")
        assert is_effective_failure(outcome) is True


class TestPlanRecoveryAllTasksFailedComplete:
    """plan_recovery treats an all-tasks-failed Complete outcome exactly
    like Failed/Cancelled for both the failure counter and rollback."""

    def test_increments_failure_counter(self):
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes = [_make_outcome(status="Complete", tasks_succeeded=0, tasks_failed=1)]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 3

    def test_rollback_uses_watermark_low(self):
        records = {"cfg-1": _make_record(consecutive_failures=0)}
        outcomes = [
            _make_outcome(
                status="Complete",
                tasks_succeeded=0,
                tasks_failed=1,
                watermark_low="2024-01-03T00:00:00Z",
            )
        ]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.rollback_to == "2024-01-03T00:00:00Z"

    def test_can_reach_disable_threshold(self):
        records = {"cfg-1": _make_record(consecutive_failures=4)}
        outcomes = [_make_outcome(status="Complete", tasks_succeeded=0, tasks_failed=1)]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=5)
        assert plan.disable_reason is not None

    def test_a_genuinely_successful_complete_job_does_not_increment(self):
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes = [_make_outcome(status="Complete", tasks_succeeded=5, tasks_failed=0)]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=10)
        assert plan.consecutive_failures == 0


class TestThresholdDisable:
    """When failures reach the threshold, a disable_reason is produced."""

    def test_at_threshold_produces_disable_reason(self):
        """Reaching the threshold produces a non-None disable_reason."""
        records = {"cfg-1": _make_record(consecutive_failures=4)}
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=5)
        # seeded=4, +1 = 5, which >= threshold=5
        assert plan.disable_reason is not None
        assert "test-bucket" in plan.disable_reason
        assert "5" in plan.disable_reason

    def test_below_threshold_no_disable(self):
        """Below threshold: disable_reason is None."""
        records = {"cfg-1": _make_record(consecutive_failures=2)}
        outcomes = [_make_outcome(status="Failed")]
        plan = plan_recovery(records, outcomes, "test-bucket", threshold=5)
        assert plan.disable_reason is None
