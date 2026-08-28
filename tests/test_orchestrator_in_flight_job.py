"""A bucket is deferred once its outstanding Batch Operations job count is at the limit.

Every outstanding job is tracked, and up to ``MaxConcurrentJobsPerBucket`` may run
at once. At the limit the bucket is skipped before the journal is read; below it,
a job is submitted alongside the running ones.

Bounded rather than serialized. Serializing costs throughput exactly where long
jobs arise: one job already spans every one of a bucket's tag-scoped rules and
reaches terminal only when all its tasks do, so serializing extends that
head-of-line blocking across batches without limit, and holds the watermark
meanwhile. But not bounding replaces one unbounded behavior with another — a
bandwidth-bound bucket submitting every 15 minutes accumulates jobs at roughly
four an hour against an account quota this Solution does not model.

Reachable in ordinary use, because a Batch Replication job is gated on
replication throughput rather than task count: a bucket of large objects throttles
on bandwidth and its job can outlast many intervals.

**Validates: bounded-concurrent-jobs Requirements 2.1-2.7, 7.1**
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.core.job_recovery import (
    TERMINAL_JOB_STATUSES,
    JobOutcome,
    SubmissionRecord,
    is_terminal_status,
    plan_recovery,
)
from src.core.models import SubmissionStatus

_NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)  # noqa: UP017

# Every status an S3 Batch Operations job can hold without being finished.
_NON_TERMINAL_STATUSES = [
    "New",
    "Preparing",
    "Ready",
    "Suspended",
    "Active",
    "Paused",
    "Completing",
    "Cancelling",
    "Failing",
]


class TestTerminalStatusDefinition:
    """One definition, shared by the three consumers that must agree."""

    @pytest.mark.parametrize("status", ["Complete", "Failed", "Cancelled"])
    def test_terminal_statuses(self, status):
        assert is_terminal_status(status)

    @pytest.mark.parametrize("status", _NON_TERMINAL_STATUSES)
    def test_non_terminal_statuses(self, status):
        assert not is_terminal_status(status)

    def test_none_and_unknown_are_not_terminal(self):
        """An unreadable status must not be mistaken for a finished job."""
        assert not is_terminal_status(None)
        assert not is_terminal_status("SomethingNew")

    def test_the_set_is_exactly_the_three_documented_statuses(self):
        assert TERMINAL_JOB_STATUSES == frozenset({"Complete", "Failed", "Cancelled"})


class TestRunningJobDoesNotResetTheFailureCounter:
    """A running job is not evidence that anything succeeded.

    ``plan_recovery`` reads a non-empty ``outcomes`` with no failures as "a check
    ran and passed" and resets the counter. The orchestrator therefore keeps a
    non-terminal job out of ``outcomes`` entirely, so rule 4 holds it instead.
    Without that, a bucket whose jobs keep failing could reset its own counter
    whenever a fresh job happened to be in flight, and the circuit breaker would
    never trip.
    """

    @staticmethod
    def _record(consecutive_failures: int) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id="my-bucket",
            job_id="job-prior",
            manifest_key="manifests/my-bucket/x/manifest.json",
            submitted_at=_NOW - timedelta(hours=1),
            status=SubmissionStatus.SUBMITTED,
            source_bucket="my-bucket",
            watermark_low="2026-08-27T10:00:00.000000Z",
            watermark_high="2026-08-27T11:00:00.000000Z",
            consecutive_failures=consecutive_failures,
        )

    def test_empty_outcomes_holds_the_seeded_counter(self):
        plan = plan_recovery(
            records={"my-bucket": self._record(3)},
            outcomes=[],
            bucket_name="my-bucket",
            threshold=4,
        )

        assert plan.consecutive_failures == 3
        assert plan.rollback_to is None
        assert plan.disable_reason is None

    def test_holding_lets_the_breaker_trip_on_the_next_real_failure(self):
        """Non-vacuous: the held counter still reaches the threshold."""
        plan = plan_recovery(
            records={"my-bucket": self._record(3)},
            outcomes=[
                JobOutcome(
                    config_id="my-bucket",
                    job_id="job-next",
                    status="Failed",
                    watermark_low="2026-08-27T10:00:00.000000Z",
                    watermark_high="2026-08-27T11:00:00.000000Z",
                    consecutive_failures=3,
                )
            ],
            bucket_name="my-bucket",
            threshold=4,
        )

        assert plan.consecutive_failures == 4
        assert plan.disable_reason is not None

    def test_a_reset_would_have_prevented_that(self):
        """Shows what scoring a running job as a passing check would cost.

        Had the counter been reset to 0 first, the same failure would leave it at
        1, four short of the threshold, and the bucket would keep submitting.
        """
        reset = plan_recovery(
            records={"my-bucket": self._record(0)},
            outcomes=[
                JobOutcome(
                    config_id="my-bucket",
                    job_id="job-next",
                    status="Failed",
                    watermark_low="2026-08-27T10:00:00.000000Z",
                    watermark_high="2026-08-27T11:00:00.000000Z",
                    consecutive_failures=0,
                )
            ],
            bucket_name="my-bucket",
            threshold=4,
        )

        assert reset.consecutive_failures == 1
        assert reset.disable_reason is None


def _prior(
    job_id: str = "job-running",
    *,
    submitted_at=None,
) -> SubmissionRecord:
    return SubmissionRecord(
        replication_config_id="my-bucket",
        job_id=job_id,
        manifest_key=f"manifests/my-bucket/{job_id}/manifest.json",
        submitted_at=submitted_at or (_NOW - timedelta(hours=2)),
        status=SubmissionStatus.SUBMITTED,
        source_bucket="my-bucket",
        watermark_low="2026-08-27T09:00:00.000000Z",
        watermark_high="2026-08-27T10:00:00.000000Z",
    )


def _describe_ctx(s3control) -> MagicMock:
    ctx = MagicMock()
    ctx.bucket_name = "my-bucket"
    ctx.s3control_client = s3control
    ctx.account_id = "123456789012"
    ctx.state_bucket = "state-bucket"
    return ctx


class TestDescribePriorJobsReportsOutstandingJobs:
    """The DescribeJob loop carries every unfinished job out rather than scoring it."""

    @staticmethod
    def _run(job_status: str, created_at=None):
        from src import orchestrator

        s3control = MagicMock()
        s3control.describe_job.return_value = {
            "Job": {
                "Status": job_status,
                "CreationTime": created_at or (_NOW - timedelta(hours=2)),
                "ProgressSummary": {
                    "NumberOfTasksSucceeded": 1,
                    "NumberOfTasksFailed": 0,
                },
            }
        }

        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            check = orchestrator._describe_prior_jobs(
                _describe_ctx(s3control),
                MagicMock(),
                MagicMock(),
                {"job-running": _prior()},
                orchestrator._NullCompletionHooks(),
            )
        return check, emitted

    @pytest.mark.parametrize("status", _NON_TERMINAL_STATUSES)
    def test_a_running_job_is_reported_as_outstanding(self, status):
        check, _ = self._run(status)

        assert len(check.outstanding) == 1
        assert check.outstanding[0].job_id == "job-running"
        assert check.outstanding[0].status == status

    @pytest.mark.parametrize("status", _NON_TERMINAL_STATUSES)
    def test_a_running_job_produces_no_outcome(self, status):
        """Keeping it out of `outcomes` is what makes plan_recovery hold."""
        check, _ = self._run(status)

        assert check.outcomes == []
        assert check.any_check_failed is False
        assert check.terminal_job_ids == []

    def test_a_terminal_job_is_scored_and_not_outstanding(self):
        """Non-vacuous: a finished job still reaches the recovery arithmetic."""
        check, _ = self._run("Complete")

        assert check.outstanding == []
        assert len(check.outcomes) == 1
        assert check.outcomes[0].status == "Complete"
        assert check.terminal_job_ids == ["job-running"]

    def test_outstanding_job_age_is_measured_from_creation(self):
        check, _ = self._run("Active", created_at=_NOW - timedelta(days=7))

        elapsed = check.outstanding[0].elapsed_seconds(_NOW)
        assert elapsed == pytest.approx(7 * 24 * 3600)

    def test_missing_creation_time_yields_no_age_rather_than_raising(self):
        from src import orchestrator

        in_flight = orchestrator._InFlightJob(
            job_id="job-running", status="Active", created_at=None
        )

        assert in_flight.elapsed_seconds(_NOW) is None


class TestEveryRecordIsVisited:
    """One job per record, not one record per bucket (Requirement 1.3)."""

    @staticmethod
    def _run(statuses: dict[str, str], failing: set[str] | None = None):
        from src import orchestrator

        failing = failing or set()

        def describe_job(AccountId, JobId):  # noqa: N803 — boto3 parameter name
            if JobId in failing:
                raise RuntimeError("transport error")
            return {
                "Job": {
                    "Status": statuses[JobId],
                    "CreationTime": _NOW - timedelta(hours=1),
                    "ProgressSummary": {
                        "NumberOfTasksSucceeded": 1,
                        "NumberOfTasksFailed": 0,
                    },
                }
            }

        s3control = MagicMock()
        s3control.describe_job.side_effect = describe_job

        records = {
            job_id: _prior(job_id, submitted_at=_NOW - timedelta(hours=index + 1))
            for index, job_id in enumerate(statuses)
        }

        with patch("src.orchestrator.observability.emit"):
            return orchestrator._describe_prior_jobs(
                _describe_ctx(s3control),
                MagicMock(),
                MagicMock(),
                records,
                orchestrator._NullCompletionHooks(),
            )

    def test_three_running_jobs_are_all_reported(self):
        check = self._run({"job-a": "Active", "job-b": "Active", "job-c": "Preparing"})

        assert {job.job_id for job in check.outstanding} == {
            "job-a", "job-b", "job-c",
        }

    def test_a_mix_reports_the_running_and_scores_the_finished(self):
        check = self._run({"job-a": "Active", "job-b": "Complete"})

        assert [job.job_id for job in check.outstanding] == ["job-a"]
        assert [outcome.job_id for outcome in check.outcomes] == ["job-b"]
        assert check.terminal_job_ids == ["job-b"]

    def test_a_describe_failure_counts_as_outstanding(self):
        """Its status is unknown, and assuming it finished is the unsafe
        assumption: it would let the bucket look under its limit and submit
        alongside a job that is still running."""
        check = self._run({"job-a": "Active", "job-b": "Complete"}, failing={"job-b"})

        assert {job.job_id for job in check.outstanding} == {"job-a", "job-b"}
        assert check.outcomes == []
        assert check.terminal_job_ids == []

    def test_a_describe_failure_does_not_disturb_the_other_records(self):
        check = self._run(
            {"job-a": "Complete", "job-b": "Complete"}, failing={"job-a"}
        )

        assert [outcome.job_id for outcome in check.outcomes] == ["job-b"]
        assert [job.job_id for job in check.outstanding] == ["job-a"]

    def test_the_report_prefix_identity_is_the_bucket_not_the_record_key(self):
        """The prefix is written from the bucket name at submission time, so the
        read side must derive it the same way or the report is never found."""
        check = self._run({"job-a": "Complete"})

        assert check.outcomes[0].config_id == "my-bucket"


class TestOldestOutstandingJob:
    """The deferral audit names the job an operator would investigate."""

    def test_the_oldest_by_submitted_at_wins(self):
        from src import orchestrator

        jobs = [
            orchestrator._InFlightJob(
                "job-new", "Active", None, submitted_at=_NOW - timedelta(hours=1),
            ),
            orchestrator._InFlightJob(
                "job-old", "Active", None, submitted_at=_NOW - timedelta(days=3),
            ),
        ]

        assert orchestrator._oldest_outstanding(jobs).job_id == "job-old"

    def test_a_missing_submitted_at_sorts_last_rather_than_raising(self):
        from src import orchestrator

        jobs = [
            orchestrator._InFlightJob("job-unknown", "Unknown", None, None),
            orchestrator._InFlightJob(
                "job-known", "Active", None, submitted_at=_NOW,
            ),
        ]

        assert orchestrator._oldest_outstanding(jobs).job_id == "job-known"

    def test_empty_yields_none(self):
        from src import orchestrator

        assert orchestrator._oldest_outstanding([]) is None


class TestDeferredSubmissionIsVisible:
    """A deferral must be observable, or an operator sees no jobs and no reason."""

    def test_bucket_metrics_carries_the_flag(self):
        from src.core.models import BucketMetrics

        metrics = BucketMetrics(
            source_bucket="my-bucket",
            ops_read=0,
            matched=0,
            submitted=0,
            errored=False,
            submission_deferred=True,
        )

        assert metrics.submission_deferred is True
        assert metrics.errored is False, "deferring is not an error"

    def test_a_deferred_bucket_publishes_the_metric(self):
        from src.adapters.metrics_publisher import _build_metric_data
        from src.core.models import BucketMetrics, RunResult

        datums = _build_metric_data(
            RunResult(
                buckets=[
                    BucketMetrics(
                        source_bucket="my-bucket",
                        ops_read=0,
                        matched=0,
                        submitted=0,
                        errored=False,
                        submission_deferred=True,
                    )
                ],
            )
        )

        deferred = [d for d in datums if d["MetricName"] == "SubmissionDeferred"]
        assert len(deferred) == 1
        assert deferred[0]["Value"] == 1.0
        assert {"Name": "SourceBucket", "Value": "my-bucket"} in deferred[0]["Dimensions"]

    def test_a_normal_run_publishes_no_deferred_metric(self):
        """Emitted only when it happened, so an alarm can treat missing as fine."""
        from src.adapters.metrics_publisher import _build_metric_data
        from src.core.models import BucketMetrics, RunResult

        datums = _build_metric_data(
            RunResult(
                buckets=[
                    BucketMetrics(
                        source_bucket="my-bucket",
                        ops_read=5,
                        matched=5,
                        submitted=1,
                        errored=False,
                    )
                ],
            )
        )

        assert not [d for d in datums if d["MetricName"] == "SubmissionDeferred"]


# ---------------------------------------------------------------------------
# The bounded guard in _process_bucket (Requirements 2.2-2.7, 7.1)
#
# _process_bucket is not driven end to end here: it hangs on unpatched AWS
# boundaries. The skip is exercised by controlling what
# _prepare_state_and_recovery returns, which is the only input the guard reads.
# ---------------------------------------------------------------------------


def _outstanding_jobs(count: int, status: str = "Active"):
    from src import orchestrator

    return [
        orchestrator._InFlightJob(
            job_id=f"job-{index}",
            status=status,
            created_at=_NOW - timedelta(hours=index + 1),
            submitted_at=_NOW - timedelta(hours=index + 1),
        )
        for index in range(count)
    ]


def _run_guard(outstanding_count: int, limit: int, status: str = "Active"):
    """Drive _process_bucket's guard. Returns (result, emitted, journal_read_called).

    _process_bucket is not driven end to end: it hangs on unpatched AWS
    boundaries. The guard reads only what _prepare_state_and_recovery returns, so
    controlling that is enough and does not require a live-looking AWS surface.
    """
    from src import orchestrator

    job_check = orchestrator._JobCheckResult(
        failed_lows=[],
        any_check_ran=False,
        any_check_failed=False,
        outcomes=[],
        outstanding=_outstanding_jobs(outstanding_count, status),
    )
    prep = (
        MagicMock(),                     # writer
        "2026-08-27T09:00:00.000000Z",   # checkpoint_watermark
        orchestrator._NullCompletionHooks(),
        0,                               # consecutive failures
        MagicMock(),                     # state
        job_check,
    )

    bucket = MagicMock()
    bucket.name = "my-bucket"
    bucket.region = "us-west-2"

    emitted: list = []
    with (
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch(
            "src.orchestrator._create_clients",
            return_value=(MagicMock(), MagicMock(), MagicMock()),
        ),
        patch("src.orchestrator._resolve_rules", return_value=[MagicMock()]),
        patch(
            "src.orchestrator._prepare_state_and_recovery", return_value=prep,
        ),
        patch("src.orchestrator._read_journal_window") as read_journal,
    ):
        read_journal.return_value = None  # ends the run if it is ever reached
        result = orchestrator._process_bucket(
            bucket=bucket,
            store=MagicMock(),
            factory=MagicMock(),
            state_bucket="state-bucket",
            athena_workgroup="wg",
            athena_output_location="s3://state-bucket/athena/",
            account_id="123456789012",
            batch_operations_role_arn="arn:aws:iam::123456789012:role/r",
            max_concurrent_jobs=limit,
        )
    return result, emitted, read_journal.called


def _deferral_entries(emitted: list) -> list[dict]:
    return [
        entry for entry in emitted
        if isinstance(entry, dict)
        and entry.get("action") == "submission_deferred_job_in_flight"
    ]


class TestBoundedDeferralGuard:
    def test_below_the_limit_the_bucket_is_not_deferred(self):
        """Non-vacuous: the guard has to let work through, or the bound is just
        serialization by another name."""
        result, emitted, journal_read = _run_guard(outstanding_count=2, limit=3)

        assert result.submission_deferred is False
        assert _deferral_entries(emitted) == []
        assert journal_read is True

    def test_at_the_limit_the_bucket_is_deferred(self):
        result, emitted, journal_read = _run_guard(outstanding_count=3, limit=3)

        assert result.submission_deferred is True
        assert len(_deferral_entries(emitted)) == 1
        # Before the journal is read, so no Athena query is billed for a run that
        # cannot submit (Requirement 2.3).
        assert journal_read is False

    def test_above_the_limit_the_bucket_is_deferred(self):
        """Reachable after an operator lowers the limit with jobs already running."""
        result, _, journal_read = _run_guard(outstanding_count=5, limit=3)

        assert result.submission_deferred is True
        assert journal_read is False

    def test_a_limit_of_one_reproduces_serialization(self):
        """The floor is 1 rather than 2 precisely so an operator who wants strict
        serialization can still choose it (Requirement 2.1)."""
        deferred, _, journal_read = _run_guard(outstanding_count=1, limit=1)
        assert deferred.submission_deferred is True
        assert journal_read is False

        clear, _, journal_read_clear = _run_guard(outstanding_count=0, limit=1)
        assert clear.submission_deferred is False
        assert journal_read_clear is True

    def test_a_describe_failed_job_counts_toward_the_limit(self):
        result, emitted, _ = _run_guard(
            outstanding_count=3, limit=3, status="Unknown",
        )

        assert result.submission_deferred is True
        assert _deferral_entries(emitted)[0]["job_status"] == "Unknown"

    def test_the_deferral_is_not_an_error(self):
        """The run did what it should. Recording it as a bucket error would make
        BucketErrors unusable as an alarm."""
        result, _, _ = _run_guard(outstanding_count=3, limit=3)

        assert result.errored is False

    def test_the_deferral_leaves_the_counters_untouched(self):
        """No checkpoint advance, no lease, nothing added to the processed window,
        so every eligible tagging event stays eligible (Requirement 2.4)."""
        result, _, _ = _run_guard(outstanding_count=3, limit=3)

        assert result.submitted == 0
        assert result.ops_read == 0
        assert result.matched == 0
        assert result.progressed is False

    def test_the_audit_entry_carries_the_count_and_the_limit(self):
        _, emitted, _ = _run_guard(outstanding_count=4, limit=3)

        # log_audit merges `details` flat into the entry.
        entry = _deferral_entries(emitted)[0]
        assert entry["outstanding_count"] == 4
        assert entry["limit"] == 3

    def test_the_audit_entry_names_the_oldest_job(self):
        """The oldest is the one an operator would investigate, so naming an
        arbitrary one would send them to the wrong job."""
        _, emitted, _ = _run_guard(outstanding_count=3, limit=3)

        entry = _deferral_entries(emitted)[0]
        # _outstanding builds job-N at NOW - (N+1) hours, so job-2 is oldest.
        assert entry["job_id"] == "job-2"
        assert entry["job_status"] == "Active"
        # Measured, not omitted. The value is relative to the real clock rather
        # than the fixture's _NOW, so only its presence is asserted here; the
        # arithmetic is covered by
        # TestDescribePriorJobsReportsOutstandingJobs.test_outstanding_job_age_is_measured_from_creation.
        assert entry["job_age_seconds"] is not None

    def test_a_job_with_no_creation_time_still_produces_the_audit_entry(self):
        """A describe-failed job has no CreationTime. The entry must still be
        emitted, or the deferral becomes invisible for exactly the case where an
        operator has least information."""
        _, emitted, _ = _run_guard(outstanding_count=3, limit=3, status="Unknown")

        entry = _deferral_entries(emitted)[0]
        assert entry["job_id"] == "job-2"
        assert entry["outstanding_count"] == 3

    def test_the_outstanding_count_is_reported_for_the_completion_report(self):
        result, _, _ = _run_guard(outstanding_count=2, limit=3)

        assert result.outstanding_jobs == 2


class TestRunStateReachesThePublishPhase:
    """The publish phase cannot derive these two values from the state object, so
    they are threaded across the isolation boundary rather than the phases merged.
    """

    def test_the_carrier_defaults_to_an_unknown_job_count(self):
        """Zero would be a claim. A bucket with no entry is one that never got as
        far as checking its jobs, or was skipped as disabled — and a bucket is
        disabled because its jobs kept failing, so a false all-clear there is the
        worst case."""
        from src import orchestrator

        state = orchestrator._BucketRunState()

        assert state.outstanding_jobs is None
        # submission_deferred needs no unknown case: for a bucket that never ran,
        # "was it skipped at the concurrency limit" is a plain no.
        assert state.submission_deferred is False

    def test_a_bucket_result_starts_with_an_unknown_job_count(self):
        """Every path that returns before the DescribeJob loop leaves it unknown:
        a client-creation failure, a rule-resolution failure, or a checkpoint read
        failure."""
        from src import orchestrator

        assert orchestrator._BucketResult().outstanding_jobs is None


# ---------------------------------------------------------------------------
# MaxConcurrentJobsPerBucket reaches _process_bucket (Requirement 2.1)
# ---------------------------------------------------------------------------


_RUNTIME = {
    "state_bucket": "scratch-state-bucket",
    "athena_workgroup": "primary",
    "athena_output_location": "s3://scratch-state-bucket/athena/",
    "account_id": "123456789012",
    "batch_operations_role_arn": "arn:aws:iam::123456789012:role/s3rot-batch-operations-role",
    "region": "us-east-1",
}
_CONFIG = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}


class TestTheLimitIsThreadedAndClamped:
    """An unwired parameter is worse than an absent one: it reads as configurable
    while every deployment silently runs the code default.
    """

    @staticmethod
    def _limit_seen(runtime_extra: dict) -> int:
        from src import orchestrator

        with (
            patch("src.orchestrator.ClientFactory"),
            patch("src.orchestrator.state_store_module.StateStore"),
            patch("src.orchestrator._check_bucket_disabled", return_value=None),
            patch("src.orchestrator.observability.emit"),
            patch("src.orchestrator.MetricsPublisher"),
            patch(
                "src.orchestrator._process_bucket",
                return_value=orchestrator._BucketResult(),
            ) as process,
        ):
            orchestrator.run_interval(_CONFIG, {**_RUNTIME, **runtime_extra})
        return process.call_args.kwargs["max_concurrent_jobs"]

    def test_the_runtime_value_reaches_process_bucket(self):
        assert self._limit_seen({"max_concurrent_jobs_per_bucket": 7}) == 7

    def test_an_absent_value_falls_back_to_the_module_default(self):
        from src.orchestrator import MAX_CONCURRENT_JOBS_DEFAULT

        assert self._limit_seen({}) == MAX_CONCURRENT_JOBS_DEFAULT

    @pytest.mark.parametrize("bad", [0, -1, -100])
    def test_a_value_below_one_is_clamped_to_one(self, bad):
        """0 would defer every bucket forever, which is a way to stop the Solution
        entirely and not something a tuning knob should do by accident."""
        assert self._limit_seen({"max_concurrent_jobs_per_bucket": bad}) == 1

    def test_a_string_value_is_accepted(self):
        """The env var arrives as text, and _build_runtime_config's int() can be
        bypassed by a library caller passing the raw value through."""
        assert self._limit_seen({"max_concurrent_jobs_per_bucket": "4"}) == 4


# ---------------------------------------------------------------------------
# Recovery arithmetic with several outcomes in one run (Requirements 4.1-4.4)
#
# Several outcomes per run is the ordinary case now: a bucket may have up to
# MaxConcurrentJobsPerBucket jobs outstanding, and several can reach terminal
# between two runs. plan_recovery already handles it; these tests fix the
# semantics so a future change cannot quietly alter them.
# ---------------------------------------------------------------------------


class TestRecoveryWithSeveralOutcomes:
    _WM_EARLY = "2026-08-27T08:00:00.000000Z"
    _WM_MID = "2026-08-27T09:00:00.000000Z"
    _WM_LATE = "2026-08-27T10:00:00.000000Z"

    @staticmethod
    def _record(job_id: str, consecutive_failures: int = 0) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id="my-bucket",
            job_id=job_id,
            manifest_key=f"manifests/my-bucket/{job_id}/manifest.json",
            submitted_at=_NOW - timedelta(hours=1),
            status=SubmissionStatus.SUBMITTED,
            source_bucket="my-bucket",
            watermark_low="2026-08-27T08:00:00.000000Z",
            watermark_high="2026-08-27T11:00:00.000000Z",
            consecutive_failures=consecutive_failures,
        )

    @staticmethod
    def _outcome(job_id: str, status: str, watermark_low: str) -> JobOutcome:
        return JobOutcome(
            config_id="my-bucket",
            job_id=job_id,
            status=status,
            watermark_low=watermark_low,
            watermark_high="2026-08-27T11:00:00.000000Z",
            consecutive_failures=0,
        )

    def _plan(self, records, outcomes, threshold=4):
        return plan_recovery(
            records=records,
            outcomes=outcomes,
            bucket_name="my-bucket",
            threshold=threshold,
        )

    def test_one_failure_among_several_successes_still_counts_as_a_failure(self):
        plan = self._plan(
            records={
                "job-a": self._record("job-a"),
                "job-b": self._record("job-b"),
                "job-c": self._record("job-c"),
            },
            outcomes=[
                self._outcome("job-a", "Complete", self._WM_EARLY),
                self._outcome("job-b", "Failed", self._WM_MID),
                self._outcome("job-c", "Complete", self._WM_LATE),
            ],
        )

        assert plan.consecutive_failures == 1
        assert plan.rollback_to == self._WM_MID

    def test_several_failures_in_one_run_increment_by_exactly_one(self):
        """The counter measures consecutive failing *intervals*, which is how
        MaxBatchJobFailures reads. Per-job would trip the breaker N times faster
        at a concurrency of N for one underlying fault — the same misconfigured
        role failing three jobs would look like three consecutive failures."""
        plan = self._plan(
            records={
                "job-a": self._record("job-a", consecutive_failures=1),
                "job-b": self._record("job-b", consecutive_failures=1),
                "job-c": self._record("job-c", consecutive_failures=1),
            },
            outcomes=[
                self._outcome("job-a", "Failed", self._WM_MID),
                self._outcome("job-b", "Failed", self._WM_LATE),
                self._outcome("job-c", "Cancelled", self._WM_EARLY),
            ],
        )

        assert plan.consecutive_failures == 2

    def test_three_failures_in_one_run_do_not_trip_a_threshold_of_four(self):
        """Non-vacuous companion to the above: per-job counting would reach 4 and
        disable the bucket on a single interval's fault."""
        plan = self._plan(
            records={"job-a": self._record("job-a", consecutive_failures=1)},
            outcomes=[
                self._outcome("job-a", "Failed", self._WM_MID),
                self._outcome("job-b", "Failed", self._WM_LATE),
                self._outcome("job-c", "Failed", self._WM_EARLY),
            ],
            threshold=4,
        )

        assert plan.consecutive_failures == 2
        assert plan.disable_reason is None

    def test_a_non_terminal_job_holds_the_counter_rather_than_resetting_it(self):
        """The orchestrator keeps a non-terminal job out of `outcomes`, so an
        outstanding job among finished ones contributes nothing. With no terminal
        outcome at all, rule 4 holds the seeded value."""
        plan = self._plan(
            records={
                "job-running": self._record("job-running", consecutive_failures=3),
                "job-also-running": self._record("job-also-running"),
            },
            outcomes=[],
        )

        assert plan.consecutive_failures == 3
        assert plan.rollback_to is None
        assert plan.disable_reason is None

    def test_a_running_job_alongside_a_success_does_not_block_the_reset(self):
        """Rule 3 still applies when at least one job did finish cleanly: the
        running job is simply not evidence either way."""
        plan = self._plan(
            records={
                "job-running": self._record("job-running", consecutive_failures=3),
                "job-done": self._record("job-done", consecutive_failures=3),
            },
            outcomes=[self._outcome("job-done", "Complete", self._WM_MID)],
        )

        assert plan.consecutive_failures == 0

    def test_the_rollback_target_is_the_minimum_failed_watermark_low(self):
        """Anything later would leave the earlier failure's objects unreplicated
        with no retry and no alert."""
        plan = self._plan(
            records={"job-a": self._record("job-a")},
            outcomes=[
                self._outcome("job-a", "Failed", self._WM_LATE),
                self._outcome("job-b", "Failed", self._WM_EARLY),
                self._outcome("job-c", "Failed", self._WM_MID),
            ],
        )

        assert plan.rollback_to == self._WM_EARLY

    def test_a_successful_jobs_watermark_does_not_pull_the_rollback_back(self):
        plan = self._plan(
            records={"job-a": self._record("job-a")},
            outcomes=[
                self._outcome("job-a", "Complete", self._WM_EARLY),
                self._outcome("job-b", "Failed", self._WM_LATE),
            ],
        )

        assert plan.rollback_to == self._WM_LATE

    def test_the_counter_seeds_from_the_maximum_across_records(self):
        """With several records the seed cannot be "the record's" value, so the
        maximum is taken: a bucket's failure history is bucket-level."""
        plan = self._plan(
            records={
                "job-a": self._record("job-a", consecutive_failures=1),
                "job-b": self._record("job-b", consecutive_failures=3),
                "job-c": self._record("job-c", consecutive_failures=0),
            },
            outcomes=[self._outcome("job-a", "Failed", self._WM_MID)],
        )

        assert plan.consecutive_failures == 4

    def test_an_empty_watermark_low_among_failures_does_not_collapse_to_the_epoch(self):
        plan = self._plan(
            records={"job-a": self._record("job-a")},
            outcomes=[
                self._outcome("job-a", "Failed", ""),
                self._outcome("job-b", "Failed", self._WM_MID),
            ],
        )

        assert plan.rollback_to == self._WM_MID
        assert plan.skipped_empty_lows == 1


# ---------------------------------------------------------------------------
# The deferral must have an exit
#
# A record only leaves the outstanding count by being described at a terminal
# status. Both paths that delete a record — pruning and ceiling eviction — run
# inside the write that persists a submission, which the deferral prevents, and
# the circuit breaker cannot help because empty `outcomes` makes plan_recovery
# hold the counter. Without a bound, one un-describable record stalls the bucket
# for good, and at MaxConcurrentJobsPerBucket=1 that takes exactly one.
# ---------------------------------------------------------------------------


class TestUndescribableJobsDoNotStallTheBucketForever:
    @staticmethod
    def _run(age: timedelta):
        from src import orchestrator

        s3control = MagicMock()
        s3control.describe_job.side_effect = RuntimeError("AccessDenied")

        record = _prior("job-stuck", submitted_at=datetime.now(tz=timezone.utc) - age)  # noqa: UP017

        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            check = orchestrator._describe_prior_jobs(
                _describe_ctx(s3control),
                MagicMock(),
                MagicMock(),
                {"job-stuck": record},
                orchestrator._NullCompletionHooks(),
            )
        return check, emitted

    def test_a_recent_undescribable_job_still_counts(self):
        """Non-vacuous: the safe reading has to hold for the ordinary case, or a
        transient DescribeJob error would admit a job alongside a running one."""
        check, _ = self._run(timedelta(hours=6))

        assert [job.job_id for job in check.outstanding] == ["job-stuck"]

    def test_a_job_undescribable_for_longer_than_the_bound_stops_counting(self):
        from src.orchestrator import _UNDESCRIBABLE_JOB_MAX_AGE

        check, _ = self._run(_UNDESCRIBABLE_JOB_MAX_AGE + timedelta(days=1))

        assert check.outstanding == []

    def test_ageing_a_job_out_of_the_count_is_reported_as_an_error(self):
        """It is tracking state ceasing to have effect, and the operator needs the
        cause: a lost s3:DescribeJob permission or a job id from another account.
        """
        from src.orchestrator import _UNDESCRIBABLE_JOB_MAX_AGE

        _, emitted = self._run(_UNDESCRIBABLE_JOB_MAX_AGE + timedelta(days=1))

        errors = [
            entry for entry in emitted
            if isinstance(entry, dict) and entry.get("event") == "error"
            and "no longer counts toward" in entry.get("cause", "")
        ]
        assert len(errors) == 1
        assert "job-stuck" in errors[0]["cause"]
        assert "s3:DescribeJob" in errors[0]["cause"]

    def test_the_record_is_not_scored_as_an_outcome_either_way(self):
        """Ageing it out of the count must not make it look like a check that ran,
        or it would reset the bucket's consecutive-failure counter."""
        from src.orchestrator import _UNDESCRIBABLE_JOB_MAX_AGE

        check, _ = self._run(_UNDESCRIBABLE_JOB_MAX_AGE + timedelta(days=1))

        assert check.outcomes == []
        assert check.any_check_ran is False
        assert check.terminal_job_ids == []

    def test_a_naive_submitted_at_does_not_raise(self):
        """A hand-edited state object can carry one, and raising here would be on
        the path that decides whether the bucket may submit at all."""
        from src import orchestrator

        s3control = MagicMock()
        s3control.describe_job.side_effect = RuntimeError("boom")
        record = SubmissionRecord(
            replication_config_id="my-bucket",
            job_id="job-naive",
            manifest_key="manifests/my-bucket/naive/manifest.json",
            submitted_at=datetime(2020, 1, 1),  # noqa: DTZ001 — the point of the test
            status=SubmissionStatus.SUBMITTED,
            source_bucket="my-bucket",
        )

        with patch("src.orchestrator.observability.emit"):
            check = orchestrator._describe_prior_jobs(
                _describe_ctx(s3control),
                MagicMock(),
                MagicMock(),
                {"job-naive": record},
                orchestrator._NullCompletionHooks(),
            )

        # Treated as UTC and therefore far past the bound, so it stops counting.
        assert check.outstanding == []

    def test_the_bound_is_far_beyond_any_plausible_job(self):
        """The bound governs cost, not correctness — records stay tracked either
        way — but it must not fire on a genuinely long-running job."""
        from src.orchestrator import _UNDESCRIBABLE_JOB_MAX_AGE

        assert _UNDESCRIBABLE_JOB_MAX_AGE >= timedelta(days=7)
        # Inside the 90 days S3 Batch Operations retains a job record.
        assert _UNDESCRIBABLE_JOB_MAX_AGE < timedelta(days=90)


class TestAnAllUnknownDeferralEscalates:
    """A deferral held up entirely by jobs whose status could not be read is not
    the ordinary "a job outlasted an interval" case, and an audit entry is the
    wrong weight for it. It resolves on its own, but not before an operator would
    otherwise have had to notice a stretch of SubmissionDeferred datapoints.
    """

    @staticmethod
    def _stall_errors(emitted: list) -> list[dict]:
        return [
            entry for entry in emitted
            if isinstance(entry, dict) and entry.get("event") == "error"
            and "status of all" in entry.get("cause", "")
        ]

    def test_an_all_unknown_deferral_emits_an_error(self):
        _, emitted, _ = _run_guard(outstanding_count=3, limit=3, status="Unknown")

        errors = self._stall_errors(emitted)
        assert len(errors) == 1
        assert "s3:DescribeJob" in errors[0]["cause"]

    def test_an_ordinary_deferral_emits_no_error(self):
        """Non-vacuous: a bucket whose jobs are simply slow is working correctly,
        and reporting that as an error would make the signal useless."""
        _, emitted, _ = _run_guard(outstanding_count=3, limit=3, status="Active")

        assert self._stall_errors(emitted) == []


# ---------------------------------------------------------------------------
# A terminal job's outcome is scored exactly once, ever
#
# A record now outlives the run in which its job finished: Requirement 3.2 keeps
# a terminal record whose completion report has not been read so that
# check_report_handler can alert on it. Re-scoring such a record every run rolls
# the watermark back to the same point each time, resubmits the same journal
# range at a fresh per-job charge each time, and climbs consecutive_failures
# until the breaker disables the bucket with a reason claiming N consecutive
# failures for one job that failed once.
# ---------------------------------------------------------------------------


def _terminal_record(
    job_id: str,
    *,
    status: str,
    recovery_scored: bool,
    report_diagnosed: bool = False,
    consecutive_failures: int = 0,
) -> SubmissionRecord:
    del status  # the status comes from DescribeJob, not the record
    return SubmissionRecord(
        replication_config_id="my-bucket",
        job_id=job_id,
        manifest_key=f"manifests/my-bucket/{job_id}/manifest.json",
        submitted_at=_NOW - timedelta(hours=2),
        status=SubmissionStatus.SUBMITTED,
        source_bucket="my-bucket",
        watermark_low="2026-08-27T09:00:00.000000Z",
        watermark_high="2026-08-27T10:00:00.000000Z",
        consecutive_failures=consecutive_failures,
        report_diagnosed=report_diagnosed,
        recovery_scored=recovery_scored,
    )


class TestATerminalOutcomeIsScoredOnlyOnce:
    @staticmethod
    def _run(job_status: str, *, recovery_scored: bool):
        from src import orchestrator

        s3control = MagicMock()
        s3control.describe_job.return_value = {
            "Job": {
                "Status": job_status,
                "CreationTime": _NOW - timedelta(hours=2),
                "TerminationDate": _NOW - timedelta(hours=1),
                "ProgressSummary": {
                    "NumberOfTasksSucceeded": 0,
                    "NumberOfTasksFailed": 5,
                },
            }
        }
        record = _terminal_record(
            "job-failed", status=job_status, recovery_scored=recovery_scored,
        )
        writer = MagicMock()

        emitted: list = []
        with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
            check = orchestrator._describe_prior_jobs(
                _describe_ctx(s3control),
                writer,
                MagicMock(),
                {"job-failed": record},
                orchestrator._NullCompletionHooks(),
            )
        return check, emitted, writer

    @staticmethod
    def _readmits(emitted: list) -> list[dict]:
        return [
            entry for entry in emitted
            if isinstance(entry, dict)
            and entry.get("action") == "batch_job_failure_readmit"
        ]

    def test_an_unscored_failure_is_scored(self):
        """Non-vacuous: a job that has not been acted on must still be acted on."""
        check, emitted, _ = self._run("Failed", recovery_scored=False)

        assert len(check.outcomes) == 1
        assert check.any_check_failed is True
        assert check.any_check_ran is True
        assert check.failed_lows == ["2026-08-27T09:00:00.000000Z"]
        assert len(self._readmits(emitted)) == 1

    def test_an_already_scored_failure_is_not_scored_again(self):
        check, emitted, _ = self._run("Failed", recovery_scored=True)

        assert check.outcomes == []
        assert check.any_check_failed is False
        assert check.failed_lows == []
        assert self._readmits(emitted) == []

    def test_an_already_scored_job_does_not_reset_the_failure_counter_either(self):
        """It must not take plan_recovery's rule 3. An outcome already acted on is
        not evidence about this interval, so counting it as a check that ran would
        reset the counter on a run that learned nothing."""
        check, _, _ = self._run("Complete", recovery_scored=True)

        assert check.any_check_ran is False
        assert check.outcomes == []

    def test_an_already_scored_job_still_counts_as_terminal_for_pruning(self):
        """Its record is still settleable and must still be prunable, or nothing
        would ever remove it."""
        check, _, _ = self._run("Complete", recovery_scored=True)

        assert check.terminal_job_ids == ["job-failed"]

    def test_scoring_a_job_reports_it_rather_than_committing_the_flag(self):
        """The flag authorizes a rollback that is in memory until a resubmission
        makes it durable, so this loop reports the id and the caller commits it
        once that work has landed (Requirements 5.1, 5.2)."""
        check, _, writer = self._run("Failed", recovery_scored=False)

        assert check.newly_scored_job_ids == ["job-failed"]
        for call in writer.mark_report_diagnosed.call_args_list:
            assert call.kwargs.get("recovery_scored") is not True

    def test_an_already_scored_and_diagnosed_job_writes_nothing(self):
        """Neither flag changes, so the write is skipped: a conditional write per
        run per retained record, for nothing, is worth avoiding."""
        from src import orchestrator

        s3control = MagicMock()
        s3control.describe_job.return_value = {
            "Job": {
                "Status": "Complete",
                "CreationTime": _NOW - timedelta(hours=2),
                "TerminationDate": _NOW - timedelta(hours=1),
                "ProgressSummary": {
                    "NumberOfTasksSucceeded": 5, "NumberOfTasksFailed": 0,
                },
            }
        }
        record = _terminal_record(
            "job-settled", status="Complete",
            recovery_scored=True, report_diagnosed=True,
        )
        writer = MagicMock()

        with patch("src.orchestrator.observability.emit"):
            orchestrator._describe_prior_jobs(
                _describe_ctx(s3control), writer, MagicMock(),
                {"job-settled": record},
                orchestrator._NullCompletionHooks(),
            )

        writer.mark_report_diagnosed.assert_not_called()

    def test_an_unreadable_report_still_scores_and_still_defers_the_flag(self):
        """The case that makes re-scoring reachable at all: an unreadable report
        leaves report_diagnosed False, so the separate recovery_scored flag is
        what records that the outcome was acted on. It is still not written here —
        neither flag is, since diagnosis did not happen either."""
        from src import orchestrator
        from src.adapters import bops_report_reader

        s3control = MagicMock()
        s3control.describe_job.return_value = {
            "Job": {
                "Status": "Failed",
                "CreationTime": _NOW - timedelta(hours=2),
                "TerminationDate": _NOW - timedelta(hours=1),
                "ProgressSummary": {
                    "NumberOfTasksSucceeded": 1, "NumberOfTasksFailed": 1,
                },
            }
        }
        record = _terminal_record(
            "job-unreadable", status="Failed", recovery_scored=False,
        )
        writer = MagicMock()

        with (
            patch("src.orchestrator.observability.emit"),
            patch(
                "src.orchestrator.bops_report_reader.read_bops_completion_report",
                side_effect=bops_report_reader.CompletionReportMalformed("bad rows"),
            ),
        ):
            check = orchestrator._describe_prior_jobs(
                _describe_ctx(s3control), writer, MagicMock(),
                {"job-unreadable": record},
                orchestrator._NullCompletionHooks(),
            )

        assert len(check.outcomes) == 1  # scored this run
        assert check.newly_scored_job_ids == ["job-unreadable"]
        writer.mark_report_diagnosed.assert_not_called()


class TestTheSeedComesFromTheNewestRecords:
    """Every record carries the bucket-level counter as it stood at that
    submission, so the newest record is the current value and an older one is
    history. A maximum across the whole population would let a counter from
    before a successful job survive the reset that job earned.
    """

    @staticmethod
    def _record(job_id: str, hours_ago: int, consecutive_failures: int):
        return SubmissionRecord(
            replication_config_id="my-bucket",
            job_id=job_id,
            manifest_key=f"manifests/my-bucket/{job_id}/manifest.json",
            submitted_at=_NOW - timedelta(hours=hours_ago),
            status=SubmissionStatus.SUBMITTED,
            source_bucket="my-bucket",
            watermark_low="2026-08-27T09:00:00.000000Z",
            watermark_high="2026-08-27T10:00:00.000000Z",
            consecutive_failures=consecutive_failures,
        )

    def _plan(self, records, outcomes=(), threshold=4):
        return plan_recovery(
            records=records,
            outcomes=list(outcomes),
            bucket_name="my-bucket",
            threshold=threshold,
        )

    def test_a_stale_high_counter_does_not_survive_a_later_reset(self):
        """job-old recorded 3 failures; job-new recorded the 0 a later success
        earned. A maximum would seed 3 and disable the bucket on one more failure.
        """
        records = {
            "job-old": self._record("job-old", hours_ago=10, consecutive_failures=3),
            "job-new": self._record("job-new", hours_ago=1, consecutive_failures=0),
        }

        plan = self._plan(records, outcomes=[
            JobOutcome(
                config_id="my-bucket", job_id="job-new", status="Failed",
                watermark_low="2026-08-27T09:00:00.000000Z",
                watermark_high="2026-08-27T10:00:00.000000Z",
                consecutive_failures=0,
            )
        ])

        assert plan.consecutive_failures == 1
        assert plan.disable_reason is None

    def test_the_newest_counter_is_honoured_when_it_is_the_high_one(self):
        """Non-vacuous: the breaker must still trip. Newest, not lowest."""
        records = {
            "job-old": self._record("job-old", hours_ago=10, consecutive_failures=0),
            "job-new": self._record("job-new", hours_ago=1, consecutive_failures=3),
        }

        plan = self._plan(records, outcomes=[
            JobOutcome(
                config_id="my-bucket", job_id="job-new", status="Failed",
                watermark_low="2026-08-27T09:00:00.000000Z",
                watermark_high="2026-08-27T10:00:00.000000Z",
                consecutive_failures=3,
            )
        ])

        assert plan.consecutive_failures == 4
        assert plan.disable_reason is not None

    def test_records_sharing_a_timestamp_still_consolidate_by_maximum(self):
        """Several records written in one run share a submitted_at, and the
        maximum across that group is the value that run computed."""
        records = {
            "job-a": self._record("job-a", hours_ago=1, consecutive_failures=1),
            "job-b": self._record("job-b", hours_ago=1, consecutive_failures=3),
        }

        plan = self._plan(records)

        assert plan.consecutive_failures == 3  # rule 4 holds the seed

    def test_no_records_seeds_zero(self):
        assert self._plan({}).consecutive_failures == 0

    def test_a_naive_submitted_at_does_not_raise(self):
        records = {
            "job-naive": SubmissionRecord(
                replication_config_id="my-bucket",
                job_id="job-naive",
                manifest_key="manifests/my-bucket/naive/manifest.json",
                submitted_at=datetime(2020, 1, 1),  # noqa: DTZ001 — the point
                status=SubmissionStatus.SUBMITTED,
                source_bucket="my-bucket",
                consecutive_failures=2,
            ),
            "job-aware": self._record("job-aware", hours_ago=1, consecutive_failures=0),
        }

        plan = self._plan(records)

        # The aware record is newer, so it is the seed.
        assert plan.consecutive_failures == 0
