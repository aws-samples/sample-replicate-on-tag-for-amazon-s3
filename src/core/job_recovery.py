"""Failed-job recovery planning for the tag-based S3 replication backfill Solution.

Pure functions - no AWS dependencies.

When a prior Batch Operations job is found in a terminal failure state
(Failed or Cancelled), the orchestrator must decide how to adjust the
consecutive-failure counter and whether to roll the checkpoint watermark back
so that the affected operations become eligible again.

The four consolidation rules (see :func:`plan_recovery`):

1. **Seed** from the maximum prior ``consecutive_failures`` across all
   submission records for the bucket.
2. **Increment** by exactly one when any check failed.
3. **Reset** to zero when at least one check ran and none failed.
4. **Hold** at the seeded value when no check could be evaluated (e.g. all
   ``DescribeJob`` calls raised transport errors).

"Failed" here means :func:`is_effective_failure`: an explicit ``Failed``/
``Cancelled`` job status, OR a ``Complete`` job where every task failed
(``NumberOfTasksSucceeded == 0`` and ``NumberOfTasksFailed > 0``) — the
signature of a role-permission gap (e.g. the Batch Operations job role missing
``s3:InitiateReplication``) that lets the job run to completion without
replicating anything. ``Job.Status`` alone never surfaces that case.

The watermark-rollback decision includes the rule that an empty
``watermark_low`` is skipped rather than treated as the epoch - a safeguard
against an accidental full-bucket rescan.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from src.core.models import SubmissionRecord


@dataclass(frozen=True)
class JobOutcome:
    """Terminal outcome of a single prior Batch Operations job.

    Built by the orchestrator's ``DescribeJob`` loop and passed into
    :func:`plan_recovery` as the sequence of observations.

    Attributes:
        config_id:            Replication config that produced this job.
        job_id:               The S3 Batch Operations job identifier.
        status:               Terminal status string (e.g. "Complete", "Failed",
                              "Cancelled").
        watermark_low:        The checkpoint watermark at the time of submission.
                              May be empty if the record pre-dates the field.
        watermark_high:       The candidate HWM submitted with this job.
        consecutive_failures: The failure counter stored in the submission record.
        tasks_succeeded:      ``Job.ProgressSummary.NumberOfTasksSucceeded``, or
                              ``None`` when the DescribeJob response carried no
                              ``ProgressSummary`` (older/partial responses).
        tasks_failed:         ``Job.ProgressSummary.NumberOfTasksFailed``, or
                              ``None`` under the same condition.
    """

    config_id: str
    job_id: str
    status: str
    watermark_low: str
    watermark_high: str
    consecutive_failures: int
    tasks_succeeded: int | None = None
    tasks_failed: int | None = None


@dataclass(frozen=True)
class RecoveryPlan:
    """The pure decision output of :func:`plan_recovery`.

    The orchestrator applies this plan by mutating state and calling callbacks;
    the plan itself performs no side effects.

    Attributes:
        consecutive_failures: Updated failure counter after applying the
                              consolidation rules.
        rollback_to:          The watermark to roll back to, or ``None`` if no
                              rollback is needed (either no failures occurred or
                              all failed records had empty watermark_lows).
        skipped_empty_lows:   Count of failed outcomes whose ``watermark_low``
                              was empty and therefore excluded from rollback
                              consideration.
        disable_reason:       When the failure counter meets or exceeds the
                              threshold, this carries the human-readable reason.
                              ``None`` means the bucket stays enabled.
    """

    consecutive_failures: int
    rollback_to: str | None
    skipped_empty_lows: int
    disable_reason: str | None


# The three S3 Batch Operations job statuses from which a job never transitions
# again. Everything else — New, Preparing, Ready, Suspended, Active, Paused,
# Completing, Cancelling, Failing — means the job is still in flight.
#
# Defined here, in the pure core, because three separate consumers have to agree
# on it: the completion-merge gate, the report-missing checker, and the in-flight
# submission guard. They previously each carried their own literal tuple.
TERMINAL_JOB_STATUSES = frozenset({"Complete", "Failed", "Cancelled"})


def is_terminal_status(status: str | None) -> bool:
    """True iff *status* is a status a Batch Operations job never leaves."""
    return status in TERMINAL_JOB_STATUSES


def _is_all_tasks_failed_complete(outcome: JobOutcome) -> bool:
    """True iff *outcome* is a ``Complete`` job where every task failed.

    A job can reach ``Status: Complete`` while every one of its tasks failed
    (e.g. every task rejected with ``InitiateReplicationNotPermitted`` because
    the Batch Operations job role lacks that permission) — nothing about the object
    was actually replicated, but ``Job.Status`` alone never surfaces this,
    since ``Complete`` is also the status of a fully successful job.

    Deliberately narrow: requires ``tasks_succeeded == 0`` (not merely
    ``tasks_failed > 0``), because a job with any succeeded task is by
    definition not a job where every task failed. A large
    ``NumberOfTasksFailed`` alongside a nonzero ``NumberOfTasksSucceeded`` is a
    partially failed job, which recovery handles through the ordinary
    completion-report path rather than by flagging the whole job.

    An earlier version of this docstring justified the narrowness differently,
    claiming ``NumberOfTasksFailed`` was an unreliable signal at large object
    counts and citing a production job that had "replicated ~100,000 objects
    successfully" while reporting ``NumberOfTasksSucceeded: 1`` alongside
    ``NumberOfTasksFailed: 100001``. That claim was wrong: the two figures had
    been transposed when they were recorded. Job
    ``0f65a1b7-9b4c-4124-a1ad-06ea77d7224f`` in ``us-west-2`` reports
    ``TotalNumberOfTasks: 100002, NumberOfTasksSucceeded: 100001,
    NumberOfTasksFailed: 1``, and its completion report parses to exactly
    100,002 rows. The counts are consistent and trustworthy. Nothing in this
    predicate's behavior changes; only its stated reason does.

    Returns ``False`` when the DescribeJob response carried no
    ``ProgressSummary`` (``tasks_succeeded``/``tasks_failed`` are ``None``),
    rather than treating an absent field as either verdict.
    """
    if outcome.status != "Complete":
        return False
    if outcome.tasks_succeeded is None or outcome.tasks_failed is None:
        return False
    return outcome.tasks_succeeded == 0 and outcome.tasks_failed > 0


def is_effective_failure(outcome: JobOutcome) -> bool:
    """True iff *outcome* should be treated as a job failure for recovery
    purposes — either an explicit terminal failure status, or a
    ``Complete`` job where every task failed (see
    :func:`_is_all_tasks_failed_complete`).

    The single place this predicate is defined, so :func:`plan_recovery`'s
    failure-counting and rollback-target computation, and the orchestrator's
    own readmission-audit gate, all agree on what counts as a failure.
    """
    return outcome.status in ("Failed", "Cancelled") or _is_all_tasks_failed_complete(outcome)


def _submitted_at_key(record: SubmissionRecord) -> datetime:
    """``record.submitted_at``, made comparable across records.

    A hand-edited state object can carry a naive value, and comparing naive
    against aware datetimes raises. UTC is assumed rather than raising, because
    the alternative is an exception in the arithmetic that decides whether a
    bucket keeps processing.
    """
    submitted_at = record.submitted_at
    if submitted_at.tzinfo is None:
        return submitted_at.replace(tzinfo=UTC)
    return submitted_at


def _seed_consecutive_failures(records: Mapping[str, SubmissionRecord]) -> int:
    """The bucket's stored consecutive-failure count, from its newest records.

    Zero when there are no records, which is the first run for the bucket and also
    the state a disabled-then-re-enabled bucket is deliberately left in. Both
    reductions carry a ``default`` rather than relying on a truthiness check on
    *records*, so a caller passing a stand-in mapping that is truthy but yields
    nothing gets 0 instead of a ``ValueError``.
    """
    latest = max(
        (_submitted_at_key(rec) for rec in records.values()),
        default=None,
    )
    if latest is None:
        return 0
    return max(
        (
            rec.consecutive_failures
            for rec in records.values()
            if _submitted_at_key(rec) == latest
        ),
        default=0,
    )


def plan_recovery(
    records: Mapping[str, SubmissionRecord],
    outcomes: Sequence[JobOutcome],
    bucket_name: str,
    threshold: int,
) -> RecoveryPlan:
    """Compute the recovery plan for a bucket's prior job outcomes.

    This is a pure decision function: it inspects the outcomes, applies the
    four consolidation rules, determines whether a watermark rollback is
    warranted, and checks whether the failure threshold has been breached.

    The caller (``_apply_recovery_plan`` in ``src/orchestrator.py``) is
    responsible for applying the plan's effects: mutating state, emitting
    audit logs, and invoking the disable callback.

    Consolidation rules (Requirement 6.1):

    1. Seed ``consecutive_failures`` from the most recently submitted records —
       the maximum across those sharing the latest ``submitted_at``.
    2. Increment by exactly one when any outcome was an effective failure
       (see :func:`is_effective_failure` — Failed/Cancelled, or Complete
       with every task failed).
    3. Reset to zero when at least one outcome was evaluated and none failed.
    4. Hold at the seeded value when no outcome could be evaluated (the
       ``outcomes`` sequence is empty, because every ``DescribeJob`` call either
       raised a transport error or reported a job that is still in flight).

    On rule 3 versus rule 4 for a job that has not finished: the caller does not
    put a non-terminal job in *outcomes* at all, so such a job takes rule 4 and
    holds the counter. It must not take rule 3. A running job is not evidence
    that anything succeeded, and scoring it as a passing check would let a bucket
    whose jobs keep failing reset its own counter every time a fresh job happened
    to be in flight when the counter was read, so the circuit breaker would never
    trip.

    On rule 1 taking the latest records rather than the maximum across all of
    them: every record written carries the bucket-level counter as it stood at
    that submission, so the newest record is the current value and an older one is
    history. Taking the maximum across the whole population was correct when a
    bucket's records were the several per-rule records of a single run, which is
    what this function was written against. Under per-job keying they span time and
    a record can outlive many runs — Requirement 3.2 keeps a terminal record whose
    report has not been read — so a maximum would let a counter of 3 from before a
    successful job survive the reset that job earned and push a single later
    failure to 4, disabling the bucket at the default ``MaxBatchJobFailures``.

    The tie on ``submitted_at`` is what keeps the original consolidation: several
    records written in the same run share a timestamp, and the maximum across that
    group is the value that run computed.

    Several outcomes in one run is the ordinary case, since a bucket may have up
    to ``MaxConcurrentJobsPerBucket`` jobs outstanding and several can reach
    terminal between two runs. Rule 2 increments by one **per failing run**, not
    per failing job: the counter measures consecutive failing intervals, which is
    what ``MaxBatchJobFailures`` reads as. Incrementing per job would trip the
    breaker N times faster at a concurrency of N for one underlying fault — the
    same misconfigured role failing three jobs would look like three consecutive
    failures rather than one.

    Watermark rollback (Requirement 6.4):

    When failures occurred, the rollback target is the minimum of the
    ``watermark_low`` values from failed outcomes, so the rollback covers every
    failure in the run rather than only the last one examined. Outcomes with an
    empty ``watermark_low`` are skipped rather than collapsing the rollback to the
    epoch (Requirement 6.4).

    A rollback target older than the oldest ``processed_window`` entry can
    resubmit objects a still-outstanding job already covers. ``processed_window``
    is pruned to the ``JournalLookbackSeconds`` bound, so dedupe cannot suppress
    a rollback reaching further back than that. This is a property of rollback
    plus a bounded dedupe window rather than of concurrency, and the duplicate is
    harmless: the task re-initiates replication, the version returns to
    ``COMPLETED``, and the task succeeds. Documented rather than worked around
    (Requirement 4.4).

    Threshold check:

    When ``consecutive_failures >= threshold``, the plan includes a
    ``disable_reason`` string. The orchestrator uses this to disable the
    bucket and halt processing.

    Args:
        records:     The bucket's prior submission records, keyed by ``job_id``.
                     Used to seed the consecutive-failure counter. The key is not
                     read — only ``consecutive_failures`` on each value is — so
                     the keying change is transparent here.
        outcomes:    Terminal job outcomes from the ``DescribeJob`` loop.
                     May be empty if all describe calls failed.
        bucket_name: The source bucket name, used in the disable reason message.
        threshold:   The ``max_batch_job_failures`` limit above which the bucket
                     is disabled.

    Returns:
        A :class:`RecoveryPlan` describing the recovery decision.

    Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
    """
    # Rule 1: Seed from the most recently submitted records. See the docstring
    # for why this is not the maximum across every record.
    seeded = _seed_consecutive_failures(records)

    # Determine whether any outcome was evaluated and whether any failed.
    any_check_ran = len(outcomes) > 0
    any_check_failed = any(is_effective_failure(o) for o in outcomes)

    # Rules 2-4: consolidate the failure counter.
    if any_check_failed:
        # Rule 2: increment by exactly one.
        consecutive_failures = seeded + 1
    elif any_check_ran:
        # Rule 3: at least one check ran, none failed - reset.
        consecutive_failures = 0
    else:
        # Rule 4: no check could be evaluated - hold at seeded.
        consecutive_failures = seeded

    # Watermark rollback decision.
    failed_lows = [o.watermark_low for o in outcomes if is_effective_failure(o)]
    usable_lows = [low for low in failed_lows if low]
    skipped_empty_lows = len(failed_lows) - len(usable_lows)

    rollback_to: str | None = None
    if usable_lows:
        rollback_to = min(usable_lows)

    # Threshold check.
    disable_reason: str | None = None
    if consecutive_failures >= threshold:
        disable_reason = (
            f"S3 Batch Operations job for bucket {bucket_name!r} has failed "
            f"{consecutive_failures} consecutive time(s) (threshold: "
            f"{threshold}). Bucket disabled to prevent runaway "
            f"per-job costs. Investigate and re-enable by setting "
            f'"disabled": false in the bucket\'s state object.'
        )

    return RecoveryPlan(
        consecutive_failures=consecutive_failures,
        rollback_to=rollback_to,
        skipped_empty_lows=skipped_empty_lows,
        disable_reason=disable_reason,
    )
