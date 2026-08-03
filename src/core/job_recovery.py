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

The watermark-rollback decision includes the rule that an empty
``watermark_low`` is skipped rather than treated as the epoch - a safeguard
against an accidental full-bucket rescan.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

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
    """

    config_id: str
    job_id: str
    status: str
    watermark_low: str
    watermark_high: str
    consecutive_failures: int


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

    1. Seed ``consecutive_failures`` from the maximum value across all
       submission records for the bucket.
    2. Increment by exactly one when any outcome was Failed or Cancelled.
    3. Reset to zero when at least one outcome was evaluated and none failed.
    4. Hold at the seeded value when no outcome could be evaluated (the
       ``outcomes`` sequence is empty, meaning every ``DescribeJob`` call
       raised a transport error and was skipped).

    Watermark rollback (Requirement 6.4):

    When failures occurred, the rollback target is the minimum of the
    ``watermark_low`` values from failed outcomes. Outcomes with an empty
    ``watermark_low`` are skipped rather than collapsing the rollback to the
    epoch (Requirement 6.4).

    Threshold check:

    When ``consecutive_failures >= threshold``, the plan includes a
    ``disable_reason`` string. The orchestrator uses this to disable the
    bucket and halt processing.

    Args:
        records:     The bucket's prior submission records, keyed by config_id.
                     Used to seed the consecutive-failure counter.
        outcomes:    Terminal job outcomes from the ``DescribeJob`` loop.
                     May be empty if all describe calls failed.
        bucket_name: The source bucket name, used in the disable reason message.
        threshold:   The ``max_batch_job_failures`` limit above which the bucket
                     is disabled.

    Returns:
        A :class:`RecoveryPlan` describing the recovery decision.

    Requirements: 6.1, 6.2, 6.3, 6.4, 6.5
    """
    # Rule 1: Seed from the maximum prior consecutive_failures.
    seeded = max(
        (rec.consecutive_failures for rec in records.values()),
        default=0,
    )

    # Determine whether any outcome was evaluated and whether any failed.
    any_check_ran = len(outcomes) > 0
    any_check_failed = any(
        o.status in ("Failed", "Cancelled") for o in outcomes
    )

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
    failed_lows = [
        o.watermark_low for o in outcomes
        if o.status in ("Failed", "Cancelled")
    ]
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
            f"disabled=false in the solution-config."
        )

    return RecoveryPlan(
        consecutive_failures=consecutive_failures,
        rollback_to=rollback_to,
        skipped_empty_lows=skipped_empty_lows,
        disable_reason=disable_reason,
    )
