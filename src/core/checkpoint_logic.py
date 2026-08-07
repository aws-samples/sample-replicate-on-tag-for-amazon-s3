"""Checkpoint eligibility and advancement logic for the tag-based S3 replication backfill Solution.

Pure functions — no AWS dependencies.

The checkpoint is a ``record_timestamp`` **watermark** (a canonical UTC string,
see :mod:`src.core.watermark`), not a ``sequence_number``.  Amazon S3 documents
``sequence_number`` ordering only *per (bucket, key)*, so it cannot be used as a
single cross-key cursor; ``record_timestamp`` is globally comparable and is the
correct field for a per-bucket watermark.

Because the journal is only eventually consistent, a record can be *introduced*
to the journal with a ``record_timestamp`` at or below the current watermark
(it "arrived late").  To catch such records the Journal_Monitor re-scans a
configurable **lookback window** below the watermark each run.  A bounded
**processed-operation window** (``CheckpointState.processed_window``) remembers
which operations in that window were already submitted, so re-scanning never
re-submits an object — late arrivals are picked up exactly once and already
-processed operations are suppressed.

Requirements: 4.3, 4.6, 7.6, 7.7, 9.1, 9.3, 9.4, 10.2, 12.6
"""
from __future__ import annotations

from datetime import timedelta

from src.core.models import CheckpointState, LeaseStatus, ProcessedRef, TaggingOperation
from src.core.watermark import EPOCH_WATERMARK, subtract, to_watermark


def is_eligible(
    op: TaggingOperation,
    state: CheckpointState,
    lookback: timedelta,
    processed_ids: set[str] | None = None,
) -> bool:
    """Return ``True`` if the operation should be processed in the current run.

    An operation is **eligible** unless one of the following excludes it:

    1. **In-flight lease** (Req 9.4) — when ``state.lease`` is present and
       ``IN_FLIGHT``, operations whose watermark is at or below the lease's
       ``candidate_max_watermark`` are currently being submitted by this run (or
       a concurrent run) and are excluded to prevent duplicate submission.

    2. **At or below the watermark and outside the lookback window** — the
       operation was processed in a prior run and is too old to be a late
       arrival worth re-checking.

    3. **A re-scanned operation already submitted** — its watermark is inside
       the lookback window ``(watermark - lookback, watermark]`` but its
       ``logical_operation_id`` is already recorded in
       ``state.processed_window``, so it was submitted on an earlier run.

    Operations strictly above the watermark are always eligible (subject to the
    lease guard).  On the first run for a bucket the watermark is the epoch
    (empty string), so every operation is eligible.

    Args:
        op:            The candidate ``TaggingOperation`` to evaluate.
        state:         The current persisted ``CheckpointState`` for the bucket.
        lookback:      How far below the watermark to keep re-scanning for late
                       arrivals.  Must be non-negative.
        processed_ids: Precomputed set of every ``logical_operation_id`` in
                       ``state.processed_window``. Optional — when omitted, it
                       is rebuilt from ``state.processed_window`` on every
                       call, which is the correct behavior for a single
                       lookup but becomes O(window_size) per call, and
                       therefore O(n * window_size) when called once per
                       candidate operation from
                       :func:`~src.core.journal_dedup.select_eligible_operations`.
                       That caller passes a set built once up front instead,
                       since both ``n`` (candidate ops) and ``window_size``
                       (``processed_window`` entries) can independently reach
                       the tens or hundreds of thousands — see design.md's
                       ``processed_window`` growth note (a large tagging burst
                       within one lookback window transiently inflates the
                       window to one entry per operation). Without
                       precomputing this set once, that combination degrades
                       to effectively O(n^2), which has been observed in
                       practice to exceed the Lambda's 900s timeout entirely
                       rather than merely being slow.

    Returns:
        ``True`` if the operation is eligible for the current run.
    """
    op_wm = to_watermark(op.event_time)

    # Guard 1: in-flight lease — exclude operations being submitted now (Req 9.4).
    if (
        state.lease is not None
        and state.lease.status == LeaseStatus.IN_FLIGHT
        and op_wm <= state.lease.candidate_max_watermark
    ):
        return False

    watermark = state.last_processed_watermark

    # First run: no watermark yet, everything is eligible (Req 4.1).
    if watermark == EPOCH_WATERMARK:
        return True

    # Strictly newer than the watermark → always eligible.
    if op_wm > watermark:
        return True

    # At/below the watermark: only a late arrival inside the lookback window,
    # not already submitted, is eligible.
    window_start = subtract(watermark, lookback)
    if op_wm <= window_start:
        # Below the lookback window — already processed and too old to re-check.
        return False

    if processed_ids is None:
        processed_ids = {ref.logical_operation_id for ref in state.processed_window}
    if op.logical_operation_id in processed_ids:
        # Re-scanned but already submitted on an earlier run (Req 9.1).
        return False

    # A genuine late arrival inside the lookback window — process it.
    return True


def advance_checkpoint(
    state: CheckpointState,
    submitted_refs: list[ProcessedRef] | None,
    lookback: timedelta,
    candidate_max_watermark: str | None = None,
) -> CheckpointState:
    """Return the new ``CheckpointState`` after a job submission attempt.

    **Watermark advancement** (Requirements 4.3, 9.1):

    * When *submitted_refs* is non-empty (a job was successfully submitted),
      advance ``last_processed_watermark`` to the maximum of its current value,
      the highest watermark among *submitted_refs*, and
      *candidate_max_watermark*.  The watermark is therefore monotonically
      non-decreasing — a re-observed or late record can never move it backward.

      *candidate_max_watermark* is the high-water mark over **all** eligible
      operations of the interval, including those that matched no rule or were
      excluded by the Deleted_Version_Filter and therefore have no ref (see
      :func:`~src.core.journal_dedup.build_submitted_refs`).  Without it the
      cursor would stall below any non-matching record newer than the newest
      record that reached a manifest, and every subsequent run would re-read a
      growing journal window (retag-suppression Requirement 2.5, design D3).
      Those operations are still absent from the processed-operation window, so
      they stay eligible until the lookback window passes them
      (Requirement 2.6).

      Note the asymmetry with *submitted_refs*: an empty *submitted_refs* is
      read as "nothing was submitted" and takes the failure path below, so
      *candidate_max_watermark* is never applied on its own.  A caller that
      submitted a job must therefore pass a non-empty *submitted_refs* — see
      the invariant recorded at the ``build_submitted_refs`` call site in
      ``orchestrator._leased_manifest_and_submit``.

    * When *submitted_refs* is ``None`` or empty (job creation/submission
      failed), leave ``last_processed_watermark`` unchanged so all candidate
      operations remain eligible for reprocessing (Requirements 7.6, 7.7, 9.3,
      10.2, 12.6).

    **Processed-operation window** (avoids re-replication under lookback):

    On success, the submitted refs are merged into the window and the window is
    pruned to entries whose watermark is still within ``lookback`` of the new
    watermark (older entries can never be re-scanned, so remembering them is
    unnecessary).  This keeps the window bounded by the lookback duration.

    **Lease release** (Requirement 9.4):

    The in-flight lease is **always** cleared, on success and failure alike.

    Args:
        state:          The current ``CheckpointState`` for the bucket.
        submitted_refs: The operations included in a successfully submitted job
                        (each carries its ``logical_operation_id`` and
                        watermark), or ``None``/empty when nothing was submitted.
        lookback:       The lookback window used to bound the processed-operation
                        window.
        candidate_max_watermark:
                        The high-water mark over all eligible operations of the
                        interval, or ``None`` when the caller has none.  Used
                        only on the success path.

    Returns:
        A new ``CheckpointState`` with an updated-or-unchanged watermark, a
        pruned processed-operation window, and the lease cleared to ``None``.
    """
    if not submitted_refs:
        # Failure path: keep watermark and window, just release the lease.
        return CheckpointState(
            source_bucket=state.source_bucket,
            last_processed_watermark=state.last_processed_watermark,
            lease=None,
            processed_window=list(state.processed_window),
        )

    # Success path: advance the watermark monotonically.
    new_watermark = state.last_processed_watermark
    for ref in submitted_refs:
        if ref.watermark > new_watermark:
            new_watermark = ref.watermark
    if candidate_max_watermark is not None and candidate_max_watermark > new_watermark:
        new_watermark = candidate_max_watermark

    # Merge submitted refs into the window and prune to the lookback bound.
    window_start = subtract(new_watermark, lookback)
    merged = list(state.processed_window) + list(submitted_refs)
    pruned = _prune_window(merged, window_start)

    return CheckpointState(
        source_bucket=state.source_bucket,
        last_processed_watermark=new_watermark,
        lease=None,
        processed_window=pruned,
    )


def _prune_window(
    refs: list[ProcessedRef],
    window_start: str,
) -> list[ProcessedRef]:
    """Keep only refs strictly above *window_start*, deduped by logical id.

    Entries at or below *window_start* sit below the lookback re-scan window, so
    they can never be re-observed and need not be remembered.  Among entries
    sharing a ``logical_operation_id`` the one with the highest watermark is
    kept.  The result is sorted for deterministic serialization.
    """
    best: dict[str, ProcessedRef] = {}
    for ref in refs:
        if ref.watermark <= window_start:
            continue
        existing = best.get(ref.logical_operation_id)
        if existing is None or ref.watermark > existing.watermark:
            best[ref.logical_operation_id] = ref
    return sorted(best.values(), key=lambda r: (r.watermark, r.logical_operation_id))
