"""Journal deduplication and eligibility for the tag-based S3 replication backfill Solution.

Pure function — no AWS dependencies.

Pipeline applied to the raw journal records read for one interval:

1. **Validity** — drop records missing ``object_key`` or ``resulting_tag_set``
   (Requirement 4.5); they are reported as :class:`SkippedRecord`.
2. **Deduplication** — collapse records sharing a ``logical_operation_id``
   (at-least-once journal delivery) to one, keeping the highest
   ``sequence_number``.  This is the *only* use of ``sequence_number``, and it
   is its documented use: ordering records of the **same** ``(bucket, key)``
   (Requirement 9.2).
3. **Eligibility** — keep only operations the checkpoint deems unprocessed,
   evaluated against the ``record_timestamp`` watermark, the lookback window,
   and the processed-operation window (see
   :func:`src.core.checkpoint_logic.is_eligible`).

Requirements: 4.2, 4.5, 9.1, 9.2
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from src.core.checkpoint_logic import is_eligible
from src.core.models import (
    CheckpointState,
    ProcessedRef,
    SequenceNumber,
    TaggingOperation,
)
from src.core.watermark import to_watermark


@dataclass
class SkippedRecord:
    """A journal record dropped during deduplication, with the reason.

    Produced for records that are missing ``object_key`` or
    ``resulting_tag_set`` (Requirement 4.5).  Records filtered out by the
    checkpoint/eligibility step are silently excluded and are *not* reported
    here — that is expected behavior (Requirement 9.1), not a skip condition.
    """

    sequence_number: str
    source_bucket: str | None
    object_key: str | None
    reason: str


def select_eligible_operations(
    ops: list[TaggingOperation],
    state: CheckpointState,
    lookback: timedelta,
) -> tuple[
    list[TaggingOperation],
    list[SkippedRecord],
    SequenceNumber | None,
]:
    """Deduplicate journal records and return those eligible for the current run.

    Args:
        ops:      Raw ``TaggingOperation`` records read from the journal for one
                  interval, possibly containing malformed entries and duplicate
                  deliveries.
        state:    The persisted ``CheckpointState`` for the bucket (watermark,
                  optional in-flight lease, processed-operation window).
        lookback: The lookback window below the watermark used to catch late
                  arrivals.

    Returns:
        A three-element tuple:

        * **eligible** — one ``TaggingOperation`` per distinct
          ``logical_operation_id`` that the checkpoint deems unprocessed, ready
          to forward to the Rule_Matcher.
        * **skipped** — ``SkippedRecord`` entries for records dropped due to a
          missing field (4.5).  Eligibility-filtered records are *not* included.
        * **candidate_max_watermark** — the maximum canonical watermark among
          *eligible*; ``None`` when *eligible* is empty.  Used as the lease
          high-water mark and to advance the checkpoint on success.

        The processed-operation refs are *not* returned here: they are built
        from the written manifest by :func:`build_submitted_refs` once a job has
        been submitted successfully, so only operations that actually reached a
        manifest are recorded as processed (Requirement 2.1).
    """
    skipped: list[SkippedRecord] = []

    # ------------------------------------------------------------------
    # Step 1: validity — drop records missing required fields
    # ------------------------------------------------------------------
    valid: list[TaggingOperation] = []
    for op in ops:
        if not op.object_key:
            skipped.append(
                SkippedRecord(
                    sequence_number=op.sequence_number,
                    source_bucket=op.source_bucket,
                    object_key=op.object_key if op.object_key else None,
                    reason="missing object_key",
                )
            )
            continue
        if not op.resulting_tag_set:  # None or empty dict
            skipped.append(
                SkippedRecord(
                    sequence_number=op.sequence_number,
                    source_bucket=op.source_bucket,
                    object_key=op.object_key,
                    reason="missing or empty resulting_tag_set",
                )
            )
            continue
        valid.append(op)

    # ------------------------------------------------------------------
    # Step 2: deduplication by logical_operation_id
    #
    # Among records sharing the same logical_operation_id (at-least-once
    # journal delivery for the same bucket+key+version), keep the one with the
    # highest sequence_number.  This is sequence_number's documented use —
    # ordering records of the *same* key — not a cross-key cursor.
    # ------------------------------------------------------------------
    best: dict[str, TaggingOperation] = {}
    for op in valid:
        lid = op.logical_operation_id
        if lid not in best or op.sequence_number > best[lid].sequence_number:
            best[lid] = op
    deduped = list(best.values())

    # ------------------------------------------------------------------
    # Step 3: eligibility — exclude operations already processed (by the
    # record_timestamp watermark, the lookback window, and the processed
    # -operation window) or held by an in-flight lease.
    #
    # processed_ids is built once here (O(window_size)), not once per
    # candidate operation inside is_eligible (which would be
    # O(len(deduped) * window_size)) — see is_eligible's own docstring for
    # why that combination has been observed to exceed the Lambda's 900s
    # timeout entirely when a tagging burst has transiently inflated
    # processed_window to a large size.
    # ------------------------------------------------------------------
    processed_ids = {ref.logical_operation_id for ref in state.processed_window}
    eligible = [
        op for op in deduped if is_eligible(op, state, lookback, processed_ids)
    ]

    # ------------------------------------------------------------------
    # Step 4: high-water mark (canonical watermarks)
    # ------------------------------------------------------------------
    candidate_max_watermark: SequenceNumber | None = None
    if eligible:
        candidate_max_watermark = max(
            to_watermark(op.event_time) for op in eligible
        )

    return eligible, skipped, candidate_max_watermark


def build_submitted_refs(
    ops: list[TaggingOperation],
    kept_triples: set[tuple[str, str, str | None]],
) -> list[ProcessedRef]:
    """Build the processed-operation refs for a successfully submitted job.

    Pure counterpart of the manifest-writing path: an operation is recorded as
    processed only when its object actually reached the written manifest of the
    submitted job.  *kept_triples* is the set of
    ``(source_bucket, object_key, version_id)`` triples of the manifest entries
    that survived the Deleted_Version_Filter, which joins back to
    ``TaggingOperation`` on ``(source_bucket, object_key, operation_version)``.

    Operations that matched no rule, or whose version was excluded by the
    Deleted_Version_Filter, are absent from *kept_triples* and therefore are not
    recorded — they stay eligible on the next interval (Requirements 2.1, 2.7).

    WHERE several operations share one kept triple (two matching tag states on
    the same object version inside one interval), a ref is returned for **every**
    one of them (Requirement 2.4).  The manifest carries a single entry for the
    triple and the job replicates against the object's live tags, so the later
    state is what was replicated; re-processing an earlier one would submit and
    bill for the same object to no effect.

    The watermark is *not* derived from this list — it continues to come from
    ``select_eligible_operations`` over all eligible operations, so the cursor
    still advances past non-matching records (Requirement 2.5).

    Args:
        ops:          The deduplicated, eligible operations forwarded to the
                      Rule_Matcher for this interval.
        kept_triples: Triples of the manifest entries actually written.

    Returns:
        One :class:`~src.core.models.ProcessedRef` per operation whose triple
        appears in *kept_triples*, in the order of *ops*.

    Requirements: 2.1, 2.3, 2.4, 2.7
    """
    return [
        ProcessedRef(
            logical_operation_id=op.logical_operation_id,
            watermark=to_watermark(op.event_time),
        )
        for op in ops
        if (op.source_bucket, op.object_key, op.operation_version) in kept_triples
    ]
