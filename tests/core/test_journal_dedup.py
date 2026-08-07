"""Unit tests for src/core/journal_dedup.py.

Covers Requirements 4.2, 4.5, 9.1, 9.2 under the ``record_timestamp`` watermark
model:
- Field extraction from representative records (4.2)
- Records missing key or resulting_tag_set skipped and reported (4.5)
- Duplicate deliveries sharing a logical_operation_id collapse to one, keeping
  the highest sequence_number (per-key tie-break, 9.2)
- Eligibility filtering against the watermark / lookback window / processed
  window (9.1)
- Candidate max watermark
- Retag eligibility: a second tagging event on the same object version is a
  distinct operation (Requirements 4.1, 4.2)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.core.journal_dedup import (
    SkippedRecord,
    build_submitted_refs,
    select_eligible_operations,
)
from src.core.models import CheckpointState, ProcessedRef, TaggingOperation
from src.core.watermark import to_watermark

_EPOCH = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
_LOOKBACK = timedelta(minutes=10)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _at(minute: int) -> datetime:
    return _EPOCH + timedelta(minutes=minute)


def wm(minute: int) -> str:
    return to_watermark(_at(minute))


def make_op(
    object_key: str = "path/obj.txt",
    source_bucket: str = "bucket-a",
    resulting_tag_set: object = None,
    sequence_number: str = "seq-001",
    operation_version: str | None = None,
    minute: int = 1,
) -> TaggingOperation:
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key=object_key,
        resulting_tag_set=(
            resulting_tag_set if resulting_tag_set is not None else {"env": "prod"}
        ),
        sequence_number=sequence_number,
        operation="PutObjectTagging",
        event_time=_at(minute),
        operation_version=operation_version,
    )


def epoch_state(source_bucket: str = "bucket-a") -> CheckpointState:
    """A first-run state: epoch watermark, no lease, empty window."""
    return CheckpointState(
        source_bucket=source_bucket,
        last_processed_watermark="",
        lease=None,
    )


def state_at(
    watermark: str,
    processed_window: list[ProcessedRef] | None = None,
    source_bucket: str = "bucket-a",
) -> CheckpointState:
    return CheckpointState(
        source_bucket=source_bucket,
        last_processed_watermark=watermark,
        lease=None,
        processed_window=processed_window or [],
    )


# ---------------------------------------------------------------------------
# Field extraction (Req 4.2)
# ---------------------------------------------------------------------------


class TestFieldExtraction:
    def test_valid_op_passes_through(self):
        ops = [make_op(object_key="my/key.txt", resulting_tag_set={"k": "v"})]
        eligible, skipped, _ = select_eligible_operations(ops, epoch_state(), _LOOKBACK)
        assert len(eligible) == 1
        assert skipped == []
        assert eligible[0].object_key == "my/key.txt"
        assert eligible[0].resulting_tag_set == {"k": "v"}

    def test_empty_input_returns_empty(self):
        eligible, skipped, hwm = select_eligible_operations([], epoch_state(), _LOOKBACK)
        assert eligible == []
        assert skipped == []
        assert hwm is None

    def test_single_valid_op_hwm_equals_its_watermark(self):
        ops = [make_op(minute=5)]
        _, _, hwm = select_eligible_operations(ops, epoch_state(), _LOOKBACK)
        assert hwm == wm(5)


# ---------------------------------------------------------------------------
# Skipped records (Req 4.5)
# ---------------------------------------------------------------------------


class TestSkippedRecords:
    def test_empty_object_key_skipped(self):
        ops = [make_op(object_key="")]
        eligible, skipped, _ = select_eligible_operations(ops, epoch_state(), _LOOKBACK)
        assert eligible == []
        assert len(skipped) == 1
        assert isinstance(skipped[0], SkippedRecord)
        assert "object_key" in skipped[0].reason

    def test_none_resulting_tag_set_skipped(self):
        op = TaggingOperation(
            source_bucket="b",
            object_key="my/key.txt",
            resulting_tag_set=None,  # type: ignore[arg-type]
            sequence_number="seq-001",
            operation="PutObjectTagging",
            event_time=_at(1),
        )
        eligible, skipped, _ = select_eligible_operations([op], epoch_state("b"), _LOOKBACK)
        assert eligible == []
        assert len(skipped) == 1
        assert "tag_set" in skipped[0].reason

    def test_empty_resulting_tag_set_skipped(self):
        ops = [make_op(resulting_tag_set={})]
        eligible, skipped, _ = select_eligible_operations(ops, epoch_state(), _LOOKBACK)
        assert eligible == []
        assert len(skipped) == 1

    def test_valid_records_processed_alongside_invalid(self):
        good = make_op(object_key="good/path", minute=2)
        bad = make_op(object_key="", minute=3)
        eligible, skipped, _ = select_eligible_operations([good, bad], epoch_state(), _LOOKBACK)
        assert len(eligible) == 1
        assert eligible[0].object_key == "good/path"
        assert len(skipped) == 1


# ---------------------------------------------------------------------------
# Deduplication by logical_operation_id (Req 9.2)
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_duplicate_operation_version_collapses_to_one(self):
        op1 = make_op(sequence_number="seq-001", operation_version="v1")
        op2 = make_op(sequence_number="seq-002", operation_version="v1")
        eligible, _, _ = select_eligible_operations([op1, op2], epoch_state(), _LOOKBACK)
        assert len(eligible) == 1

    def test_highest_sequence_number_wins_in_dedup(self):
        op1 = make_op(sequence_number="seq-001", operation_version="v1")
        op2 = make_op(sequence_number="seq-002", operation_version="v1")
        eligible, _, _ = select_eligible_operations([op1, op2], epoch_state(), _LOOKBACK)
        assert eligible[0].sequence_number == "seq-002"

    def test_different_operation_versions_kept_separately(self):
        op1 = make_op(sequence_number="seq-001", operation_version="v1")
        op2 = make_op(sequence_number="seq-002", operation_version="v2")
        eligible, _, _ = select_eligible_operations([op1, op2], epoch_state(), _LOOKBACK)
        assert len(eligible) == 2

    def test_null_version_same_tag_set_collapses(self):
        # Renamed by retag-suppression task 4: there is no fallback branch any
        # more — the tag set is always part of the identity. The collapse
        # behaviour asserted here is unchanged.
        tags = {"k": "v"}
        op1 = make_op(sequence_number="seq-001", resulting_tag_set=tags, operation_version=None)
        op2 = make_op(sequence_number="seq-002", resulting_tag_set=tags, operation_version=None)
        eligible, _, _ = select_eligible_operations([op1, op2], epoch_state(), _LOOKBACK)
        assert len(eligible) == 1
        assert eligible[0].sequence_number == "seq-002"


# ---------------------------------------------------------------------------
# Eligibility filtering against the watermark (Req 9.1)
# ---------------------------------------------------------------------------


class TestEligibilityFiltering:
    def test_ops_below_watermark_outside_lookback_excluded(self):
        ops = [
            make_op(object_key="a", minute=20, operation_version="a"),  # below window
            make_op(object_key="b", minute=60, operation_version="b"),  # above watermark
        ]
        state = state_at(wm(50))  # window (40, 50]
        eligible, skipped, _ = select_eligible_operations(ops, state, _LOOKBACK)
        keys = {op.object_key for op in eligible}
        assert keys == {"b"}
        # Eligibility-filtered records are NOT reported as skipped.
        assert skipped == []

    def test_late_arrival_in_lookback_window_is_eligible(self):
        ops = [make_op(object_key="late", minute=45, operation_version="late")]
        state = state_at(wm(50))  # window (40, 50]
        eligible, _, _ = select_eligible_operations(ops, state, _LOOKBACK)
        assert len(eligible) == 1

    def test_processed_window_suppresses_already_submitted_op(self):
        op = make_op(object_key="seen", minute=45, operation_version="seen")
        state = state_at(
            wm(50),
            processed_window=[
                ProcessedRef(logical_operation_id=op.logical_operation_id, watermark=wm(45))
            ],
        )
        eligible, _, _ = select_eligible_operations([op], state, _LOOKBACK)
        assert eligible == []

    def test_epoch_state_includes_all_valid_ops(self):
        ops = [
            make_op(object_key="a", minute=1, operation_version="a"),
            make_op(object_key="b", minute=2, operation_version="b"),
        ]
        eligible, _, _ = select_eligible_operations(ops, epoch_state(), _LOOKBACK)
        assert len(eligible) == 2

    def test_all_ops_filtered_returns_empty_with_none_hwm(self):
        ops = [make_op(minute=10, operation_version="old")]
        state = state_at(wm(50))  # op at 10 is far below window start 40
        eligible, _, hwm = select_eligible_operations(ops, state, _LOOKBACK)
        assert eligible == []
        assert hwm is None


# ---------------------------------------------------------------------------
# Loss-table rows: a second tagging event on the same object version is a
# second operation (Requirements 4.1, 4.2)
#
# Each fixture places the *first* event's logical_operation_id in
# processed_window with a watermark inside the lookback window, and the second
# event inside the same window, so both preconditions for suppression hold and
# the assertion is not vacuous.
# ---------------------------------------------------------------------------
class TestRetagEligibility:
    def test_row2_added_matching_tag_on_same_version_is_eligible(self):
        """Tag {project:x} then {project:x, r:yes} on one version: second is eligible.

        Row 2 of the loss table — a tooling loop that writes tags in two calls.
        Under the old identity both events shared one id and the second was
        suppressed, so the object was never replicated.

        Validates: Requirements 4.1
        """
        first = make_op(
            object_key="k", operation_version="v1", minute=45,
            resulting_tag_set={"project": "x"},
        )
        second = make_op(
            object_key="k", operation_version="v1", minute=46,
            resulting_tag_set={"project": "x", "replicate": "yes"},
        )
        state = state_at(  # window (40, 50]
            wm(50),
            processed_window=[
                ProcessedRef(
                    logical_operation_id=first.logical_operation_id,
                    watermark=wm(45),
                )
            ],
        )

        eligible, _, _ = select_eligible_operations([second], state, _LOOKBACK)

        assert [op.logical_operation_id for op in eligible] == [
            second.logical_operation_id
        ]
        # The first event really is suppressed, so the fixture exercises the
        # processed_window path rather than passing for an unrelated reason.
        assert select_eligible_operations([first], state, _LOOKBACK)[0] == []

    def test_row3_retag_for_second_destination_is_eligible(self):
        """Tag {r:yes} (dest A) then {r2:yes} (dest B): second is eligible.

        Row 3 of the loss table — the case where a destination silently never
        receives the object.

        Validates: Requirements 4.1
        """
        first = make_op(
            object_key="k", operation_version="v1", minute=45,
            resulting_tag_set={"replicate": "yes"},
        )
        second = make_op(
            object_key="k", operation_version="v1", minute=46,
            resulting_tag_set={"replicate2": "yes"},
        )
        state = state_at(
            wm(50),
            processed_window=[
                ProcessedRef(
                    logical_operation_id=first.logical_operation_id,
                    watermark=wm(45),
                )
            ],
        )

        eligible, _, _ = select_eligible_operations([second], state, _LOOKBACK)

        assert [op.logical_operation_id for op in eligible] == [
            second.logical_operation_id
        ]
        assert select_eligible_operations([first], state, _LOOKBACK)[0] == []

    def test_row1_identical_tag_set_same_version_collapses_to_one(self):
        """Two deliveries of one event still collapse to a single operation.

        Row 1 of the loss table: making the identity event-scoped must not
        trade duplicate suppression for event fidelity.

        Validates: Requirements 4.2
        """
        tags = {"replicate": "yes"}
        first = make_op(
            object_key="k", operation_version="v1", minute=45,
            resulting_tag_set=dict(tags), sequence_number="seq-001",
        )
        duplicate = make_op(
            object_key="k", operation_version="v1", minute=45,
            resulting_tag_set=dict(tags), sequence_number="seq-002",
        )

        eligible, skipped, _ = select_eligible_operations(
            [first, duplicate], epoch_state(), _LOOKBACK
        )

        assert len(eligible) == 1
        assert skipped == []


# ---------------------------------------------------------------------------
# Candidate high-water mark
# ---------------------------------------------------------------------------


class TestCandidateHwm:
    def test_candidate_max_watermark_is_max_among_eligible(self):
        ops = [
            make_op(object_key="a", minute=10, operation_version="a"),
            make_op(object_key="b", minute=50, operation_version="b"),
            make_op(object_key="c", minute=30, operation_version="c"),
        ]
        _, _, hwm = select_eligible_operations(ops, epoch_state(), _LOOKBACK)
        assert hwm == wm(50)

    def test_hwm_uses_max_after_dedup(self):
        # op1/op2 share a logical id (kept: higher sequence); op3 distinct.
        op1 = make_op(object_key="key1", minute=90, operation_version="v1", sequence_number="seq-090")
        op2 = make_op(object_key="key1", minute=90, operation_version="v1", sequence_number="seq-080")
        op3 = make_op(object_key="key2", minute=50, operation_version="v2")
        eligible, _, hwm = select_eligible_operations([op1, op2, op3], epoch_state(), _LOOKBACK)
        assert len(eligible) == 2
        assert hwm == wm(90)


# ---------------------------------------------------------------------------
# Performance regression: large processed_window x large candidate-op count
# must not degrade to O(n * window_size).
#
# Reproduces the real production incident: a 100,000-object scale-test burst
# inflated processed_window to ~100,000 entries (documented in
# .kiro/specs/.complete/code-review-remediation/verification-notes.md's
# "processed_window growth" section). A subsequent run reading ~100,000
# candidate operations against that window caused every real Lambda
# invocation to hang for the full 900s timeout — is_eligible was rebuilding
# the processed_ids set from scratch on every call instead of once per
# select_eligible_operations call.
# ---------------------------------------------------------------------------


class TestLargeProcessedWindowPerformance:
    def test_large_window_and_op_count_completes_quickly(self):
        import time

        n = 20_000
        # processed_window: n entries, all within the lookback window so
        # is_eligible must actually consult processed_ids for each of them
        # rather than short-circuiting earlier (e.g. via the watermark-only
        # checks).
        window = [
            ProcessedRef(logical_operation_id=f"op-window-{i}", watermark=wm(45))
            for i in range(n)
        ]
        state = state_at(wm(50), processed_window=window)  # window (40, 50]

        # n candidate ops, all inside the same lookback window, each with a
        # DISTINCT logical_operation_id not present in processed_window, so
        # every single one reaches the processed_ids membership check.
        ops = [
            make_op(object_key=f"k{i}", minute=45, operation_version=f"cand-{i}")
            for i in range(n)
        ]

        start = time.monotonic()
        eligible, _, _ = select_eligible_operations(ops, state, _LOOKBACK)
        elapsed = time.monotonic() - start

        assert len(eligible) == n
        # O(n) (one set built once, n O(1) lookups) should comfortably finish
        # in well under a second even at n=20,000; the O(n * window_size) bug
        # this guards against would take on the order of tens of seconds to
        # minutes at this size, and would exceed the Lambda's 900s timeout
        # entirely at real production scale (n ~ 100,000).
        assert elapsed < 5.0, (
            f"select_eligible_operations took {elapsed:.2f}s for n={n} — "
            "likely reintroduced the O(n * window_size) processed_ids "
            "rebuild-per-call regression"
        )


# ---------------------------------------------------------------------------
# build_submitted_refs — refs come from the written manifest, not from every
# eligible operation (Requirements 2.1, 2.3, 2.4, 2.7)
# ---------------------------------------------------------------------------
class TestBuildSubmittedRefs:
    def test_only_ops_in_kept_triples_are_recorded(self):
        """A matching op is recorded; a non-matching one in the same interval is not.

        Validates: Requirements 2.1
        """
        matched = make_op(object_key="matched", operation_version="v1", minute=5)
        unmatched = make_op(object_key="unmatched", operation_version="v2", minute=6)
        kept = {("bucket-a", "matched", "v1")}

        refs = build_submitted_refs([matched, unmatched], kept)

        assert [r.logical_operation_id for r in refs] == [
            matched.logical_operation_id
        ]
        assert refs[0].watermark == wm(5)

    def test_every_op_sharing_one_kept_triple_is_recorded(self):
        """Two matching tag states on one object version both get a ref.

        Validates: Requirements 2.4
        """
        first = make_op(
            object_key="k", operation_version="v1", minute=5,
            resulting_tag_set={"replicate": "yes"},
        )
        second = make_op(
            object_key="k", operation_version="v1", minute=6,
            resulting_tag_set={"replicate": "yes", "extra": "1"},
        )
        assert first.logical_operation_id != second.logical_operation_id

        refs = build_submitted_refs([first, second], {("bucket-a", "k", "v1")})

        assert {r.logical_operation_id for r in refs} == {
            first.logical_operation_id, second.logical_operation_id
        }
        assert {r.watermark for r in refs} == {wm(5), wm(6)}

    def test_delete_filter_excluded_op_is_not_recorded(self):
        """An op whose version the Deleted_Version_Filter excluded gets no ref.

        Validates: Requirements 2.7
        """
        kept_op = make_op(object_key="kept", operation_version="v1", minute=5)
        excluded_op = make_op(object_key="gone", operation_version=None, minute=6)

        refs = build_submitted_refs(
            [kept_op, excluded_op], {("bucket-a", "kept", "v1")}
        )

        assert [r.logical_operation_id for r in refs] == [
            kept_op.logical_operation_id
        ]

    def test_null_version_triple_matches_none_operation_version(self):
        """A manifest entry with version_id None joins to operation_version None.

        Validates: Requirements 2.1
        """
        op = make_op(object_key="k", operation_version=None, minute=5)

        refs = build_submitted_refs([op], {("bucket-a", "k", None)})

        assert [r.logical_operation_id for r in refs] == [op.logical_operation_id]

    def test_empty_inputs_yield_no_refs(self):
        """No ops or no kept triples means nothing is recorded as processed.

        Validates: Requirements 2.1
        """
        op = make_op(object_key="k", operation_version="v1")
        assert build_submitted_refs([], {("bucket-a", "k", "v1")}) == []
        assert build_submitted_refs([op], set()) == []
