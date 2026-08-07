"""Tests for the Archived_Object_Filter.

Covers the storage-class classification itself, and the interaction with
deduplication that the filter's placement depends on: a lifecycle transition
into an archived class must be the record that survives dedup, or an object
already known to be archived still reaches a manifest.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

from src.core.archived_filter import (
    ARCHIVED_STORAGE_CLASSES,
    count_by_storage_class,
    filter_archived_operations,
    is_archived,
)
from src.core.journal_dedup import select_eligible_operations
from src.core.models import CheckpointState, TaggingOperation

_EPOCH = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
_LOOKBACK = timedelta(minutes=10)

# Every storage class the journal's storage_class column is documented to
# carry. Kept complete rather than sampled so a class added to
# ARCHIVED_STORAGE_CLASSES without deliberate intent shows up as a failure.
_ALL_STORAGE_CLASSES = (
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "DEEP_ARCHIVE",
    "GLACIER_IR",
)

_REPLICABLE = (
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER_IR",
)


def _op(
    object_key: str = "path/obj.txt",
    storage_class: str | None = "STANDARD",
    sequence_number: str = "seq-001",
    minute: int = 1,
    tags: dict | None = None,
    operation_version: str | None = "v1",
) -> TaggingOperation:
    return TaggingOperation(
        source_bucket="bucket-a",
        object_key=object_key,
        resulting_tag_set=tags if tags is not None else {"replicate": "true"},
        sequence_number=sequence_number,
        operation="PutObjectTagging",
        event_time=_EPOCH + timedelta(minutes=minute),
        operation_version=operation_version,
        storage_class=storage_class,
    )


# ---------------------------------------------------------------------------
# is_archived — classification
# ---------------------------------------------------------------------------


class TestIsArchived:
    def test_glacier_and_deep_archive_are_archived(self):
        assert is_archived("GLACIER")
        assert is_archived("DEEP_ARCHIVE")

    def test_glacier_ir_is_not_archived(self):
        """S3 Glacier Instant Retrieval replicates normally.

        Despite the name it is not an archived class and needs no restore.
        Excluding it would silently drop objects the Solution exists to
        replicate.
        """
        assert not is_archived("GLACIER_IR")

    def test_intelligent_tiering_is_not_archived(self):
        """INTELLIGENT_TIERING cannot be treated as archived.

        The Archive Access and Deep Archive Access tiers are equally
        unreplicable, but the journal reports INTELLIGENT_TIERING for such an
        object whatever tier it currently occupies, so an archive-tier object
        is indistinguishable here from a frequent-access one. Treating the
        class as archived would exclude every Intelligent-Tiering object in
        the bucket.
        """
        assert not is_archived("INTELLIGENT_TIERING")

    def test_every_other_documented_class_is_replicable(self):
        for storage_class in _REPLICABLE:
            assert not is_archived(storage_class), storage_class

    def test_none_fails_open(self):
        """Absent evidence is not evidence of archival.

        The journal documents storage_class as optional and writes NULL for
        records whose object version no longer existed when the event was
        processed. Excluding on None would drop replicable objects.
        """
        assert not is_archived(None)
        assert not is_archived("")

    def test_case_and_whitespace_tolerant(self):
        assert is_archived("glacier")
        assert is_archived("  Deep_Archive  ")
        assert not is_archived(" standard ")

    def test_archived_set_contents_are_exactly_the_two_classes(self):
        """Guards against a class being added without the reasoning above."""
        assert ARCHIVED_STORAGE_CLASSES == frozenset({"GLACIER", "DEEP_ARCHIVE"})

    @given(st.sampled_from(_ALL_STORAGE_CLASSES))
    def test_classification_partitions_documented_classes(self, storage_class: str):
        assert is_archived(storage_class) == (storage_class in ARCHIVED_STORAGE_CLASSES)


# ---------------------------------------------------------------------------
# filter_archived_operations
# ---------------------------------------------------------------------------


class TestFilterArchivedOperations:
    def test_splits_archived_from_replicable(self):
        ops = [
            _op("a.txt", storage_class="STANDARD"),
            _op("b.txt", storage_class="GLACIER"),
            _op("c.txt", storage_class="DEEP_ARCHIVE"),
            _op("d.txt", storage_class="GLACIER_IR"),
        ]
        kept, excluded = filter_archived_operations(ops)
        assert [o.object_key for o in kept] == ["a.txt", "d.txt"]
        assert [o.object_key for o in excluded] == ["b.txt", "c.txt"]

    def test_empty_input(self):
        assert filter_archived_operations([]) == ([], [])

    def test_no_archived_objects_returns_empty_excluded(self):
        ops = [_op("a.txt"), _op("b.txt")]
        kept, excluded = filter_archived_operations(ops)
        assert kept == ops
        assert excluded == []

    def test_all_archived_returns_empty_kept(self):
        ops = [_op("a.txt", storage_class="GLACIER")]
        kept, excluded = filter_archived_operations(ops)
        assert kept == []
        assert len(excluded) == 1

    def test_missing_storage_class_is_kept(self):
        ops = [_op("a.txt", storage_class=None)]
        kept, excluded = filter_archived_operations(ops)
        assert len(kept) == 1
        assert excluded == []

    def test_order_preserved_in_both_outputs(self):
        ops = [
            _op(f"{i}.txt", storage_class="GLACIER" if i % 2 else "STANDARD")
            for i in range(10)
        ]
        kept, excluded = filter_archived_operations(ops)
        assert [o.object_key for o in kept] == [f"{i}.txt" for i in range(0, 10, 2)]
        assert [o.object_key for o in excluded] == [f"{i}.txt" for i in range(1, 10, 2)]

    @given(st.lists(st.sampled_from(_ALL_STORAGE_CLASSES), max_size=40))
    def test_partition_is_total_and_disjoint(self, classes: list[str]):
        ops = [_op(f"{i}.txt", storage_class=c) for i, c in enumerate(classes)]
        kept, excluded = filter_archived_operations(ops)
        assert len(kept) + len(excluded) == len(ops)
        assert all(not is_archived(o.storage_class) for o in kept)
        assert all(is_archived(o.storage_class) for o in excluded)


# ---------------------------------------------------------------------------
# count_by_storage_class
# ---------------------------------------------------------------------------


class TestCountByStorageClass:
    def test_breakdown_by_class(self):
        ops = [
            _op("a.txt", storage_class="GLACIER"),
            _op("b.txt", storage_class="GLACIER"),
            _op("c.txt", storage_class="DEEP_ARCHIVE"),
        ]
        assert count_by_storage_class(ops) == {"GLACIER": 2, "DEEP_ARCHIVE": 1}

    def test_normalizes_case(self):
        ops = [_op("a.txt", storage_class="glacier"), _op("b.txt", storage_class="GLACIER")]
        assert count_by_storage_class(ops) == {"GLACIER": 2}

    def test_none_counted_as_unknown(self):
        assert count_by_storage_class([_op("a.txt", storage_class=None)]) == {"UNKNOWN": 1}

    def test_empty_input(self):
        assert count_by_storage_class([]) == {}

    @given(st.lists(st.sampled_from(_ALL_STORAGE_CLASSES), max_size=40))
    def test_total_preserved(self, classes: list[str]):
        ops = [_op(f"{i}.txt", storage_class=c) for i, c in enumerate(classes)]
        assert sum(count_by_storage_class(ops).values()) == len(ops)


# ---------------------------------------------------------------------------
# Interaction with deduplication — the reason for the filter's placement
# ---------------------------------------------------------------------------


class TestDedupOrderingInteraction:
    """The filter runs after dedup, and that ordering is the mechanism.

    A lifecycle transition into an archived class writes its own
    UPDATE_METADATA record. Because a transition changes neither the object
    key, its version, nor its tags, that record's logical_operation_id is
    identical to the earlier tagging record's, so the two collapse in dedup
    and the higher sequence_number wins. The transition is the later record,
    so post-dedup the surviving record reports the archived class and the
    filter excludes it.
    """

    @staticmethod
    def _tag_then_transition() -> tuple[TaggingOperation, TaggingOperation]:
        """The two records a tag-then-transition sequence produces.

        Identical bucket, key, version and tags; differing only in
        storage_class, sequence_number and event_time.
        """
        tagged = _op(
            "photo.jpg", storage_class="STANDARD",
            sequence_number="seq-001", minute=1,
        )
        transitioned = _op(
            "photo.jpg", storage_class="GLACIER",
            sequence_number="seq-002", minute=5,
        )
        return tagged, transitioned

    def test_transition_shares_identity_with_tagging_record(self):
        """The premise: storage_class is outside logical_operation_id."""
        tagged, transitioned = self._tag_then_transition()
        assert tagged.logical_operation_id == transitioned.logical_operation_id

    def test_transition_record_survives_dedup_and_is_then_excluded(self):
        tagged, transitioned = self._tag_then_transition()
        state = CheckpointState(
            source_bucket="bucket-a", last_processed_watermark="", lease=None,
        )
        deduped, _skipped, _hwm = select_eligible_operations(
            [tagged, transitioned], state, _LOOKBACK,
        )
        # One identity, and the winner is the transition.
        assert len(deduped) == 1
        assert deduped[0].storage_class == "GLACIER"
        assert deduped[0].sequence_number == "seq-002"

        kept, excluded = filter_archived_operations(deduped)
        assert kept == []
        assert len(excluded) == 1

    def test_row_order_from_athena_does_not_change_the_outcome(self):
        """Dedup selects on sequence_number, not on arrival order."""
        tagged, transitioned = self._tag_then_transition()
        state = CheckpointState(
            source_bucket="bucket-a", last_processed_watermark="", lease=None,
        )
        for ordering in ([tagged, transitioned], [transitioned, tagged]):
            deduped, _s, _h = select_eligible_operations(ordering, state, _LOOKBACK)
            kept, excluded = filter_archived_operations(deduped)
            assert kept == []
            assert len(excluded) == 1

    def test_filtering_before_dedup_would_let_the_object_through(self):
        """Demonstrates why the SQL WHERE clause is the wrong place.

        Excluding archived records first removes the transition, leaving the
        pre-transition tagging record as the survivor. That record reports the
        storage class the object held *before* it was archived, so it passes
        the filter and would reach a manifest. This test pins the failure mode
        so a later change that moves the exclusion earlier fails here rather
        than silently in production.
        """
        tagged, transitioned = self._tag_then_transition()
        state = CheckpointState(
            source_bucket="bucket-a", last_processed_watermark="", lease=None,
        )

        # Wrong order: filter, then dedup.
        prefiltered, _ = filter_archived_operations([tagged, transitioned])
        deduped_wrong, _s, _h = select_eligible_operations(
            prefiltered, state, _LOOKBACK,
        )
        kept_wrong, _ = filter_archived_operations(deduped_wrong)
        assert [o.object_key for o in kept_wrong] == ["photo.jpg"]
        assert kept_wrong[0].storage_class == "STANDARD"

        # Right order: dedup, then filter.
        deduped_right, _s, _h = select_eligible_operations(
            [tagged, transitioned], state, _LOOKBACK,
        )
        kept_right, _ = filter_archived_operations(deduped_right)
        assert kept_right == []

    def test_already_archived_object_retagged_is_excluded(self):
        """The simpler case: the tagging record itself reports GLACIER.

        An object archived long before being re-tagged produces a single
        UPDATE_METADATA record whose storage_class is already the archived
        class, so no dedup interaction is involved.
        """
        state = CheckpointState(
            source_bucket="bucket-a", last_processed_watermark="", lease=None,
        )
        deduped, _s, _h = select_eligible_operations(
            [_op("old.txt", storage_class="DEEP_ARCHIVE")], state, _LOOKBACK,
        )
        kept, excluded = filter_archived_operations(deduped)
        assert kept == []
        assert len(excluded) == 1

    def test_unrelated_objects_are_unaffected(self):
        """An archived object does not suppress a replicable one."""
        tagged, transitioned = self._tag_then_transition()
        other = _op("doc.pdf", storage_class="STANDARD", sequence_number="seq-003")
        state = CheckpointState(
            source_bucket="bucket-a", last_processed_watermark="", lease=None,
        )
        deduped, _s, _h = select_eligible_operations(
            [tagged, transitioned, other], state, _LOOKBACK,
        )
        kept, excluded = filter_archived_operations(deduped)
        assert [o.object_key for o in kept] == ["doc.pdf"]
        assert [o.object_key for o in excluded] == ["photo.jpg"]
