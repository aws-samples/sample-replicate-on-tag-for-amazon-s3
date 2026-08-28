"""Unit tests for src/core/checkpoint_logic.py.

Covers Requirements 4.3, 4.6, 7.6, 7.7, 9.1, 9.3, 9.4, 10.2, 12.6 under the
``record_timestamp`` watermark model:

- is_eligible: watermark gating (9.1), lookback-window late-arrival handling,
  processed-operation-window suppression (9.x), in-flight lease exclusion (9.4)
- advance_checkpoint: success advances the watermark monotonically (4.3, 9.1)
  and maintains the bounded processed-operation window; failure leaves the
  watermark unchanged (7.6, 7.7, 9.3, 10.2, 12.6); lease always cleared (9.4)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.core.checkpoint_logic import advance_checkpoint, is_eligible
from src.core.models import (
    CheckpointState,
    Lease,
    LeaseStatus,
    ProcessedRef,
    TaggingOperation,
)
from src.core.watermark import to_watermark

_EPOCH = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
_LOOKBACK = timedelta(minutes=10)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _at(minute: int, second: int = 0) -> datetime:
    """Return a UTC datetime offset from the base by minutes/seconds."""
    return _EPOCH + timedelta(minutes=minute, seconds=second)


def wm(minute: int, second: int = 0) -> str:
    """Canonical watermark string for the given minute/second offset."""
    return to_watermark(_at(minute, second))


def make_op(
    minute: int,
    object_key: str = "path/obj.txt",
    source_bucket: str = "my-bucket",
    operation_version: str | None = "v1",
) -> TaggingOperation:
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key=object_key,
        resulting_tag_set={"env": "prod"},
        sequence_number="seq-001",
        operation="PutObjectTagging",
        event_time=_at(minute),
        operation_version=operation_version,
    )


def make_state(
    watermark: str = "",
    lease: Lease | None = None,
    processed_window: list[ProcessedRef] | None = None,
    source_bucket: str = "my-bucket",
) -> CheckpointState:
    return CheckpointState(
        source_bucket=source_bucket,
        last_processed_watermark=watermark,
        lease=lease,
        processed_window=processed_window or [],
    )


def make_lease(
    candidate_max: str,
    status: LeaseStatus = LeaseStatus.IN_FLIGHT,
) -> Lease:
    return Lease(
        lease_id="lease-abc",
        candidate_max_watermark=candidate_max,
        acquired_at=_EPOCH,
        status=status,
    )


# ---------------------------------------------------------------------------
# is_eligible — watermark gating (Req 9.1)
# ---------------------------------------------------------------------------


class TestIsEligibleWatermark:
    def test_above_watermark_is_eligible(self):
        op = make_op(minute=60)
        state = make_state(watermark=wm(50))
        assert is_eligible(op, state, _LOOKBACK) is True

    def test_epoch_watermark_makes_all_ops_eligible(self):
        op = make_op(minute=1)
        state = make_state(watermark="")
        assert is_eligible(op, state, _LOOKBACK) is True

    def test_at_watermark_within_lookback_not_previously_processed_is_eligible(self):
        """A record exactly at the watermark, inside the window and unseen → late arrival."""
        op = make_op(minute=50)
        state = make_state(watermark=wm(50))  # window is (40, 50]
        assert is_eligible(op, state, _LOOKBACK) is True

    def test_below_watermark_outside_lookback_not_eligible(self):
        """Already processed and too old to re-check → excluded (Req 9.1)."""
        op = make_op(minute=30)
        state = make_state(watermark=wm(50))  # window start is minute 40
        assert is_eligible(op, state, _LOOKBACK) is False

    def test_below_watermark_at_window_start_not_eligible(self):
        """Exactly at the window start (exclusive lower bound) → excluded."""
        op = make_op(minute=40)
        state = make_state(watermark=wm(50))  # window is (40, 50]
        assert is_eligible(op, state, _LOOKBACK) is False


# ---------------------------------------------------------------------------
# is_eligible — lookback window + processed-operation window (Req 9.x)
# ---------------------------------------------------------------------------


class TestIsEligibleLookbackWindow:
    def test_late_arrival_in_window_not_processed_is_eligible(self):
        """Record inside the lookback window, not yet submitted → eligible."""
        op = make_op(minute=45, operation_version="late")
        state = make_state(watermark=wm(50))  # window (40, 50]
        assert is_eligible(op, state, _LOOKBACK) is True

    def test_record_in_window_already_processed_is_excluded(self):
        """Re-scanned record already submitted on an earlier run → excluded (Req 9.1)."""
        op = make_op(minute=45, operation_version="seen")
        state = make_state(
            watermark=wm(50),
            processed_window=[
                ProcessedRef(
                    logical_operation_id=op.logical_operation_id,
                    watermark=wm(45),
                )
            ],
        )
        assert is_eligible(op, state, _LOOKBACK) is False

    def test_different_logical_id_in_window_still_eligible(self):
        """The window only suppresses the exact logical operation, not others."""
        op = make_op(minute=45, operation_version="new")
        state = make_state(
            watermark=wm(50),
            processed_window=[
                ProcessedRef(logical_operation_id="some-other-op", watermark=wm(45))
            ],
        )
        assert is_eligible(op, state, _LOOKBACK) is True

    def test_zero_lookback_excludes_everything_at_or_below_watermark(self):
        """With no lookback, only strictly-newer records are eligible."""
        op = make_op(minute=50)
        state = make_state(watermark=wm(50))
        assert is_eligible(op, state, timedelta(0)) is False


# ---------------------------------------------------------------------------
# is_eligible — in-flight lease exclusion (Req 9.4)
# ---------------------------------------------------------------------------


class TestIsEligibleLease:
    def test_op_at_lease_hwm_is_excluded(self):
        op = make_op(minute=99)
        state = make_state(watermark=wm(50), lease=make_lease(candidate_max=wm(99)))
        assert is_eligible(op, state, _LOOKBACK) is False

    def test_op_below_lease_hwm_is_excluded(self):
        op = make_op(minute=60)
        state = make_state(watermark=wm(50), lease=make_lease(candidate_max=wm(99)))
        assert is_eligible(op, state, _LOOKBACK) is False

    def test_op_above_lease_hwm_is_eligible(self):
        op = make_op(minute=100)
        state = make_state(watermark=wm(50), lease=make_lease(candidate_max=wm(99)))
        assert is_eligible(op, state, _LOOKBACK) is True

    def test_no_lease_ignores_lease_check(self):
        op = make_op(minute=75)
        state = make_state(watermark=wm(50), lease=None)
        assert is_eligible(op, state, _LOOKBACK) is True


# ---------------------------------------------------------------------------
# advance_checkpoint — success path (Req 4.3, 9.1)
# ---------------------------------------------------------------------------


class TestAdvanceCheckpointSuccess:
    def test_advance_when_ref_above_current(self):
        state = make_state(watermark=wm(50))
        refs = [ProcessedRef(logical_operation_id="op-a", watermark=wm(90))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.last_processed_watermark == wm(90)

    def test_watermark_is_max_of_submitted_refs(self):
        state = make_state(watermark=wm(50))
        refs = [
            ProcessedRef(logical_operation_id="op-a", watermark=wm(70)),
            ProcessedRef(logical_operation_id="op-b", watermark=wm(95)),
            ProcessedRef(logical_operation_id="op-c", watermark=wm(85)),
        ]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.last_processed_watermark == wm(95)

    def test_advance_from_epoch(self):
        state = make_state(watermark="")
        refs = [ProcessedRef(logical_operation_id="op-a", watermark=wm(10))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.last_processed_watermark == wm(10)

    def test_source_bucket_preserved(self):
        state = make_state(watermark=wm(50), source_bucket="specific-bucket")
        refs = [ProcessedRef(logical_operation_id="op-a", watermark=wm(90))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.source_bucket == "specific-bucket"

    def test_window_keeps_refs_within_lookback_of_new_watermark(self):
        """Submitted refs within lookback of the new watermark are retained."""
        state = make_state(watermark=wm(50))
        refs = [
            ProcessedRef(logical_operation_id="recent", watermark=wm(95)),
            ProcessedRef(logical_operation_id="edge", watermark=wm(91)),  # window start = 85
        ]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        ids = {r.logical_operation_id for r in new_state.processed_window}
        assert ids == {"recent", "edge"}

    def test_window_prunes_refs_below_lookback_of_new_watermark(self):
        """Refs older than (new_watermark - lookback) are pruned — they can't be re-scanned."""
        state = make_state(
            watermark=wm(50),
            processed_window=[
                ProcessedRef(logical_operation_id="old", watermark=wm(40)),
            ],
        )
        refs = [ProcessedRef(logical_operation_id="new", watermark=wm(95))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)  # window start = 85
        ids = {r.logical_operation_id for r in new_state.processed_window}
        assert "old" not in ids
        assert ids == {"new"}

    def test_window_dedups_by_logical_id_keeping_highest_watermark(self):
        state = make_state(
            watermark=wm(50),
            processed_window=[
                ProcessedRef(logical_operation_id="op", watermark=wm(88)),
            ],
        )
        refs = [ProcessedRef(logical_operation_id="op", watermark=wm(92))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        matching = [r for r in new_state.processed_window if r.logical_operation_id == "op"]
        assert len(matching) == 1
        assert matching[0].watermark == wm(92)


# ---------------------------------------------------------------------------
# advance_checkpoint — failure path (Req 7.6, 7.7, 9.3, 10.2, 12.6)
# ---------------------------------------------------------------------------


class TestAdvanceCheckpointFailure:
    def test_none_refs_leaves_watermark_unchanged(self):
        state = make_state(watermark=wm(50))
        new_state = advance_checkpoint(state, None, _LOOKBACK)
        assert new_state.last_processed_watermark == wm(50)

    def test_empty_refs_leaves_watermark_unchanged(self):
        state = make_state(watermark=wm(50))
        new_state = advance_checkpoint(state, [], _LOOKBACK)
        assert new_state.last_processed_watermark == wm(50)

    def test_failure_preserves_existing_window(self):
        window = [ProcessedRef(logical_operation_id="op", watermark=wm(48))]
        state = make_state(watermark=wm(50), processed_window=window)
        new_state = advance_checkpoint(state, None, _LOOKBACK)
        assert new_state.processed_window == window

    def test_source_bucket_preserved_on_failure(self):
        state = make_state(watermark=wm(50), source_bucket="my-bucket")
        new_state = advance_checkpoint(state, None, _LOOKBACK)
        assert new_state.source_bucket == "my-bucket"


# ---------------------------------------------------------------------------
# advance_checkpoint — lease always cleared (Req 9.4)
# ---------------------------------------------------------------------------


class TestAdvanceCheckpointLeaseCleared:
    def test_lease_cleared_on_success(self):
        state = make_state(watermark=wm(50), lease=make_lease(candidate_max=wm(90)))
        refs = [ProcessedRef(logical_operation_id="op", watermark=wm(90))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.lease is None

    def test_lease_cleared_on_failure(self):
        state = make_state(watermark=wm(50), lease=make_lease(candidate_max=wm(90)))
        new_state = advance_checkpoint(state, None, _LOOKBACK)
        assert new_state.lease is None

    def test_advance_checkpoint_does_not_mutate_input(self):
        lease = make_lease(candidate_max=wm(90))
        window = [ProcessedRef(logical_operation_id="op", watermark=wm(48))]
        state = make_state(watermark=wm(50), lease=lease, processed_window=window)
        original_wm = state.last_processed_watermark
        original_window = list(state.processed_window)

        advance_checkpoint(state, [ProcessedRef("op2", wm(90))], _LOOKBACK)

        assert state.last_processed_watermark == original_wm
        assert state.lease is lease
        assert state.processed_window == original_window


# ---------------------------------------------------------------------------
# Property 6: Checkpoint advances only on success; failures remain eligible
# Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success;
#          failures remain eligible
# ---------------------------------------------------------------------------

from hypothesis import assume, given, settings
from hypothesis import strategies as st

# Canonical watermarks ordered by an integer second offset, so larger int =>
# chronologically (and lexicographically) larger watermark.
_seconds = st.integers(min_value=0, max_value=1_000_000)


def _wm_from_seconds(s: int) -> str:
    return to_watermark(_EPOCH + timedelta(seconds=s))


class TestProperty6CheckpointAdvancementAndEligibility:
    """Watermark advances only on success; failed operations remain eligible.

    # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
    Validates: Requirements 4.3, 4.6, 7.6, 7.7, 9.1, 9.3, 9.4, 10.2, 12.6
    """

    @given(checkpoint_s=_seconds, hwm_s=_seconds)
    @settings(max_examples=100)
    def test_successful_submission_advances_watermark(self, checkpoint_s, hwm_s):
        """Watermark advances to the submitted HWM on success (Req 4.3, 9.1).

        # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
        """
        assume(hwm_s > checkpoint_s)
        state = make_state(watermark=_wm_from_seconds(checkpoint_s))
        refs = [ProcessedRef(logical_operation_id="op", watermark=_wm_from_seconds(hwm_s))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.last_processed_watermark == _wm_from_seconds(hwm_s)

    @given(checkpoint_s=_seconds)
    @settings(max_examples=100)
    def test_failed_submission_leaves_watermark_unchanged(self, checkpoint_s):
        """No submission leaves the watermark unchanged (Req 7.6, 7.7, 9.3, 10.2).

        # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
        """
        state = make_state(watermark=_wm_from_seconds(checkpoint_s))
        new_state = advance_checkpoint(state, None, _LOOKBACK)
        assert new_state.last_processed_watermark == _wm_from_seconds(checkpoint_s)

    @given(checkpoint_s=_seconds, hwm_s=_seconds, op_s=_seconds)
    @settings(max_examples=100)
    def test_operation_excluded_when_at_or_below_advanced_watermark_outside_lookback(
        self, checkpoint_s, hwm_s, op_s
    ):
        """Ops at/below the advanced watermark and older than the lookback are excluded.

        # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
        """
        assume(hwm_s > checkpoint_s)
        # Place the op well below the new watermark so it is outside any lookback
        # window (use a large gap relative to the 10-minute lookback).
        assume(op_s <= hwm_s - 3600)
        state = make_state(watermark=_wm_from_seconds(checkpoint_s))
        refs = [ProcessedRef(logical_operation_id="hwm-op", watermark=_wm_from_seconds(hwm_s))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)

        op = TaggingOperation(
            source_bucket="b",
            object_key="k.txt",
            resulting_tag_set={"k": "v"},
            sequence_number="seq",
            operation="PutObjectTagging",
            event_time=_EPOCH + timedelta(seconds=op_s),
        )
        assert is_eligible(op, new_state, _LOOKBACK) is False

    @given(checkpoint_s=_seconds, op_s=_seconds)
    @settings(max_examples=100)
    def test_operation_above_watermark_eligible_after_failure(self, checkpoint_s, op_s):
        """Ops above an unchanged watermark remain eligible after failure (Req 9.3).

        # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
        """
        assume(op_s > checkpoint_s)
        state = make_state(watermark=_wm_from_seconds(checkpoint_s))
        new_state = advance_checkpoint(state, None, _LOOKBACK)

        op = TaggingOperation(
            source_bucket="b",
            object_key="k.txt",
            resulting_tag_set={"k": "v"},
            sequence_number="seq",
            operation="PutObjectTagging",
            event_time=_EPOCH + timedelta(seconds=op_s),
        )
        assert is_eligible(op, new_state, _LOOKBACK) is True

    @given(checkpoint_s=_seconds, hwm_s=_seconds)
    @settings(max_examples=100)
    def test_lease_always_cleared_regardless_of_success(self, checkpoint_s, hwm_s):
        """Lease is always None after advance_checkpoint (Req 9.4).

        # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
        """
        lease = make_lease(candidate_max=_wm_from_seconds(hwm_s))
        state = make_state(watermark=_wm_from_seconds(checkpoint_s), lease=lease)
        refs = [ProcessedRef(logical_operation_id="op", watermark=_wm_from_seconds(hwm_s))]
        assert advance_checkpoint(state, refs, _LOOKBACK).lease is None
        assert advance_checkpoint(state, None, _LOOKBACK).lease is None

    @given(checkpoint_s=st.integers(min_value=1, max_value=1_000_000), hwm_s=_seconds)
    @settings(max_examples=100)
    def test_watermark_is_monotonically_non_decreasing(self, checkpoint_s, hwm_s):
        """Watermark never goes backwards even when a submitted ref is older (Req 4.3).

        # Feature: tag-based-s3-replication, Property 6: Checkpoint advances only on success; failures remain eligible
        """
        assume(hwm_s < checkpoint_s)
        state = make_state(watermark=_wm_from_seconds(checkpoint_s))
        refs = [ProcessedRef(logical_operation_id="op", watermark=_wm_from_seconds(hwm_s))]
        new_state = advance_checkpoint(state, refs, _LOOKBACK)
        assert new_state.last_processed_watermark == _wm_from_seconds(checkpoint_s)


# ---------------------------------------------------------------------------
# A newly created stack replicates tagging from just before it existed
# Feature: report-derived-completion
# Requirements: 7.1
# ---------------------------------------------------------------------------


class TestFreshlySeededCheckpointCoversPreCreationTagging:
    """A new stack picks up tagging done shortly before it was deployed.

    ``deploy/config_resource/index.py`` seeds ``last_processed_watermark`` to the
    moment of stack creation with an empty ``processed_window``. That seeded value
    is the newest point already accounted for, not the oldest point that can be
    read: a run scans from ``watermark - lookback``, and an empty
    ``processed_window`` suppresses nothing in that range. So the first run of a
    new stack reads the lookback period preceding creation and replicates whatever
    was tagged in it.

    This is operator-visible behavior rather than an implementation detail. The
    "Journal Start Point" section of ``README.md`` tells an operator to expect the
    first run to replicate objects they tagged while preparing to deploy, and to be
    billed for the resulting Batch Operations job. Asserted here so that
    documentation cannot quietly stop being true.
    """

    _CREATED_AT_MINUTE = 60

    def _seeded_state(self) -> CheckpointState:
        """State exactly as the stack-create custom resource writes it."""
        return make_state(
            watermark=wm(self._CREATED_AT_MINUTE),
            lease=None,
            processed_window=[],
        )

    def test_tagging_inside_the_lookback_window_before_creation_is_eligible(self):
        state = self._seeded_state()

        # _LOOKBACK is 10 minutes, so creation-minus-1 and creation-minus-9 are
        # inside the window.
        assert is_eligible(make_op(self._CREATED_AT_MINUTE - 1), state, _LOOKBACK)
        assert is_eligible(make_op(self._CREATED_AT_MINUTE - 9), state, _LOOKBACK)

    def test_tagging_at_the_moment_of_creation_is_eligible(self):
        state = self._seeded_state()

        assert is_eligible(make_op(self._CREATED_AT_MINUTE), state, _LOOKBACK)

    def test_tagging_older_than_the_lookback_window_is_not_eligible(self):
        """The coverage is bounded, which is why the note still warns."""
        state = self._seeded_state()

        assert not is_eligible(make_op(self._CREATED_AT_MINUTE - 10), state, _LOOKBACK)
        assert not is_eligible(make_op(self._CREATED_AT_MINUTE - 11), state, _LOOKBACK)
        assert not is_eligible(make_op(self._CREATED_AT_MINUTE - 30), state, _LOOKBACK)

    def test_coverage_survives_until_the_first_successful_run(self):
        """The window is anchored on the watermark, not on wall-clock time.

        The watermark advances only when a run succeeds, so a delayed or
        throttled first run does not erode the pre-creation coverage. Once a run
        does succeed and advances the watermark past the window, the pre-creation
        period is out of scope permanently.
        """
        state = self._seeded_state()
        pre_creation = make_op(self._CREATED_AT_MINUTE - 5)

        assert is_eligible(pre_creation, state, _LOOKBACK)

        # After a successful run the watermark has moved past the window that
        # reached back before creation. How advance_checkpoint decides to move it
        # is covered by TestAdvanceCheckpointSuccess; what matters here is that
        # the pre-creation period is then permanently out of scope.
        after_first_run = make_state(
            watermark=wm(self._CREATED_AT_MINUTE + 20),
            processed_window=[],
        )

        assert not is_eligible(pre_creation, after_first_run, _LOOKBACK)
