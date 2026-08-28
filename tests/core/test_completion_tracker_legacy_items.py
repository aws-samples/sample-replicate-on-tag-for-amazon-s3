"""Leftover 1.0.1 completion items drain instead of accumulating.

A state object written by 1.0.1 can hold items 1.1.0 has no way to resolve:
lifecycle ``PENDING``, or ``RESOLVED`` carrying an outcome of ``PENDING``,
``GONE``, or ``EXPIRED``. Because publication is decided per item, such an item
blocks nothing — but nothing prunes it either, so it would stay in the state
object for the life of the stack, growing it without bound, and the objects it
describes would never be reported at all.

``resolve_legacy_item`` normalizes them in memory so the ordinary
publish-then-delete path removes them.

**Validates: Requirements 4.2, 4.6**
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.core.completion_tracker import (
    build_completion_report,
    is_legacy_item,
    resolve_legacy_item,
    should_publish,
)
from src.core.models import (
    CompletionState,
    ConfigContext,
    ScanState,
    TrackedObject,
)

_BUCKET = "example-source-bucket"
_MANIFEST_AT = datetime(2026, 8, 20, 9, 0, tzinfo=timezone.utc)  # noqa: UP017
_TAGGED_AT = _MANIFEST_AT - timedelta(minutes=5)


def _item(
    *,
    object_key: str = "obj.txt",
    state: CompletionState,
    outcome: str | None,
) -> TrackedObject:
    return TrackedObject(
        source_bucket=_BUCKET,
        object_key=object_key,
        version_id="v1",
        configs={
            _BUCKET: ConfigContext(
                replication_config_id=_BUCKET,
                job_id="job-old",
                manifest_generated_at=_MANIFEST_AT,
            )
        },
        state=state,
        resolved_at=None if state is CompletionState.PENDING else _MANIFEST_AT,
        resolution_method=None if state is CompletionState.PENDING else "source_status_header",
        replication_outcome=outcome,
        tagged_at=_TAGGED_AT,
        last_modified=_TAGGED_AT,
        matched_rules=frozenset({"rule-a"}),
        destinations=frozenset({"dest-a"}),
    )


def _quiescent() -> dict[str, ScanState]:
    """Scan state that satisfies quiescence for the item's config."""
    return {
        _BUCKET: ScanState(
            last_scan_at=_MANIFEST_AT + timedelta(hours=1),
            last_scan_match_count=0,
        )
    }


_LEGACY_SHAPES = [
    ("lifecycle PENDING", CompletionState.PENDING, None),
    ("lifecycle PENDING with outcome", CompletionState.PENDING, "PENDING"),
    ("resolved PENDING", CompletionState.RESOLVED, "PENDING"),
    ("resolved GONE", CompletionState.RESOLVED, "GONE"),
    ("resolved EXPIRED", CompletionState.RESOLVED, "EXPIRED"),
]


class TestLegacyDetection:
    @pytest.mark.parametrize(("label", "state", "outcome"), _LEGACY_SHAPES)
    def test_every_legacy_shape_is_detected(self, label, state, outcome):
        assert is_legacy_item(_item(state=state, outcome=outcome)), label

    @pytest.mark.parametrize("outcome", ["COMPLETE", "FAILED", "UNKNOWN"])
    def test_a_1_1_0_outcome_is_not_legacy(self, outcome):
        assert not is_legacy_item(
            _item(state=CompletionState.RESOLVED, outcome=outcome)
        )

    @pytest.mark.parametrize("outcome", ["COMPLETE", "FAILED", "UNKNOWN"])
    def test_a_1_1_0_item_is_returned_unchanged(self, outcome):
        item = _item(state=CompletionState.RESOLVED, outcome=outcome)

        assert resolve_legacy_item(item) is item


class TestLegacyNormalization:
    @pytest.mark.parametrize(("label", "state", "outcome"), _LEGACY_SHAPES)
    def test_normalizes_to_resolved_unknown(self, label, state, outcome):
        resolved = resolve_legacy_item(_item(state=state, outcome=outcome))

        assert resolved.state is CompletionState.RESOLVED, label
        assert resolved.replication_outcome == "UNKNOWN", label
        assert resolved.resolution_method == "legacy_1_0_1_state", label

    @pytest.mark.parametrize(("label", "state", "outcome"), _LEGACY_SHAPES)
    def test_preserves_identity_routing_and_timestamps(self, label, state, outcome):
        original = _item(state=state, outcome=outcome)

        resolved = resolve_legacy_item(original)

        assert resolved.source_bucket == original.source_bucket, label
        assert resolved.object_key == original.object_key, label
        assert resolved.version_id == original.version_id, label
        assert resolved.configs == original.configs, label
        assert resolved.tagged_at == original.tagged_at, label
        assert resolved.last_modified == original.last_modified, label
        assert resolved.matched_rules == original.matched_rules, label
        assert resolved.destinations == original.destinations, label

    def test_does_not_mutate_the_stored_item(self):
        """The state object is untouched; normalization is in memory only."""
        original = _item(state=CompletionState.PENDING, outcome=None)

        resolve_legacy_item(original)

        assert original.state is CompletionState.PENDING
        assert original.replication_outcome is None
        assert original.resolution_method is None

    def test_normalizing_is_idempotent(self):
        once = resolve_legacy_item(_item(state=CompletionState.PENDING, outcome=None))

        assert resolve_legacy_item(once) is once


class TestLegacyItemsBecomePublishable:
    def test_a_lifecycle_pending_item_is_unpublishable_before_normalization(self):
        """Without normalization the item is stranded, which is the defect."""
        item = _item(state=CompletionState.PENDING, outcome=None)

        assert not should_publish(item, _quiescent())

    @pytest.mark.parametrize(("label", "state", "outcome"), _LEGACY_SHAPES)
    def test_publishable_after_normalization_when_quiescent(self, label, state, outcome):
        resolved = resolve_legacy_item(_item(state=state, outcome=outcome))

        assert should_publish(resolved, _quiescent()), label

    def test_normalization_does_not_bypass_the_quiescence_gate(self):
        """A normalized item still waits for a clean scan, like any other."""
        resolved = resolve_legacy_item(_item(state=CompletionState.PENDING, outcome=None))
        not_quiescent = {
            _BUCKET: ScanState(
                last_scan_at=_MANIFEST_AT + timedelta(hours=1),
                last_scan_match_count=3,
            )
        }

        assert not should_publish(resolved, not_quiescent)
        assert not should_publish(resolved, {})


class TestLegacyItemsInTheReport:
    def test_a_legacy_item_is_counted_and_actionable(self):
        """GONE and EXPIRED counts used to vanish from the human summary.

        Neither value appears in ``_OUTCOME_PHRASES``, so a report containing
        only such items produced a summary with an empty clause list. As
        ``UNKNOWN`` they are both counted and flagged as needing attention.
        """
        items = [
            resolve_legacy_item(
                _item(object_key="gone.txt", state=CompletionState.RESOLVED, outcome="GONE")
            ),
            resolve_legacy_item(
                _item(object_key="exp.txt", state=CompletionState.RESOLVED, outcome="EXPIRED")
            ),
        ]

        report = build_completion_report(_BUCKET, items, outstanding_jobs=0)

        assert report["outcome_counts"] == {"UNKNOWN": 2}
        assert "GONE" not in report["summary"]
        assert "EXPIRED" not in report["summary"]
        assert "unrecognized task status" in report["summary"]
        assert "Action needed" in report["summary"]

    def test_the_un_normalized_summary_is_the_bug_being_fixed(self):
        """Non-vacuous: the same items unnormalized produce a broken summary."""
        items = [
            _item(object_key="gone.txt", state=CompletionState.RESOLVED, outcome="GONE"),
        ]

        report = build_completion_report(_BUCKET, items, outstanding_jobs=0)

        assert report["outcome_counts"] == {"GONE": 1}
        # The count is dropped from the prose entirely, leaving an empty clause.
        assert "1 " not in report["summary"].split("—")[-1]

    def test_legacy_items_drain_rather_than_accumulating(self):
        """The defect: an un-normalized legacy item is never publishable, so it is
        never deleted and the bucket's completion items only ever grow."""
        legacy = resolve_legacy_item(
            _item(object_key="legacy.txt", state=CompletionState.PENDING, outcome=None)
        )
        fresh = _item(
            object_key="fresh.txt", state=CompletionState.RESOLVED, outcome="COMPLETE"
        )
        scan_state = _quiescent()

        publishable = [
            item for item in (legacy, fresh) if should_publish(item, scan_state)
        ]

        assert len(publishable) == 2, (
            "both items must publish, or the un-published one is never deleted"
        )

        report = build_completion_report(_BUCKET, publishable, outstanding_jobs=0)
        assert report["item_count"] == 2
        assert "No replication jobs remain outstanding." in report["summary"]
