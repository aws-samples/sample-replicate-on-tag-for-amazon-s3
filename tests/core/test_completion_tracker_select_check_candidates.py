"""Unit and property tests for ``select_check_candidates`` / ``CheckCandidate`` (task 4.1).

Feature: source-status-completion-tracking.

This REPLACES the superseded ``test_completion_tracker_select_poll_candidates.py``
(deleted) entirely: there is no ``CheckKind``, no age gate, and no
``threshold`` parameter. A ``TrackedObject`` is selected iff ``state ==
PENDING`` AND every ``ConfigContext`` is ``bops_confirmed``.
"""
from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import CheckCandidate, select_check_candidates
from src.core.models import CompletionState, ConfigContext, TrackedObject

_MANIFEST_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def make_config_context(
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
    manifest_generated_at: datetime = _MANIFEST_AT,
    bops_confirmed: bool = True,
) -> ConfigContext:
    return ConfigContext(
        replication_config_id=replication_config_id,
        job_id=job_id,
        manifest_generated_at=manifest_generated_at,
        bops_confirmed=bops_confirmed,
    )


def make_obj(
    object_key: str = "a.txt",
    version_id: str | None = "v1",
    source_bucket: str = "my-bucket",
    configs: dict[str, ConfigContext] | None = None,
    state: CompletionState = CompletionState.PENDING,
    replication_outcome: str | None = None,
) -> TrackedObject:
    return TrackedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
        configs=configs if configs is not None else {"cfg-1": make_config_context()},
        state=state,
        replication_outcome=replication_outcome,
    )


class TestSelectCheckCandidates:
    def test_resolved_object_never_included(self):
        obj = make_obj(state=CompletionState.RESOLVED, replication_outcome="COMPLETE")
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert candidates == []

    def test_resolved_object_never_included_even_with_literal_pending_outcome(self):
        """RESOLVED filtering is structural (on state), never on
        replication_outcome — even a RESOLVED object whose
        replication_outcome literal string is "PENDING" (a legal
        Replication_Outcome value, per Property 5) must be excluded."""
        obj = make_obj(state=CompletionState.RESOLVED, replication_outcome="PENDING")
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert candidates == []

    def test_pending_all_confirmed_is_selected(self):
        obj = make_obj(configs={"cfg-1": make_config_context(bops_confirmed=True)})
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert len(candidates) == 1
        assert candidates[0].item_key == "a.txt\x00v1"
        assert candidates[0].obj == obj

    def test_pending_with_unconfirmed_config_is_excluded(self):
        obj = make_obj(configs={"cfg-1": make_config_context(bops_confirmed=False)})
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert candidates == []

    def test_pending_with_one_confirmed_and_one_unconfirmed_config_is_excluded(self):
        obj = make_obj(
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a", bops_confirmed=True),
                "cfg-b": make_config_context(replication_config_id="cfg-b", bops_confirmed=False),
            }
        )
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert candidates == []

    def test_pending_with_all_configs_confirmed_is_selected(self):
        obj = make_obj(
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a", bops_confirmed=True),
                "cfg-b": make_config_context(replication_config_id="cfg-b", bops_confirmed=True),
            }
        )
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert len(candidates) == 1

    def test_flattens_across_multiple_items(self):
        item_one = make_obj(object_key="a.txt")
        item_two = make_obj(object_key="b.txt")
        candidates = select_check_candidates(
            {"a.txt\x00v1": item_one, "b.txt\x00v1": item_two}
        )
        assert len(candidates) == 2
        assert {c.item_key for c in candidates} == {"a.txt\x00v1", "b.txt\x00v1"}

    def test_empty_items_returns_empty_list(self):
        assert select_check_candidates({}) == []

    def test_candidate_carries_the_tracked_object(self):
        obj = make_obj()
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert isinstance(candidates[0], CheckCandidate)
        assert candidates[0].obj == obj

    def test_empty_configs_dict_is_vacuously_selected(self):
        """A TrackedObject with zero configs trivially satisfies the
        conjunction over an empty set — this should not normally occur
        since a TrackedObject is only created alongside at least one
        ConfigContext, but the function must not crash on it."""
        obj = make_obj(configs={})
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert len(candidates) == 1


# ---------------------------------------------------------------------------
# Property 5: A RESOLVED Tracked_Object is never re-selected as a check
# candidate
# Feature: source-status-completion-tracking, Property 5: A RESOLVED Tracked_Object is never re-selected as a check candidate
# Validates: Requirements 3.3
# ---------------------------------------------------------------------------


class TestProperty5ResolvedNeverReselected:
    """# Feature: source-status-completion-tracking, Property 5: A RESOLVED Tracked_Object is never re-selected as a check candidate

    Validates: Requirements 3.3
    """

    @given(
        replication_outcome=st.one_of(
            st.none(), st.sampled_from(["COMPLETE", "PENDING", "FAILED", "UNKNOWN"])
        ),
        bops_confirmed=st.booleans(),
    )
    @settings(max_examples=100)
    def test_resolved_state_excludes_regardless_of_outcome_or_confirmation(
        self, replication_outcome: str | None, bops_confirmed: bool
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 5: A RESOLVED Tracked_Object is never re-selected as a check candidate"""
        obj = make_obj(
            state=CompletionState.RESOLVED,
            replication_outcome=replication_outcome,
            configs={"cfg-1": make_config_context(bops_confirmed=bops_confirmed)},
        )
        candidates = select_check_candidates({"a.txt\x00v1": obj})
        assert candidates == []

    @given(
        states=st.lists(st.sampled_from(list(CompletionState)), min_size=1, max_size=6),
    )
    @settings(max_examples=100)
    def test_mixed_batch_only_pending_selected(self, states: list[CompletionState]) -> None:
        """# Feature: source-status-completion-tracking, Property 5: A RESOLVED Tracked_Object is never re-selected as a check candidate"""
        items = {}
        for idx, state in enumerate(states):
            key = f"key-{idx}\x00v1"
            outcome = "COMPLETE" if state == CompletionState.RESOLVED else None
            items[key] = make_obj(object_key=f"key-{idx}", state=state, replication_outcome=outcome)

        candidates = select_check_candidates(items)
        selected_keys = {c.item_key for c in candidates}
        expected_keys = {
            f"key-{idx}\x00v1" for idx, state in enumerate(states) if state == CompletionState.PENDING
        }
        assert selected_keys == expected_keys


# ---------------------------------------------------------------------------
# Property 6: A Source_Status_Check is never issued for a Tracked_Object
# until every one of its Config_Contexts is bops_confirmed
# Feature: source-status-completion-tracking, Property 6: A Source_Status_Check is never issued for a Tracked_Object until every one of its Config_Contexts is bops_confirmed
# Validates: Requirements 3.1
# ---------------------------------------------------------------------------


class TestProperty6RequiresAllConfigsConfirmed:
    """# Feature: source-status-completion-tracking, Property 6: A Source_Status_Check is never issued for a Tracked_Object until every one of its Config_Contexts is bops_confirmed

    Validates: Requirements 3.1
    """

    @given(
        confirmed_flags=st.lists(st.booleans(), min_size=1, max_size=6),
    )
    @settings(max_examples=100)
    def test_selected_iff_all_confirmed(self, confirmed_flags: list[bool]) -> None:
        """# Feature: source-status-completion-tracking, Property 6: A Source_Status_Check is never issued for a Tracked_Object until every one of its Config_Contexts is bops_confirmed"""
        configs = {
            f"cfg-{idx}": make_config_context(replication_config_id=f"cfg-{idx}", bops_confirmed=flag)
            for idx, flag in enumerate(confirmed_flags)
        }
        obj = make_obj(configs=configs)
        candidates = select_check_candidates({"a.txt\x00v1": obj})

        expected_selected = all(confirmed_flags)
        assert (len(candidates) == 1) is expected_selected
