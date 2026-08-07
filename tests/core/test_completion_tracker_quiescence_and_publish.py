"""Unit and property tests for ``quiescence_check`` / ``should_publish`` (task 6.1).

Feature: source-status-completion-tracking.

Exercises ``should_publish`` against the object-level ``TrackedObject``
type: resolution is now a single object-level ``state`` (not per
destination), so the conjunction is over ``configs`` for quiescence only.
``quiescence_check`` itself is UNCHANGED from the superseded design.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import quiescence_check, should_publish
from src.core.models import CompletionState, ConfigContext, ScanState, TrackedObject

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
    state: CompletionState = CompletionState.RESOLVED,
    replication_outcome: str | None = "COMPLETE",
) -> TrackedObject:
    return TrackedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
        configs=configs if configs is not None else {},
        state=state,
        replication_outcome=replication_outcome,
    )


def make_scan_state(last_scan_at: datetime, last_scan_match_count: int) -> ScanState:
    return ScanState(last_scan_at=last_scan_at, last_scan_match_count=last_scan_match_count)


# ---------------------------------------------------------------------------
# quiescence_check
# ---------------------------------------------------------------------------


class TestQuiescenceCheck:
    def test_none_scan_state_returns_false(self):
        assert quiescence_check(_MANIFEST_AT, None) is False

    def test_scan_before_manifest_generated_at_returns_false(self):
        scan_state = make_scan_state(last_scan_at=_MANIFEST_AT - timedelta(hours=1), last_scan_match_count=0)
        assert quiescence_check(_MANIFEST_AT, scan_state) is False

    def test_scan_exactly_equal_to_manifest_generated_at_returns_false(self):
        """Boundary case: scan_state.last_scan_at <= manifest_generated_at is False —
        strictly-after is required, exact equality does not count."""
        scan_state = make_scan_state(last_scan_at=_MANIFEST_AT, last_scan_match_count=0)
        assert quiescence_check(_MANIFEST_AT, scan_state) is False

    def test_scan_after_manifest_with_nonzero_matches_returns_false(self):
        scan_state = make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=1)
        assert quiescence_check(_MANIFEST_AT, scan_state) is False

    def test_scan_after_manifest_with_zero_matches_returns_true(self):
        scan_state = make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0)
        assert quiescence_check(_MANIFEST_AT, scan_state) is True


# ---------------------------------------------------------------------------
# should_publish
# ---------------------------------------------------------------------------


class TestShouldPublish:
    def test_resolved_single_config_quiescent_returns_true(self):
        obj = make_obj(state=CompletionState.RESOLVED, configs={"cfg-1": make_config_context()})
        scan_state_by_config = {
            "cfg-1": make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0)
        }
        assert should_publish(obj, scan_state_by_config) is True

    def test_pending_returns_false(self):
        obj = make_obj(
            state=CompletionState.PENDING, replication_outcome=None, configs={"cfg-1": make_config_context()}
        )
        scan_state_by_config = {
            "cfg-1": make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0)
        }
        assert should_publish(obj, scan_state_by_config) is False

    def test_resolved_but_not_quiescent_returns_false(self):
        obj = make_obj(state=CompletionState.RESOLVED, configs={"cfg-1": make_config_context()})
        scan_state_by_config: dict[str, ScanState | None] = {"cfg-1": None}
        assert should_publish(obj, scan_state_by_config) is False

    def test_two_configs_one_not_quiescent_blocks_whole_object(self):
        obj = make_obj(
            state=CompletionState.RESOLVED,
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a"),
                "cfg-b": make_config_context(replication_config_id="cfg-b"),
            },
        )
        scan_state_by_config = {
            "cfg-a": make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0),
            "cfg-b": make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=1),
        }
        assert should_publish(obj, scan_state_by_config) is False

    def test_two_configs_both_quiescent_under_different_manifest_times_returns_true(self):
        later_manifest_at = _MANIFEST_AT + timedelta(days=10)
        obj = make_obj(
            state=CompletionState.RESOLVED,
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a", manifest_generated_at=_MANIFEST_AT),
                "cfg-b": make_config_context(replication_config_id="cfg-b", manifest_generated_at=later_manifest_at),
            },
        )
        scan_state_by_config = {
            "cfg-a": make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0),
            "cfg-b": make_scan_state(
                last_scan_at=later_manifest_at + timedelta(hours=1), last_scan_match_count=0
            ),
        }
        assert should_publish(obj, scan_state_by_config) is True

    def test_missing_config_id_from_scan_state_by_config_treated_as_not_quiescent(self):
        obj = make_obj(
            state=CompletionState.RESOLVED,
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a"),
                "cfg-b": make_config_context(replication_config_id="cfg-b"),
            },
        )
        scan_state_by_config = {
            "cfg-a": make_scan_state(last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0),
        }
        assert should_publish(obj, scan_state_by_config) is False

    def test_empty_configs_resolved_returns_true_vacuously(self):
        obj = make_obj(state=CompletionState.RESOLVED, configs={})
        assert should_publish(obj, {}) is True

    def test_single_bucket_sentinel_key_resolves_against_matching_scan_state(self):
        """design.md D4/D5 (single-batch-job-per-bucket): a TrackedObject's
        single ConfigContext keyed by the per-bucket sentinel (the bucket's
        own name, per task 5.1) correctly resolves quiescence against the
        ScanState recorded under that identical sentinel key (via
        StateStore.record_scan_result, task 5.2). Confirms the "conjunction"
        reduces to a single quiescence check, not a dict-key mismatch."""
        bucket_sentinel = "my-bucket"
        obj = make_obj(
            state=CompletionState.RESOLVED,
            configs={
                bucket_sentinel: make_config_context(
                    replication_config_id=bucket_sentinel,
                    manifest_generated_at=_MANIFEST_AT,
                )
            },
        )
        scan_state_by_config = {
            bucket_sentinel: make_scan_state(
                last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0
            )
        }
        assert should_publish(obj, scan_state_by_config) is True

    def test_single_bucket_sentinel_key_not_yet_quiescent_returns_false(self):
        """Same per-bucket sentinel wiring as above, but the recorded scan
        for the bucket found nonzero matches — should_publish defers."""
        bucket_sentinel = "my-bucket"
        obj = make_obj(
            state=CompletionState.RESOLVED,
            configs={
                bucket_sentinel: make_config_context(
                    replication_config_id=bucket_sentinel,
                    manifest_generated_at=_MANIFEST_AT,
                )
            },
        )
        scan_state_by_config = {
            bucket_sentinel: make_scan_state(
                last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=3
            )
        }
        assert should_publish(obj, scan_state_by_config) is False


# ---------------------------------------------------------------------------
# Property 8: A Tracked_Object publishes exactly when it is resolved and
# every routing config is quiescent
# Feature: source-status-completion-tracking, Property 8: A Tracked_Object publishes exactly when it is resolved and every routing config is quiescent
# Validates: Requirements 4.1, 4.4, 5.4
# ---------------------------------------------------------------------------


def _config_case(quiescent: bool, config_id: str) -> tuple[ConfigContext, ScanState | None]:
    manifest_at = _MANIFEST_AT
    ctx = make_config_context(replication_config_id=config_id, manifest_generated_at=manifest_at)
    if quiescent:
        scan_state = make_scan_state(last_scan_at=manifest_at + timedelta(hours=1), last_scan_match_count=0)
    else:
        scan_state = None
    return ctx, scan_state


class TestProperty8PublishExactlyWhenResolvedAndAllQuiescent:
    """# Feature: source-status-completion-tracking, Property 8: A Tracked_Object publishes exactly when it is resolved and every routing config is quiescent

    Validates: Requirements 4.1, 4.4, 5.4
    """

    @given(
        resolved=st.booleans(),
        quiescence_flags=st.lists(st.booleans(), min_size=1, max_size=5),
    )
    @settings(max_examples=100)
    def test_should_publish_true_iff_resolved_and_all_quiescent(
        self, resolved: bool, quiescence_flags: list[bool]
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 8: A Tracked_Object publishes exactly when it is resolved and every routing config is quiescent"""
        configs: dict[str, ConfigContext] = {}
        scan_state_by_config: dict[str, ScanState | None] = {}
        for idx, quiescent in enumerate(quiescence_flags):
            config_id = f"cfg-{idx}"
            ctx, scan_state = _config_case(quiescent, config_id)
            configs[config_id] = ctx
            scan_state_by_config[config_id] = scan_state

        state = CompletionState.RESOLVED if resolved else CompletionState.PENDING
        outcome = "COMPLETE" if resolved else None
        obj = make_obj(state=state, replication_outcome=outcome, configs=configs)

        expected = resolved and all(quiescence_flags)
        assert should_publish(obj, scan_state_by_config) is expected


# ---------------------------------------------------------------------------
# Property 10: Quiescence requires a scan strictly after manifest
# generation, and is per-config independent
# Feature: source-status-completion-tracking, Property 10: Quiescence requires a scan strictly after manifest generation, and is per-config independent
# Validates: Requirements 5.1, 5.2, 5.3
# ---------------------------------------------------------------------------


class TestProperty10QuiescencePerConfigIndependent:
    """# Feature: source-status-completion-tracking, Property 10: Quiescence requires a scan strictly after manifest generation, and is per-config independent

    Validates: Requirements 5.1, 5.2, 5.3
    """

    @given(
        scan_offset_seconds=st.integers(min_value=-3600, max_value=3600),
        match_count=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=100)
    def test_quiescence_requires_strictly_after_and_zero_matches(
        self, scan_offset_seconds: int, match_count: int
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 10: Quiescence requires a scan strictly after manifest generation, and is per-config independent"""
        scan_at = _MANIFEST_AT + timedelta(seconds=scan_offset_seconds)
        scan_state = make_scan_state(last_scan_at=scan_at, last_scan_match_count=match_count)
        result = quiescence_check(_MANIFEST_AT, scan_state)
        expected = scan_at > _MANIFEST_AT and match_count == 0
        assert result is expected

    @given(
        other_config_match_count=st.integers(min_value=1, max_value=10),
        other_config_scan_offset=st.integers(min_value=1, max_value=3600),
    )
    @settings(max_examples=100)
    def test_a_different_configs_scan_never_affects_the_config_under_test(
        self, other_config_match_count: int, other_config_scan_offset: int
    ) -> None:
        """A scan for a *different* replication_config_id never changes the
        result for the config under test, including when both configs
        belong to the same TrackedObject."""
        ctx_under_test = make_config_context(replication_config_id="cfg-under-test", manifest_generated_at=_MANIFEST_AT)
        other_manifest_at = _MANIFEST_AT
        ctx_other = make_config_context(replication_config_id="cfg-other", manifest_generated_at=other_manifest_at)
        obj = make_obj(
            state=CompletionState.RESOLVED,
            configs={"cfg-under-test": ctx_under_test, "cfg-other": ctx_other},
        )

        scan_state_by_config = {
            "cfg-under-test": make_scan_state(
                last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0
            ),
            "cfg-other": make_scan_state(
                last_scan_at=other_manifest_at + timedelta(seconds=other_config_scan_offset),
                last_scan_match_count=other_config_match_count,
            ),
        }

        # quiescence_check for cfg-under-test alone is unaffected by cfg-other's scan_state.
        assert quiescence_check(
            _MANIFEST_AT, scan_state_by_config["cfg-under-test"]
        ) is True

        # But should_publish for the whole object is False, because
        # cfg-other (nonzero matches) is not quiescent — proving the
        # per-object conjunction correctly reads each config's OWN
        # scan_state rather than sharing cfg-under-test's quiescent result.
        assert should_publish(obj, scan_state_by_config) is False
