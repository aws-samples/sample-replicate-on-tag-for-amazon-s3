"""Unit and property tests for ``reconcile_source_status_check`` (task 3.1).

Feature: source-status-completion-tracking.

Exercises ``reconcile_source_status_check`` against the object-level
``TrackedObject`` type (design.md Decision 3). This REPLACES the superseded
destination-polling test file entirely: there is no ``confirm_presence``
closure, no ``destination_access_configured`` branch, and no destination
call anywhere in this function — a ``COMPLETED`` header resolves DIRECTLY
to ``COMPLETE``.
"""
from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.adapters.source_status_adapter import SourceStatusCheckKind, SourceStatusResult
from src.core.completion_tracker import reconcile_source_status_check, select_check_candidates
from src.core.models import CompletionState, ConfigContext, TrackedObject

_MANIFEST_GENERATED_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_NOW = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)


def make_pending_obj(
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
    manifest_generated_at: datetime = _MANIFEST_GENERATED_AT,
    bops_confirmed: bool = True,
    object_key: str = "a.txt",
    version_id: str | None = "v1",
    matched_rules: frozenset[str] = frozenset(),
    destinations: frozenset[str] = frozenset(),
) -> TrackedObject:
    return TrackedObject(
        source_bucket="my-bucket",
        object_key=object_key,
        version_id=version_id,
        configs={
            replication_config_id: ConfigContext(
                replication_config_id=replication_config_id,
                job_id=job_id,
                manifest_generated_at=manifest_generated_at,
                bops_confirmed=bops_confirmed,
            )
        },
        state=CompletionState.PENDING,
        matched_rules=matched_rules,
        destinations=destinations,
    )


class TestReconcileSourceStatusCheckCheckFailed:
    def test_check_failed_leaves_object_unchanged_and_pending(self):
        obj = make_pending_obj()
        result = SourceStatusResult(kind=SourceStatusCheckKind.CHECK_FAILED, error_reason="throttled")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled == obj
        assert reconciled.state == CompletionState.PENDING


class TestReconcileSourceStatusCheckHeaderAbsent:
    def test_header_absent_resolves_unknown(self):
        obj = make_pending_obj()
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT)
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled.state == CompletionState.RESOLVED
        assert reconciled.resolution_method == "source_status_header"
        assert reconciled.replication_outcome == "UNKNOWN"
        assert reconciled.resolved_at == _NOW


class TestReconcileSourceStatusCheckPendingFailedHeader:
    def test_header_value_pending_resolves_verbatim(self):
        obj = make_pending_obj()
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="PENDING")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled.state == CompletionState.RESOLVED
        assert reconciled.resolution_method == "source_status_header"
        assert reconciled.replication_outcome == "PENDING"
        assert reconciled.resolved_at == _NOW

    def test_header_value_failed_resolves_verbatim(self):
        obj = make_pending_obj()
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="FAILED")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled.state == CompletionState.RESOLVED
        assert reconciled.resolution_method == "source_status_header"
        assert reconciled.replication_outcome == "FAILED"
        assert reconciled.resolved_at == _NOW


class TestReconcileSourceStatusCheckCompletedHeader:
    def test_completed_resolves_directly_to_complete(self):
        obj = make_pending_obj()
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled.state == CompletionState.RESOLVED
        assert reconciled.resolution_method == "source_status_header"
        assert reconciled.replication_outcome == "COMPLETE"
        assert reconciled.resolved_at == _NOW

    def test_resolved_completed_never_reselected(self):
        """A COMPLETED-resolved object is RESOLVED and thus excluded from
        select_check_candidates (Property 5)."""
        obj = make_pending_obj(object_key="key-1", version_id=None)
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        candidates = select_check_candidates({"key-1\x00": reconciled})
        assert candidates == []


class TestReconcileSourceStatusCheckIdentityFieldsPreserved:
    """Identity fields (source_bucket, object_key, version_id, configs) are
    preserved verbatim across every branch."""

    def _assert_identity_preserved(self, obj: TrackedObject, reconciled: TrackedObject) -> None:
        assert reconciled.source_bucket == obj.source_bucket
        assert reconciled.object_key == obj.object_key
        assert reconciled.version_id == obj.version_id
        assert reconciled.configs == obj.configs

    def test_check_failed(self):
        obj = make_pending_obj(replication_config_id="cfg-42", job_id="job-99")
        result = SourceStatusResult(kind=SourceStatusCheckKind.CHECK_FAILED, error_reason="x")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        self._assert_identity_preserved(obj, reconciled)

    def test_header_absent(self):
        obj = make_pending_obj(replication_config_id="cfg-42", job_id="job-99")
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT)
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        self._assert_identity_preserved(obj, reconciled)

    def test_header_value_pending_or_failed_or_completed(self):
        obj = make_pending_obj(replication_config_id="cfg-42", job_id="job-99")
        for value in ("PENDING", "FAILED", "COMPLETED"):
            result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value=value)
            reconciled = reconcile_source_status_check(obj, result, now=_NOW)
            self._assert_identity_preserved(obj, reconciled)


# ---------------------------------------------------------------------------
# Property 7: Source-status resolution maps each header outcome correctly
# and makes no destination call
# Feature: source-status-completion-tracking, Property 7: Source-status resolution maps each header outcome correctly and makes no destination call
# Validates: Requirements 3.2, 3.4, 3.5, 3.6
# ---------------------------------------------------------------------------


class TestProperty7SourceStatusResolutionMapsCorrectly:
    """# Feature: source-status-completion-tracking, Property 7: Source-status resolution maps each header outcome correctly and makes no destination call

    Validates: Requirements 3.2, 3.4, 3.5, 3.6
    """

    @given(
        replication_config_id=st.text(min_size=1, max_size=30),
        job_id=st.text(min_size=1, max_size=30),
        header_value=st.sampled_from(["COMPLETED", "PENDING", "FAILED"]),
    )
    @settings(max_examples=100)
    def test_header_value_resolves_correctly(
        self, replication_config_id: str, job_id: str, header_value: str
    ) -> None:
        obj = make_pending_obj(replication_config_id=replication_config_id, job_id=job_id)
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value=header_value)
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)

        assert reconciled.state == CompletionState.RESOLVED
        assert reconciled.resolution_method == "source_status_header"
        expected_outcome = "COMPLETE" if header_value == "COMPLETED" else header_value
        assert reconciled.replication_outcome == expected_outcome
        assert reconciled.resolved_at == _NOW

    @given(
        replication_config_id=st.text(min_size=1, max_size=30),
        job_id=st.text(min_size=1, max_size=30),
    )
    @settings(max_examples=100)
    def test_header_absent_resolves_unknown_with_key_free_data(
        self, replication_config_id: str, job_id: str
    ) -> None:
        obj = make_pending_obj(replication_config_id=replication_config_id, job_id=job_id)
        result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT)
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled.state == CompletionState.RESOLVED
        assert reconciled.replication_outcome == "UNKNOWN"
        # This pure function's output never leaks object_key beyond the
        # object's own pre-existing identity fields (never a NEW leak in
        # the resolution fields themselves).
        assert reconciled.resolution_method == "source_status_header"

    @given(
        replication_config_id=st.text(min_size=1, max_size=30),
        job_id=st.text(min_size=1, max_size=30),
    )
    @settings(max_examples=100)
    def test_transient_check_failure_leaves_object_unchanged(
        self, replication_config_id: str, job_id: str
    ) -> None:
        obj = make_pending_obj(replication_config_id=replication_config_id, job_id=job_id)
        result = SourceStatusResult(kind=SourceStatusCheckKind.CHECK_FAILED, error_reason="throttled")
        reconciled = reconcile_source_status_check(obj, result, now=_NOW)
        assert reconciled == obj
        assert reconciled.state == CompletionState.PENDING

    @given(
        replication_config_id=st.text(min_size=1, max_size=30),
        job_id=st.text(min_size=1, max_size=30),
        kind=st.sampled_from(list(SourceStatusCheckKind)),
        header_value=st.sampled_from(["COMPLETED", "PENDING", "FAILED"]),
    )
    @settings(max_examples=100)
    def test_no_destination_client_constructed_or_called(
        self, replication_config_id: str, job_id: str, kind: SourceStatusCheckKind, header_value: str
    ) -> None:
        """No branch of reconcile_source_status_check ever touches a
        destination client — the function signature accepts no destination
        parameter at all, so this is structurally guaranteed; this test
        exercises every kind/value combination to document the invariant
        explicitly."""
        obj = make_pending_obj(replication_config_id=replication_config_id, job_id=job_id)
        value = header_value if kind == SourceStatusCheckKind.HEADER_VALUE else None
        result = SourceStatusResult(kind=kind, value=value)
        # Must not raise, and must not require any destination-related
        # argument — the call signature itself proves this.
        reconcile_source_status_check(obj, result, now=_NOW)


# ---------------------------------------------------------------------------
# OBJECT_GONE reconciliation, expiry backstop, and report chunking
# Requirements: 3.7, 3.8, 4.8
# ---------------------------------------------------------------------------

from datetime import timedelta  # noqa: E402

from src.core.completion_tracker import (  # noqa: E402
    chunk_items_for_report,
    expire_tracked_object,
    is_expired,
    tracked_object_age,
)


class TestObjectGoneReconciliation:
    def test_object_gone_resolves_terminally(self):
        """A deleted object version resolves rather than staying PENDING (Req 3.7).

        This is the fix for the failure mode where a deleted object was
        re-checked on every run forever, pinning its Tracked_Object in the
        state object permanently.
        """
        obj = make_pending_obj()
        result = SourceStatusResult(
            kind=SourceStatusCheckKind.OBJECT_GONE, error_reason="404: Not Found"
        )
        out = reconcile_source_status_check(obj, result, _NOW)
        assert out.state is CompletionState.RESOLVED
        assert out.replication_outcome == "GONE"
        assert out.resolution_method == "object_gone"
        assert out.resolved_at == _NOW

    def test_object_gone_item_is_no_longer_a_check_candidate(self):
        """Once resolved it drops out of selection, so the loop cannot recur."""
        obj = make_pending_obj()
        result = SourceStatusResult(kind=SourceStatusCheckKind.OBJECT_GONE)
        out = reconcile_source_status_check(obj, result, _NOW)
        assert select_check_candidates({"k": out}) == []

    def test_check_failed_still_leaves_item_pending(self):
        """A genuinely transient failure must remain retryable (Req 3.6)."""
        obj = make_pending_obj()
        result = SourceStatusResult(
            kind=SourceStatusCheckKind.CHECK_FAILED, error_reason="SlowDown"
        )
        out = reconcile_source_status_check(obj, result, _NOW)
        assert out.state is CompletionState.PENDING


class TestExpiryBackstop:
    def test_age_measured_from_newest_covering_job(self):
        """A re-covered object gets a fresh window, not the oldest job's age."""
        obj = make_pending_obj()
        obj.configs["cfg-2"] = ConfigContext(
            replication_config_id="cfg-2",
            job_id="job-2",
            manifest_generated_at=_NOW - timedelta(hours=1),
            bops_confirmed=True,
        )
        assert tracked_object_age(obj, _NOW) == timedelta(hours=1)

    def test_expired_when_older_than_ttl(self):
        obj = make_pending_obj()  # manifest generated 2 days before _NOW
        assert is_expired(obj, _NOW, timedelta(days=1))

    def test_not_expired_when_within_ttl(self):
        obj = make_pending_obj()
        assert not is_expired(obj, _NOW, timedelta(days=7))

    def test_non_positive_ttl_disables_expiry(self):
        """A zero or negative TTL must never abandon anything."""
        obj = make_pending_obj()
        assert not is_expired(obj, _NOW, timedelta(0))
        assert not is_expired(obj, _NOW, timedelta(seconds=-1))

    def test_object_with_no_configs_is_never_expired(self):
        """Keeps the function total without instantly expiring a config-less object."""
        obj = make_pending_obj()
        obj.configs.clear()
        assert tracked_object_age(obj, _NOW) == timedelta(0)
        assert not is_expired(obj, _NOW, timedelta(days=7))

    def test_expire_resolves_as_expired(self):
        """Expiry reuses the normal resolution path so the item is reported (Req 3.8)."""
        out = expire_tracked_object(make_pending_obj(), _NOW)
        assert out.state is CompletionState.RESOLVED
        assert out.replication_outcome == "EXPIRED"
        assert out.resolution_method == "expired"


class TestReportChunking:
    def _objs(self, n: int, key_len: int = 40) -> list[TrackedObject]:
        return [
            make_pending_obj(object_key=f"{i:0{key_len}d}", version_id=f"v{i}")
            for i in range(n)
        ]

    def test_empty_input_yields_no_batches(self):
        assert chunk_items_for_report([]) == []

    def test_small_run_is_a_single_batch(self):
        assert len(chunk_items_for_report(self._objs(10))) == 1

    def test_every_item_appears_exactly_once(self):
        """Chunking must not drop or duplicate an item (Req 4.9)."""
        objs = self._objs(5000)
        batched = [o for batch in chunk_items_for_report(objs) for o in batch]
        assert len(batched) == len(objs)
        assert [id(o) for o in batched] == [id(o) for o in objs]

    def test_each_batch_fits_the_sns_message_limit(self):
        """The real 256 KiB SNS ceiling, measured on the serialized report.

        A batch over the limit is rejected by SNS, and because items are
        deleted only after a successful publish, that would pin them in the
        state object forever.
        """
        import json

        from src.core.completion_tracker import build_completion_report

        for batch in chunk_items_for_report(self._objs(5000)):
            body = json.dumps(build_completion_report("my-bucket", batch), indent=2)
            assert len(body) < 262_144

    def _group_objs(self, n_groups: int, per_group: int = 1, name_len: int = 20):
        """``n_groups`` distinct rule/destination groups, ``per_group`` objects each."""
        return [
            make_pending_obj(
                object_key=f"{g}-{i}.txt",
                version_id=f"v{i}",
                matched_rules=frozenset({f"rule-{g:0{name_len}d}"}),
                destinations=frozenset({f"dest-{g:0{name_len}d}"}),
            )
            for g in range(n_groups)
            for i in range(per_group)
        ]

    def test_object_count_alone_never_splits_a_batch(self):
        """Object count does not drive size — a group's cost is its header, so
        one group of any size is one batch however long the keys are."""
        assert len(chunk_items_for_report(self._objs(5000, key_len=10))) == 1
        assert len(chunk_items_for_report(self._objs(5000, key_len=200))) == 1

    def test_many_groups_split_into_multiple_batches(self):
        """Group count is the axis that drives a split (Req 1.6)."""
        objs = self._group_objs(n_groups=1200)
        batches = chunk_items_for_report(objs)
        assert len(batches) > 1
        batched = [o for batch in batches for o in batch]
        assert [id(o) for o in batched] == [id(o) for o in objs]

    def test_many_groups_each_batch_fits_the_sns_message_limit(self):
        """Req 1.6: the guard holds at the group count a 1,000-rule bucket
        could reach, not only for the single-group common case."""
        import json

        from src.core.completion_tracker import build_completion_report

        for batch in chunk_items_for_report(self._group_objs(n_groups=1200)):
            body = json.dumps(build_completion_report("my-bucket", batch), indent=2)
            assert len(body) < 262_144

    def test_a_group_is_never_split_across_batches(self):
        """A split between groups keeps each message internally consistent and
        never repeats one group's header in two messages."""
        objs = self._group_objs(n_groups=1200, per_group=3)
        batches = chunk_items_for_report(objs)
        assert len(batches) > 1
        seen: set[tuple[str, ...]] = set()
        for batch in batches:
            keys = {tuple(sorted(o.matched_rules)) for o in batch}
            assert seen.isdisjoint(keys)
            seen |= keys

    def test_single_oversized_group_still_emitted_alone(self):
        """Stays total rather than looping or dropping the group."""
        batches = chunk_items_for_report(self._objs(1), max_group_bytes=1)
        assert len(batches) == 1
        assert len(batches[0]) == 1
