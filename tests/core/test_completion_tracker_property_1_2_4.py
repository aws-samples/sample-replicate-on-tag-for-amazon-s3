"""Property tests 1, 2, and 4 for ``create_pending_tracked_object_updates`` (tasks 2.2, 2.3, 2.4).

Feature: source-status-completion-tracking.
"""
from __future__ import annotations

from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import create_pending_tracked_object_updates
from src.core.models import CompletionState, ConfigContext, ManifestEntry, TrackedObject

_MANIFEST_GENERATED_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

_TERMINAL_STATUSES = ("Complete", "Failed", "Cancelled")

_object_keys = st.text(min_size=1, max_size=30).filter(lambda s: "\x00" not in s)
_version_ids = st.one_of(st.none(), st.text(min_size=1, max_size=20))


def _entries_strategy(min_size: int = 1, max_size: int = 8):
    return st.lists(
        st.tuples(_object_keys, _version_ids),
        min_size=min_size,
        max_size=max_size,
        unique=True,
    ).map(
        lambda pairs: [
            ManifestEntry(source_bucket="my-bucket", object_key=key, version_id=version_id)
            for key, version_id in pairs
        ]
    )


# ---------------------------------------------------------------------------
# Property 1: A terminal job's report creates one PENDING Config_Context per
# listed object version, regardless of status
# Feature: source-status-completion-tracking, Property 1: A terminal job's report creates one PENDING Config_Context per listed object version, regardless of status
# Validates: Requirements 2.1, 2.2, 2.5, 2.6
# ---------------------------------------------------------------------------


class TestProperty1CreatesOneConfigContextPerListedVersion:
    """# Feature: source-status-completion-tracking, Property 1: A terminal job's report creates one PENDING Config_Context per listed object version, regardless of status

    Validates: Requirements 2.1, 2.2, 2.5, 2.6
    """

    @given(
        entries=_entries_strategy(min_size=1, max_size=8),
        terminal_status=st.sampled_from(_TERMINAL_STATUSES),
        replication_config_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
        job_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
    )
    @settings(max_examples=100)
    def test_one_config_context_per_entry_regardless_of_terminal_status(
        self,
        entries: list[ManifestEntry],
        terminal_status: str,
        replication_config_id: str,
        job_id: str,
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 1: A terminal job's report creates one PENDING Config_Context per listed object version, regardless of status"""
        # The function takes no status parameter — usable for any terminal
        # status; we just confirm the terminal_status value itself has no
        # bearing on the outcome (it is never read).
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id=replication_config_id,
            job_id=job_id,
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )

        assert len(updates) == len(entries)
        for entry in entries:
            key = (entry.object_key, entry.version_id)
            assert key in updates
            ctx = updates[key]
            assert isinstance(ctx, ConfigContext)
            assert ctx.replication_config_id == replication_config_id
            assert ctx.job_id == job_id
            assert ctx.manifest_generated_at == _MANIFEST_GENERATED_AT
            assert ctx.bops_confirmed is True

    @given(
        entries=_entries_strategy(min_size=1, max_size=5),
        other_config_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
        other_job_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
        new_config_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
        new_job_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
    )
    @settings(max_examples=100)
    def test_merging_adds_alongside_existing_config_context_rather_than_overwriting(
        self,
        entries: list[ManifestEntry],
        other_config_id: str,
        other_job_id: str,
        new_config_id: str,
        new_job_id: str,
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 1: A terminal job's report creates one PENDING Config_Context per listed object version, regardless of status

        Simulates what the state store's merge (task 10.1) does with this
        function's output: merging a new ConfigContext into a TrackedObject
        that already has a Config_Context from a DIFFERENT
        replication_config_id's job adds the new one alongside it, rather
        than creating a duplicate TrackedObject or overwriting the existing
        entry.
        """
        if other_config_id == new_config_id:
            new_config_id = new_config_id + "-x"

        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id=new_config_id,
            job_id=new_job_id,
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )

        for entry in entries:
            key = (entry.object_key, entry.version_id)
            # Pre-existing TrackedObject from a sibling rule's job.
            existing_obj = TrackedObject(
                source_bucket="my-bucket",
                object_key=entry.object_key,
                version_id=entry.version_id,
                configs={
                    other_config_id: ConfigContext(
                        replication_config_id=other_config_id,
                        job_id=other_job_id,
                        manifest_generated_at=_MANIFEST_GENERATED_AT,
                        bops_confirmed=True,
                    )
                },
                state=CompletionState.PENDING,
            )
            new_ctx = updates[key]
            # Simulate the merge (state store's responsibility).
            merged_configs = dict(existing_obj.configs)
            merged_configs[new_config_id] = new_ctx
            merged_obj = TrackedObject(
                source_bucket=existing_obj.source_bucket,
                object_key=existing_obj.object_key,
                version_id=existing_obj.version_id,
                configs=merged_configs,
                state=existing_obj.state,
            )

            assert set(merged_obj.configs.keys()) == {other_config_id, new_config_id}
            assert merged_obj.configs[other_config_id].job_id == other_job_id
            assert merged_obj.configs[new_config_id].job_id == new_job_id


# ---------------------------------------------------------------------------
# Property 2: Null version_id maps to the null-version marker
# Feature: source-status-completion-tracking, Property 2: Null version_id maps to the null-version marker
# Validates: Requirements 2.3
# ---------------------------------------------------------------------------


class TestProperty2NullVersionIdMapsToNullVersionMarker:
    """# Feature: source-status-completion-tracking, Property 2: Null version_id maps to the null-version marker

    Validates: Requirements 2.3
    """

    @given(
        object_key=_object_keys,
        replication_config_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
        job_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
    )
    @settings(max_examples=100)
    def test_none_version_id_never_coerced(
        self, object_key: str, replication_config_id: str, job_id: str
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 2: Null version_id maps to the null-version marker"""
        entry = ManifestEntry(source_bucket="my-bucket", object_key=object_key, version_id=None)
        updates = create_pending_tracked_object_updates(
            entries=[entry],
            replication_config_id=replication_config_id,
            job_id=job_id,
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        key = (object_key, None)
        assert key in updates
        # Never coerced to an empty string or other placeholder.
        assert key[1] is None
        assert not isinstance(key[1], str)


# ---------------------------------------------------------------------------
# Property 4: PENDING records are created only from object versions listed
# in a BOPS completion report
# Feature: source-status-completion-tracking, Property 4: PENDING records are created only from object versions listed in a BOPS completion report
# Validates: Requirements 2.7
# ---------------------------------------------------------------------------


class TestProperty4OnlyListedVersionsProduceUpdates:
    """# Feature: source-status-completion-tracking, Property 4: PENDING records are created only from object versions listed in a BOPS completion report

    Validates: Requirements 2.7
    """

    @given(
        listed_entries=_entries_strategy(min_size=0, max_size=8),
        unlisted_keys=st.lists(
            st.tuples(_object_keys, _version_ids), min_size=0, max_size=5, unique=True
        ),
        replication_config_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
        job_id=st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True),
    )
    @settings(max_examples=100)
    def test_every_update_corresponds_to_a_listed_entry_and_no_others(
        self,
        listed_entries: list[ManifestEntry],
        unlisted_keys: list[tuple[str, str | None]],
        replication_config_id: str,
        job_id: str,
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 4: PENDING records are created only from object versions listed in a BOPS completion report"""
        listed_key_set = {(e.object_key, e.version_id) for e in listed_entries}
        # Remove any accidental overlap so unlisted_keys is genuinely disjoint.
        unlisted_key_set = {k for k in unlisted_keys if k not in listed_key_set}

        updates = create_pending_tracked_object_updates(
            entries=listed_entries,
            replication_config_id=replication_config_id,
            job_id=job_id,
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )

        # Every produced update corresponds to a listed entry.
        assert set(updates.keys()) == listed_key_set

        # No unlisted version produces an update.
        for key in unlisted_key_set:
            assert key not in updates
