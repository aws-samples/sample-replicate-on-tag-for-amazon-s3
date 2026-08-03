"""Unit tests for ``create_pending_tracked_object_updates`` (task 2.1).

Feature: source-status-completion-tracking.

Exercises ``create_pending_tracked_object_updates`` against the
``ConfigContext`` type (design.md Decision 2, 5). This REPLACES the
superseded ``create_pending_completion_item_updates`` test file, which
produced ``DestinationOutcome``s carrying ``terminal_at``/``state``.

Property tests 1, 2, and 4 (tasks 2.2/2.3/2.4) are implemented in
``test_completion_tracker_property_1_2_4.py``.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.core.completion_tracker import create_pending_tracked_object_updates
from src.core.models import ConfigContext, ManifestEntry

_MANIFEST_GENERATED_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def make_entry(
    source_bucket: str = "my-bucket",
    object_key: str = "path/to/object.txt",
    version_id: str | None = "v1",
) -> ManifestEntry:
    return ManifestEntry(source_bucket=source_bucket, object_key=object_key, version_id=version_id)


class TestCreatePendingTrackedObjectUpdates:
    def test_one_update_per_entry(self):
        entries = [
            make_entry(object_key="a.txt", version_id="v1"),
            make_entry(object_key="b.txt", version_id="v2"),
            make_entry(object_key="c.txt", version_id="v3"),
        ]
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        assert len(updates) == 3

    def test_keyed_by_object_key_and_version_id(self):
        entries = [make_entry(object_key="a.txt", version_id="v1")]
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        assert set(updates.keys()) == {("a.txt", "v1")}

    def test_all_updates_are_config_contexts_confirmed(self):
        entries = [make_entry(object_key="a.txt"), make_entry(object_key="b.txt")]
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        for ctx in updates.values():
            assert isinstance(ctx, ConfigContext)
            assert ctx.bops_confirmed is True

    def test_identity_and_timestamp_fields_carried_verbatim(self):
        entries = [make_entry(object_key="a.txt", version_id="v1")]
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-42",
            job_id="job-99",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        ctx = updates[("a.txt", "v1")]
        assert ctx.replication_config_id == "cfg-42"
        assert ctx.job_id == "job-99"
        assert ctx.manifest_generated_at == _MANIFEST_GENERATED_AT

    def test_none_version_id_preserved_as_none_key_component(self):
        entries = [make_entry(object_key="unversioned.txt", version_id=None)]
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        assert ("unversioned.txt", None) in updates
        key = next(k for k in updates if k[0] == "unversioned.txt")
        assert key[1] is None
        assert not isinstance(key[1], str)

    def test_duplicate_object_keys_with_different_version_ids_produce_distinct_entries(self):
        entries = [
            make_entry(object_key="same.txt", version_id="v1"),
            make_entry(object_key="same.txt", version_id="v2"),
        ]
        updates = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        assert set(updates.keys()) == {("same.txt", "v1"), ("same.txt", "v2")}

    def test_usable_for_any_terminal_status_since_function_takes_none(self):
        """The function accepts no status parameter, so it is trivially
        usable regardless of which terminal DescribeJob status (Complete,
        Failed, Cancelled) the caller observed — the caller decides *when*
        to invoke it, not this function."""
        entries = [make_entry(object_key="a.txt")]
        updates_one = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        updates_two = create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        assert updates_one.keys() == updates_two.keys()

    def test_empty_entries_returns_empty_mapping(self):
        updates = create_pending_tracked_object_updates(
            entries=[],
            replication_config_id="cfg-1",
            job_id="job-1",
            manifest_generated_at=_MANIFEST_GENERATED_AT,
        )
        assert updates == {}
