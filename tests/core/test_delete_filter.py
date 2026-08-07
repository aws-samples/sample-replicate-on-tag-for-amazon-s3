"""Tests for src/core/delete_filter.py — Tasks 11.3, 12.3.

Properties 8, 9, 11: delete classification, filter exclusion, version preservation.

Feature: large-scale-manifest-generation
Requirements: 12.1, 12.2, 12.4, 12.5, 13.1, 13.6, 13.8
"""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.delete_filter import (
    PERMANENT_DELETE_SQL_PREDICATE,
    filter_deleted_versions,
)
from src.core.models import DestinationRef, DerivedReplicationRule, MatchedObject


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEST = DestinationRef(bucket_arn="arn:aws:s3:::dst")


def _mo(key: str, version_id: str | None = "v1", config_id: str = "cfg") -> MatchedObject:
    return MatchedObject(
        source_bucket="src",
        object_key=key,
        replication_config_id=config_id,
        matched_rule_ids=frozenset(["r1"]),
        version_id=version_id,
    )


# ---------------------------------------------------------------------------
# Unit tests: filter_deleted_versions
# ---------------------------------------------------------------------------


class TestFilterDeletedVersions:
    def test_empty_permanently_deleted_keeps_all(self):
        matched = {_mo("a.txt", "v1"), _mo("b.txt", "v2")}
        kept, excluded = filter_deleted_versions(matched, set())
        assert kept == matched
        assert excluded == 0

    def test_excludes_exact_version_match(self):
        """(key, version_id) in permanently_deleted → excluded (Req 13.1)."""
        matched = {_mo("a.txt", "v1"), _mo("b.txt", "v2")}
        kept, excluded = filter_deleted_versions(matched, {("a.txt", "v1")})
        assert excluded == 1
        assert _mo("b.txt", "v2") in kept
        assert _mo("a.txt", "v1") not in kept

    def test_soft_delete_key_not_excluded(self):
        """Soft_Delete absent from permanently_deleted → version retained (Req 12.5)."""
        matched = {_mo("soft.txt", "v3")}
        # soft_delete is TRUE → not in permanently_deleted set
        kept, excluded = filter_deleted_versions(matched, set())
        assert len(kept) == 1
        assert excluded == 0

    def test_null_version_excluded_by_null_pair(self):
        """(key, None) in permanently_deleted excludes null-version object (Req 12.2)."""
        matched = {_mo("null-key.txt", None)}
        kept, excluded = filter_deleted_versions(matched, {("null-key.txt", None)})
        assert excluded == 1
        assert not kept

    def test_null_key_does_not_exclude_versioned_object(self):
        """(key, None) in set does NOT exclude an object with a real version_id (Req 12.2)."""
        matched = {_mo("key.txt", "v5")}
        kept, excluded = filter_deleted_versions(matched, {("key.txt", None)})
        assert excluded == 0
        assert _mo("key.txt", "v5") in kept

    def test_versioned_delete_does_not_exclude_null_version(self):
        """(key, 'v1') in set does NOT exclude a null-version object (Req 12.2)."""
        matched = {_mo("key.txt", None)}
        kept, excluded = filter_deleted_versions(matched, {("key.txt", "v1")})
        assert excluded == 0
        assert _mo("key.txt", None) in kept

    def test_all_excluded_returns_empty_set(self):
        """All matched objects excluded → (empty_set, len(matched))."""
        matched = {_mo("a.txt", "v1"), _mo("b.txt", "v2")}
        perm_deleted = {("a.txt", "v1"), ("b.txt", "v2")}
        kept, excluded = filter_deleted_versions(matched, perm_deleted)
        assert not kept
        assert excluded == 2

    def test_version_id_preserved_in_survivor(self):
        """Surviving objects keep their version_id unchanged (Req 13.6)."""
        matched = {_mo("kept.txt", "exact-version-xyz")}
        kept, _ = filter_deleted_versions(matched, set())
        survivor = next(iter(kept))
        assert survivor.version_id == "exact-version-xyz"

    def test_null_version_id_preserved_in_survivor(self):
        """Surviving null-version object keeps version_id=None (Req 13.6)."""
        matched = {_mo("null-obj.txt", None)}
        kept, _ = filter_deleted_versions(matched, set())
        survivor = next(iter(kept))
        assert survivor.version_id is None

    def test_excluded_count_equals_removed_count(self):
        """excluded_count == number of objects removed (Req 13.8)."""
        matched = {_mo(f"k{i}.txt", f"v{i}") for i in range(10)}
        perm_deleted = {(f"k{i}.txt", f"v{i}") for i in range(3)}
        kept, excluded = filter_deleted_versions(matched, perm_deleted)
        assert excluded == 3
        assert len(kept) == 7


# ---------------------------------------------------------------------------
# Property 9: Exclusion is exactly the permanently deleted versions (null-safe)
# Feature: large-scale-manifest-generation, Property 9: Exclusion is exactly the permanently deleted versions (null-safe)
# Requirements: 12.2, 12.4, 12.5, 13.1, 13.8
# ---------------------------------------------------------------------------

_version_id_st = st.one_of(st.none(), st.text(min_size=1, max_size=30))


@given(
    keys=st.lists(st.text(min_size=1, max_size=20), min_size=0, max_size=20, unique=True),
    perm_deleted_indices=st.lists(
        st.integers(min_value=0, max_value=19),
        min_size=0, max_size=20, unique=True,
    ),
)
@settings(max_examples=100)
def test_property_9_exclusion_exactly_permanently_deleted(
    keys: list[str],
    perm_deleted_indices: list[int],
) -> None:
    """Property 9: kept = matched - permanently_deleted (null-safe).

    # Feature: large-scale-manifest-generation, Property 9: Exclusion is exactly the permanently deleted versions (null-safe)
    """
    objects = [_mo(f"k{i}.txt", f"vid{i}" if i % 3 != 0 else None) for i in range(len(keys))]
    matched: set[MatchedObject] = set(objects)
    perm_pairs = {(objects[i].object_key, objects[i].version_id) for i in perm_deleted_indices if i < len(objects)}

    kept, excluded = filter_deleted_versions(matched, perm_pairs)

    # excluded_count is correct
    expected_excluded = len(perm_pairs & {(o.object_key, o.version_id) for o in matched})
    assert excluded == expected_excluded

    # kept is exactly those not in perm_pairs
    for obj in kept:
        assert (obj.object_key, obj.version_id) not in perm_pairs

    for obj in matched:
        if (obj.object_key, obj.version_id) not in perm_pairs:
            assert obj in kept


# ---------------------------------------------------------------------------
# Property 11: Surviving version ids are preserved
# Feature: large-scale-manifest-generation, Property 11: Surviving version ids are preserved
# Requirements: 13.6
# ---------------------------------------------------------------------------


@given(
    versions=st.lists(
        st.one_of(st.none(), st.text(min_size=1, max_size=40)),
        min_size=1, max_size=30,
    )
)
@settings(max_examples=100)
def test_property_11_surviving_version_ids_preserved(
    versions: list[str | None],
) -> None:
    """Property 11: survivors keep their version_id including None (Req 13.6).

    # Feature: large-scale-manifest-generation, Property 11: Surviving version ids are preserved
    """
    objects = [_mo(f"obj{i}.dat", v) for i, v in enumerate(versions)]
    matched: set[MatchedObject] = set(objects)
    # No deletions → all survive
    kept, excluded = filter_deleted_versions(matched, set())
    assert excluded == 0
    assert len(kept) == len(objects)

    for obj in kept:
        # Find the original
        orig = next(o for o in objects if o.object_key == obj.object_key)
        assert obj.version_id == orig.version_id
