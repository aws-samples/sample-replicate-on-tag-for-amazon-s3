"""Tests for src/core/manifest_generator.py — tasks 6.2, 6.3, 6.4, 6.5.

Property-based and unit tests:
  - Property 4: Manifest deduplication and round-trip — read(write(S)) == distinct(S),
    each (source bucket, object key) pair exactly once, serialized in comma-separated form.
  - Property 5: At-most-one job per configuration with consolidation — exactly one
    manifest per configuration with ≥1 match, zero for empty configurations.
  - Property 11: Tolerance to duplicate journal records — the pure pipeline
    dedup → match → manifest produces the same result as processing the de-duplicated
    set once; no object appears twice per configuration.
  - Unit tests: zero-match interval produces no manifest plus a "no matches" indication;
    incomplete manifest write behavior.

Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 8.1, 8.3, 9.2
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from src.core.manifest_generator import (
    ManifestGenerator,
    ManifestResult,
    deserialize,
    serialize,
)
from src.core.models import ManifestEntry, MatchedObject

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)


def make_matched(
    source_bucket: str = "src-bucket",
    object_key: str = "path/obj.txt",
    config_id: str = "cfg-1",
    rule_ids: tuple[str, ...] = ("rule-1",),
    version_id: Optional[str] = None,
    destination_arns: tuple[str, ...] = (),
) -> MatchedObject:
    return MatchedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        replication_config_id=config_id,
        matched_rule_ids=frozenset(rule_ids),
        version_id=version_id,
        destination_bucket_arns=frozenset(destination_arns),
    )


def make_entry(
    source_bucket: str = "src-bucket",
    object_key: str = "path/obj.txt",
    version_id: Optional[str] = None,
) -> ManifestEntry:
    return ManifestEntry(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
    )


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Object keys: any non-empty string; include edge cases like commas, slashes.
# Exclude control characters (Cc) and surrogates (Cs) which can't be URL-encoded.
_KEY_ST = st.text(
    alphabet=st.characters(blacklist_categories=("Cc", "Cs")),
    min_size=1,
    max_size=50,
)
# Bucket names: simple lowercase.
_BUCKET_ST = st.from_regex(r"^[a-z][a-z0-9\-]{2,20}$", fullmatch=True)
# Config IDs: simple strings.
_CONFIG_ID_ST = st.from_regex(r"^cfg-[a-z0-9]{3,10}$", fullmatch=True)


def _entry_strategy() -> st.SearchStrategy[ManifestEntry]:
    return st.builds(
        ManifestEntry,
        source_bucket=_BUCKET_ST,
        object_key=_KEY_ST,
        version_id=st.none(),
    )


def _matched_object_strategy(
    config_id: str = "cfg-1",
    bucket: str = "src-bucket",
) -> st.SearchStrategy[MatchedObject]:
    return st.builds(
        MatchedObject,
        source_bucket=st.just(bucket),
        object_key=_KEY_ST,
        replication_config_id=st.just(config_id),
        matched_rule_ids=st.just(frozenset(["rule-1"])),
        version_id=st.none(),
    )


# ---------------------------------------------------------------------------
# ManifestEntry CSV serialization helpers (Req 6.4, 6.5)
# ---------------------------------------------------------------------------


class TestManifestEntrySerialization:
    def test_to_csv_row_format(self):
        entry = make_entry(source_bucket="my-bucket", object_key="path/to/obj.txt")
        row = entry.to_csv_row()
        assert row.startswith("my-bucket,")
        assert "path/to/obj.txt" in row

    def test_versioned_round_trip(self):
        entry = make_entry(source_bucket="my-bucket", object_key="path/to/obj.txt")
        restored = ManifestEntry.from_versioned_csv_row(entry.to_csv_row())
        assert restored.source_bucket == entry.source_bucket
        assert restored.object_key == entry.object_key

    def test_comma_in_key_is_encoded(self):
        entry = make_entry(object_key="path,with,commas.txt")
        row = entry.to_csv_row()
        # The key should be URL-encoded so the comma becomes %2C.
        assert "%2C" in row

    def test_newline_in_key_is_encoded(self):
        entry = make_entry(object_key="path\nwith\nnewlines.txt")
        row = entry.to_csv_row()
        assert "%0A" in row

    def test_slash_in_key_preserved(self):
        entry = make_entry(object_key="a/b/c.txt")
        row = entry.to_csv_row()
        assert "a/b/c.txt" in row


class TestModuleSerializeNullVersionRegression:
    """Regression tests for the in-memory manifest path's counterpart of the
    unload_generator.py null-version-last-row bug: manifest_generator.serialize
    always produces 3-field rows (matching ManifestGenerator.finalize's
    all_versioned=True, the orchestrator's actual call site), so a null
    version_id must still produce a 3-field row, not a short 2-field row."""

    def test_serialize_null_version_entry_produces_three_fields(self):
        entries = [make_entry(object_key="null-ver.txt", version_id=None)]
        csv = serialize(entries)
        assert csv == "src-bucket,null-ver.txt,null"
        assert csv.count(",") == 2

    def test_serialize_mixed_entries_all_rows_have_three_fields(self):
        """A manifest with both versioned and null-version entries — every
        row must have exactly 3 comma-separated fields, including the
        null-version one, matching the real failure mode where a manifest's
        rows had inconsistent field counts."""
        entries = [
            make_entry(object_key="a.txt", version_id="v1"),
            make_entry(object_key="b.txt", version_id=None),
            make_entry(object_key="c.txt", version_id="v3"),
        ]
        csv = serialize(entries)
        rows = csv.splitlines()
        assert len(rows) == 3
        for row in rows:
            assert row.count(",") == 2, f"row {row!r} does not have 3 fields"

    def test_serialize_null_version_last_row_still_three_fields(self):
        """Specifically the last-row case that broke the UNLOAD path: a
        null-version entry as the final row must not be truncated."""
        entries = [
            make_entry(object_key="a.txt", version_id="v1"),
            make_entry(object_key="z-last.txt", version_id=None),
        ]
        csv = serialize(entries)
        last_row = csv.splitlines()[-1]
        assert last_row == "src-bucket,z-last.txt,null"

    def test_deserialize_round_trips_null_version_entry(self):
        entries = [make_entry(object_key="null-ver.txt", version_id=None)]
        csv = serialize(entries)
        restored = deserialize(csv)
        assert len(restored) == 1
        assert restored[0].object_key == "null-ver.txt"
        assert restored[0].version_id is None

    def test_null_version_never_serializes_as_empty_third_field(self):
        """Regression guard against reintroducing the empty-string variant:
        S3 Batch Operations accepts an empty third field at the manifest-
        parsing level (row has 3 fields) but then fails that individual
        task with SrcObjectNotFound: Object versionID is invalid — this
        was live-verified against a real S3 Batch Operations job. The
        third field for a null-version entry must always be the literal
        string 'null', never empty."""
        entries = [make_entry(object_key="null-ver.txt", version_id=None)]
        csv = serialize(entries)
        row = csv.splitlines()[0]
        third_field = row.split(",")[2]
        assert third_field == "null"
        assert third_field != ""


# ---------------------------------------------------------------------------
# Property 4: Manifest dedup and round-trip (task 6.2)
# Feature: tag-based-s3-replication, Property 4: Manifest deduplication and round-trip
# ---------------------------------------------------------------------------


class TestProperty4ManifestRoundTrip:
    @given(entries=st.lists(_entry_strategy(), min_size=0, max_size=20))
    @settings(max_examples=100)
    def test_round_trip_read_write(self, entries: list[ManifestEntry]) -> None:
        """read(write(S)) == S for any list of ManifestEntry objects.

        # Feature: tag-based-s3-replication, Property 4: Manifest deduplication and round-trip
        """
        csv_content = serialize(entries)
        restored = deserialize(csv_content)
        assert len(restored) == len(entries)
        for orig, rest in zip(entries, restored):
            assert rest.source_bucket == orig.source_bucket
            assert rest.object_key == orig.object_key

    @given(
        matched_list=st.lists(
            _matched_object_strategy(),
            min_size=1,
            max_size=30,
        )
    )
    @settings(max_examples=100)
    def test_finalized_manifest_has_no_duplicate_keys(
        self, matched_list: list[MatchedObject]
    ) -> None:
        """Each (source_bucket, object_key) pair appears at most once in the manifest.

        # Feature: tag-based-s3-replication, Property 4: Manifest deduplication and round-trip
        """
        gen = ManifestGenerator()
        gen.accumulate(set(matched_list))
        result = gen.finalize("src-bucket")

        if result.has_matches:
            keys = [(e.source_bucket, e.object_key) for e in result.entries]
            assert len(keys) == len(set(keys)), "Duplicate (bucket, key) pairs in manifest"

    @given(
        matched_list=st.lists(
            st.one_of(
                _matched_object_strategy(config_id="cfg-1"),
                _matched_object_strategy(config_id="cfg-1"),
            ),
            min_size=2,
            max_size=20,
        )
    )
    @settings(max_examples=100)
    def test_duplicate_matched_objects_deduplicated(
        self, matched_list: list[MatchedObject]
    ) -> None:
        """Duplicate MatchedObjects (same identity triple) produce one entry per key.

        # Feature: tag-based-s3-replication, Property 4: Manifest deduplication and round-trip
        """
        gen = ManifestGenerator()
        # Accumulate the same matched list twice (simulate duplicate journal records).
        gen.accumulate(set(matched_list))
        gen.accumulate(set(matched_list))
        result = gen.finalize("src-bucket")

        if result.has_matches:
            keys = [(e.source_bucket, e.object_key) for e in result.entries]
            assert len(keys) == len(set(keys))

    @given(entries=st.lists(_entry_strategy(), min_size=1, max_size=20))
    @settings(max_examples=100)
    def test_serialize_produces_comma_separated_format(
        self, entries: list[ManifestEntry]
    ) -> None:
        """Each CSV row contains at least one comma (bucket,key format).

        # Feature: tag-based-s3-replication, Property 4: Manifest deduplication and round-trip
        """
        csv_content = serialize(entries)
        for line in csv_content.splitlines():
            assert "," in line, f"Row has no comma: {line!r}"


# ---------------------------------------------------------------------------
# Property 5: At-most-one job per configuration with consolidation (task 6.3)
# Feature: tag-based-s3-replication, Property 5: At-most-one job per configuration with consolidation
# ---------------------------------------------------------------------------


class TestProperty5AtMostOneJobPerConfig:
    @given(
        config_ids=st.lists(
            _CONFIG_ID_ST, min_size=1, max_size=5, unique=True
        ),
        keys_per_config=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=100)
    def test_at_most_one_manifest_per_bucket_with_matches(
        self,
        config_ids: list[str],
        keys_per_config: int,
    ) -> None:
        """Exactly one finalized union manifest per bucket, spanning every
        config_id/rule that matched (design.md D1, Req 1.1).

        # Feature: tag-based-s3-replication, Property 5: At-most-one job per configuration with consolidation
        """
        gen = ManifestGenerator()
        expected_keys: set[str] = set()
        for config_id in config_ids:
            for i in range(keys_per_config):
                key = f"{config_id}-key-{i}.txt"
                expected_keys.add(key)
                obj = make_matched(object_key=key, config_id=config_id)
                gen.accumulate({obj})

        # finalize once per bucket — the union of every config_id's matches
        result = gen.finalize("src-bucket")

        assert result.has_matches
        manifest_keys = {e.object_key for e in result.entries}
        assert manifest_keys == expected_keys

    def test_zero_matches_bucket_produces_no_manifest(self) -> None:
        """A bucket with zero accumulated matches produces no manifest
        (has_matches=False).

        # Feature: tag-based-s3-replication, Property 5: At-most-one job per configuration with consolidation
        """
        gen = ManifestGenerator()
        # Do NOT accumulate anything.
        result = gen.finalize("src-bucket")
        assert result.has_matches is False
        assert result.entries == []
        assert result.object_count == 0

    @given(
        num_rules=st.integers(min_value=2, max_value=5),
        keys_per_rule=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=100)
    def test_multiple_rules_same_bucket_consolidated(
        self, num_rules: int, keys_per_rule: int
    ) -> None:
        """Multiple rules (possibly across different config_ids) for the same
        bucket → single consolidated union manifest (design.md D1).

        # Feature: tag-based-s3-replication, Property 5: At-most-one job per configuration with consolidation
        """
        gen = ManifestGenerator()
        total_keys: set[str] = set()

        for rule_n in range(num_rules):
            for key_n in range(keys_per_rule):
                key = f"rule{rule_n}-key{key_n}.txt"
                total_keys.add(key)
                obj = make_matched(
                    object_key=key,
                    config_id=f"cfg-{rule_n}",
                    rule_ids=(f"rule-{rule_n}",),
                )
                gen.accumulate({obj})

        result = gen.finalize("src-bucket")
        assert result.has_matches
        manifest_keys = {e.object_key for e in result.entries}
        assert manifest_keys == total_keys


# ---------------------------------------------------------------------------
# Property 11: Tolerance to duplicate journal records (task 6.4)
# Feature: tag-based-s3-replication, Property 11: Tolerance to duplicate journal records
# ---------------------------------------------------------------------------


class TestProperty11DuplicateTolerance:
    @given(
        distinct_keys=st.lists(
            _KEY_ST, min_size=1, max_size=10, unique=True
        ),
        dup_count=st.integers(min_value=2, max_value=4),
    )
    @settings(max_examples=100)
    def test_duplicate_accumulation_same_as_single(
        self, distinct_keys: list[str], dup_count: int
    ) -> None:
        """Accumulating duplicates dup_count times produces same manifest as once.

        # Feature: tag-based-s3-replication, Property 11: Tolerance to duplicate journal records
        """
        config_id = "cfg-1"
        matched_set = {
            make_matched(object_key=key, config_id=config_id)
            for key in distinct_keys
        }

        # Single accumulation.
        gen_single = ManifestGenerator()
        gen_single.accumulate(matched_set)
        result_single = gen_single.finalize("src-bucket")

        # Duplicate accumulations.
        gen_dup = ManifestGenerator()
        for _ in range(dup_count):
            gen_dup.accumulate(matched_set)
        result_dup = gen_dup.finalize("src-bucket")

        assert result_single.has_matches == result_dup.has_matches
        assert result_single.object_count == result_dup.object_count

        single_keys = {e.object_key for e in result_single.entries}
        dup_keys = {e.object_key for e in result_dup.entries}
        assert single_keys == dup_keys

    @given(
        distinct_keys=st.lists(
            _KEY_ST, min_size=1, max_size=10, unique=True
        ),
    )
    @settings(max_examples=100)
    def test_no_duplicate_keys_after_redundant_accumulation(
        self, distinct_keys: list[str]
    ) -> None:
        """Even with duplicate accumulation, each key appears exactly once.

        # Feature: tag-based-s3-replication, Property 11: Tolerance to duplicate journal records
        """
        config_id = "cfg-1"
        matched_set = {
            make_matched(object_key=key, config_id=config_id)
            for key in distinct_keys
        }

        gen = ManifestGenerator()
        gen.accumulate(matched_set)
        gen.accumulate(matched_set)  # same set twice
        gen.accumulate(matched_set)  # and a third time
        result = gen.finalize("src-bucket")

        object_keys = [e.object_key for e in result.entries]
        assert len(object_keys) == len(set(object_keys))


# ---------------------------------------------------------------------------
# Unit tests: edge cases (task 6.5)
# ---------------------------------------------------------------------------


class TestManifestGeneratorEdgeCases:
    def test_zero_match_interval_no_manifest(self):
        """Zero matches → has_matches=False, no entries (Req 6.6)."""
        gen = ManifestGenerator()
        result = gen.finalize("src-bucket")
        assert result.has_matches is False
        assert result.entries == []
        assert result.object_count == 0

    def test_zero_match_result_identifies_source_bucket(self):
        """no-match ManifestResult carries source_bucket."""
        gen = ManifestGenerator()
        result = gen.finalize("my-src-bucket")
        assert result.source_bucket == "my-src-bucket"

    def test_accumulate_empty_set_does_not_create_manifest(self):
        """Accumulating an empty set still yields no manifest."""
        gen = ManifestGenerator()
        gen.accumulate(set())
        result = gen.finalize("src-bucket")
        assert result.has_matches is False

    def test_one_match_produces_manifest_with_one_entry(self):
        gen = ManifestGenerator()
        gen.accumulate({make_matched(object_key="file.txt")})
        result = gen.finalize("src-bucket")
        assert result.has_matches
        assert result.object_count == 1
        assert result.entries[0].object_key == "file.txt"

    def test_manifest_entries_are_sorted_deterministically(self):
        """Entries are sorted lexicographically for deterministic output."""
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(object_key="zzz.txt"),
            make_matched(object_key="aaa.txt"),
            make_matched(object_key="mmm.txt"),
        })
        result = gen.finalize("src-bucket")
        assert result.has_matches
        keys = [e.object_key for e in result.entries]
        assert keys == sorted(keys)

    def test_object_count_equals_len_entries(self):
        gen = ManifestGenerator()
        for i in range(7):
            gen.accumulate({make_matched(object_key=f"obj-{i}.txt")})
        result = gen.finalize("src-bucket")
        assert result.object_count == len(result.entries)

    def test_same_object_matched_by_different_config_ids_yields_one_entry(self):
        """An object matched by rules under different config_ids is still one
        manifest entry (design.md D1, Req 1.1)."""
        gen = ManifestGenerator()
        gen.accumulate({make_matched(object_key="shared.txt", config_id="cfg-a")})
        gen.accumulate({make_matched(object_key="shared.txt", config_id="cfg-b")})
        result = gen.finalize("src-bucket")
        assert result.has_matches
        assert result.object_count == 1
        assert result.entries[0].object_key == "shared.txt"

    def test_same_scope_different_rule_ids_same_object_yields_one_entry(self):
        """Rules that share the same scope (same replication_config_id, i.e. the
        same tag filter/rule set on a bucket's replication configuration) but
        have different rule_ids (e.g. different destinations under the same
        config) matching the same object still collapse to one manifest entry
        (design.md D1, Req 1.1, 1.2)."""
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(
                object_key="shared.txt",
                config_id="cfg-1",
                rule_ids=("rule-dest-a",),
            )
        })
        gen.accumulate({
            make_matched(
                object_key="shared.txt",
                config_id="cfg-1",
                rule_ids=("rule-dest-b",),
            )
        })
        result = gen.finalize("src-bucket")
        assert result.has_matches
        assert result.object_count == 1
        assert result.entries[0].object_key == "shared.txt"

    def test_different_scopes_different_objects_all_present_no_cross_contamination(self):
        """Rules with different scopes (different tag filters/config_ids)
        matching different objects: every distinct object is present in the
        union manifest, and no object bleeds into a scope it wasn't matched
        under (design.md D1, Req 1.1)."""
        gen = ManifestGenerator()
        gen.accumulate({make_matched(object_key="only-in-a.txt", config_id="cfg-a", rule_ids=("rule-a",))})
        gen.accumulate({make_matched(object_key="only-in-b.txt", config_id="cfg-b", rule_ids=("rule-b",))})
        gen.accumulate({make_matched(object_key="only-in-c.txt", config_id="cfg-c", rule_ids=("rule-c",))})
        result = gen.finalize("src-bucket")
        assert result.has_matches
        keys = {e.object_key for e in result.entries}
        assert keys == {"only-in-a.txt", "only-in-b.txt", "only-in-c.txt"}
        assert result.object_count == 3

    def test_object_matched_by_three_different_rules_yields_one_entry(self):
        """A single (source_bucket, object_key) pair matched by exactly three
        distinct rules (different config_ids/rule_ids) yields exactly one
        manifest entry (design.md D1, Req 1.1, 1.2)."""
        gen = ManifestGenerator()
        gen.accumulate({make_matched(object_key="triple-matched.txt", config_id="cfg-1", rule_ids=("rule-1",))})
        gen.accumulate({make_matched(object_key="triple-matched.txt", config_id="cfg-2", rule_ids=("rule-2",))})
        gen.accumulate({make_matched(object_key="triple-matched.txt", config_id="cfg-3", rule_ids=("rule-3",))})
        result = gen.finalize("src-bucket")
        assert result.has_matches
        assert result.object_count == 1
        assert result.entries[0].object_key == "triple-matched.txt"

    def test_version_id_first_write_wins_across_duplicate_accumulation(self):
        """When the first accumulated match for a (bucket, key) carries a
        version_id, a later duplicate accumulation for the same key — even
        with a different or None version_id — does not overwrite it
        (first-write-wins retention under the union, Req 1.1, 1.2)."""
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(object_key="versioned.txt", config_id="cfg-1", version_id="v-first")
        })
        # Later duplicate accumulation for the same key under a different rule,
        # carrying a different version_id — must not overwrite "v-first".
        gen.accumulate({
            make_matched(object_key="versioned.txt", config_id="cfg-2", version_id="v-second")
        })
        # And once more with version_id=None.
        gen.accumulate({
            make_matched(object_key="versioned.txt", config_id="cfg-3", version_id=None)
        })
        result = gen.finalize("src-bucket")
        assert result.has_matches
        assert result.object_count == 1
        assert result.entries[0].version_id == "v-first"

    def test_version_id_none_first_write_stays_none(self):
        """If the first accumulated match has no version_id, a later duplicate
        with a version_id does not retroactively populate it (first-write-wins
        applies symmetrically)."""
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(object_key="unversioned.txt", config_id="cfg-1", version_id=None)
        })
        gen.accumulate({
            make_matched(object_key="unversioned.txt", config_id="cfg-2", version_id="v-later")
        })
        result = gen.finalize("src-bucket")
        assert result.has_matches
        assert result.entries[0].version_id is None

    def test_never_calling_accumulate_for_bucket_produces_no_matches(self):
        """A bucket for which accumulate() was never called still produces
        has_matches=False, entries=[], object_count=0 via finalize (Req 6.6)."""
        gen = ManifestGenerator()
        # Accumulate for a different bucket only.
        gen.accumulate({make_matched(source_bucket="other-bucket", object_key="x.txt")})
        result = gen.finalize("src-bucket")
        assert result.has_matches is False
        assert result.entries == []
        assert result.object_count == 0

    def test_has_accumulated_entries_reflects_accumulations(self):
        gen = ManifestGenerator()
        gen.accumulate({make_matched(config_id="cfg-a")})
        assert gen.has_accumulated_entries("src-bucket") is True
        assert gen.has_accumulated_entries("other-bucket") is False

    def test_has_accumulated_entries_false_for_new_generator(self):
        gen = ManifestGenerator()
        assert gen.has_accumulated_entries("src-bucket") is False


class TestSerializeDeserialize:
    def test_empty_entries_serializes_to_empty_string(self):
        assert serialize([]) == ""

    def test_empty_string_deserializes_to_empty_list(self):
        assert deserialize("") == []

    def test_single_entry_round_trip(self):
        entries = [make_entry("b", "k/e/y.txt")]
        assert deserialize(serialize(entries))[0].object_key == "k/e/y.txt"

    def test_multiple_entries_round_trip(self):
        entries = [make_entry("b", f"obj-{i}.txt") for i in range(5)]
        restored = deserialize(serialize(entries))
        assert len(restored) == 5
        for orig, rest in zip(entries, restored):
            assert rest.object_key == orig.object_key

    def test_no_trailing_newline(self):
        """serialize produces no trailing newline (ensures read(write(S)) == S)."""
        entries = [make_entry("b", "k.txt")]
        csv = serialize(entries)
        assert not csv.endswith("\n")

    def test_newline_separator_between_rows(self):
        """Multiple entries are separated by newlines."""
        entries = [make_entry("b", f"k{i}.txt") for i in range(3)]
        csv = serialize(entries)
        assert csv.count("\n") == 2  # 3 rows → 2 separators


# ---------------------------------------------------------------------------
# get_routing — matched rules and destinations for the completion report
#
# An object matched by two rules produces two Matched_Objects that collapse to
# ONE manifest entry. The manifest entry is deduplicated first-write-wins, but
# the reported rules and destinations must be the UNION across every
# Matched_Object — otherwise a second rule's destination is silently dropped.
# ---------------------------------------------------------------------------


class TestGetRouting:
    def test_single_rule_reports_its_rule_and_destination(self):
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(
                object_key="a.txt",
                rule_ids=("rule-1",),
                destination_arns=("arn:aws:s3:::dest-a",),
            )
        })
        routing = gen.get_routing("src-bucket")
        assert routing == {("a.txt", ""): (["rule-1"], ["dest-a"])}

    def test_two_rules_matching_one_object_union_both_destinations(self):
        """The dual-tag case: one object, two rules, two destinations. Both
        must appear even though the object yields a single manifest entry."""
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(
                object_key="a.txt",
                config_id="cfg-replicate",
                rule_ids=("replicate-tagged",),
                destination_arns=("arn:aws:s3:::dest-a",),
            ),
            make_matched(
                object_key="a.txt",
                config_id="cfg-archive",
                rule_ids=("archive-tagged",),
                destination_arns=("arn:aws:s3:::dest-b",),
            ),
        })

        # Still one manifest entry — dedup is unchanged.
        result = gen.finalize("src-bucket")
        assert result.object_count == 1

        rules, destinations = gen.get_routing("src-bucket")[("a.txt", "")]
        assert rules == ["archive-tagged", "replicate-tagged"]
        assert destinations == ["dest-a", "dest-b"]

    def test_union_holds_across_separate_accumulate_calls(self):
        """Matched_Objects for the same key can arrive in different
        accumulate() calls (different journal records); the union must still
        span them."""
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(object_key="a.txt", rule_ids=("rule-1",),
                         destination_arns=("arn:aws:s3:::dest-a",))
        })
        gen.accumulate({
            make_matched(object_key="a.txt", config_id="cfg-2", rule_ids=("rule-2",),
                         destination_arns=("arn:aws:s3:::dest-b",))
        })
        rules, destinations = gen.get_routing("src-bucket")[("a.txt", "")]
        assert rules == ["rule-1", "rule-2"]
        assert destinations == ["dest-a", "dest-b"]

    def test_destinations_rendered_as_bucket_names_not_arns(self):
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(destination_arns=("arn:aws:s3:::my-dest-bucket",))
        })
        _, destinations = gen.get_routing("src-bucket")[("path/obj.txt", "")]
        assert destinations == ["my-dest-bucket"]

    def test_non_arn_destination_passes_through_unchanged(self):
        """An unrecognised shape surfaces as-is rather than being truncated to
        something misleading."""
        gen = ManifestGenerator()
        gen.accumulate({make_matched(destination_arns=("not-an-arn",))})
        _, destinations = gen.get_routing("src-bucket")[("path/obj.txt", "")]
        assert destinations == ["not-an-arn"]

    def test_keyed_by_version_id_when_present(self):
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(object_key="a.txt", version_id="v1",
                         destination_arns=("arn:aws:s3:::dest-a",))
        })
        assert ("a.txt", "v1") in gen.get_routing("src-bucket")

    def test_object_with_no_destination_reports_empty_lists(self):
        gen = ManifestGenerator()
        gen.accumulate({make_matched(object_key="a.txt", rule_ids=("rule-1",))})
        rules, destinations = gen.get_routing("src-bucket")[("a.txt", "")]
        assert rules == ["rule-1"]
        assert destinations == []

    def test_routing_is_scoped_per_source_bucket(self):
        gen = ManifestGenerator()
        gen.accumulate({
            make_matched(source_bucket="bucket-a", object_key="a.txt",
                         destination_arns=("arn:aws:s3:::dest-a",)),
            make_matched(source_bucket="bucket-b", object_key="b.txt",
                         destination_arns=("arn:aws:s3:::dest-b",)),
        })
        assert list(gen.get_routing("bucket-a")) == [("a.txt", "")]
        assert list(gen.get_routing("bucket-b")) == [("b.txt", "")]

    def test_unknown_bucket_returns_empty_mapping(self):
        assert ManifestGenerator().get_routing("never-accumulated") == {}
