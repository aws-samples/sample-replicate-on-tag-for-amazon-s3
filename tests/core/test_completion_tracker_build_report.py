"""Unit and property tests for ``build_completion_report`` (task 6.4).

Feature: source-status-completion-tracking.

This tests the v2 grouped completion report format. Per-object detail
(object_key, version_id) is no longer in the report — items are aggregated
into groups by (source_bucket, matched_rules, destinations). Each group
carries count, outcome_counts, and optional datetime ranges.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import (
    build_completion_report,
    format_completion_report_subject,
)
from src.core.models import CompletionState, ConfigContext, TrackedObject

_MANIFEST_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_NOW = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)

_ALL_REPLICATION_OUTCOMES = ("COMPLETE", "FAILED", "UNKNOWN")


def make_config_context(
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
) -> ConfigContext:
    return ConfigContext(
        replication_config_id=replication_config_id,
        job_id=job_id,
        manifest_generated_at=_MANIFEST_AT,
        bops_confirmed=True,
    )


def make_obj(
    object_key: str = "a.txt",
    version_id: str | None = "v1",
    source_bucket: str = "example-source-bucket",
    configs: dict[str, ConfigContext] | None = None,
    replication_outcome: str = "COMPLETE",
    matched_rules: frozenset[str] = frozenset(),
    destinations: frozenset[str] = frozenset(),
    tagged_at: datetime | None = None,
    last_modified: datetime | None = None,
) -> TrackedObject:
    return TrackedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
        configs=configs if configs is not None else {"cfg-1": make_config_context()},
        state=CompletionState.RESOLVED,
        resolved_at=_NOW,
        resolution_method="source_status_header",
        replication_outcome=replication_outcome,
        matched_rules=matched_rules,
        destinations=destinations,
        tagged_at=tagged_at,
        last_modified=last_modified,
    )



# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestBuildCompletionReport:
    def test_format_version_is_3(self):
        """Bumped from 2 by the removal of the `outstanding` field. Removing rather than
        redefining, with an explicit bump, is cheaper for a subscriber than
        silently changing the meaning of a field it already parses."""
        obj = make_obj()
        report = build_completion_report("my-bucket", [obj])
        assert report["format_version"] == 3

    def test_source_bucket_copied_verbatim_from_parameter(self):
        obj = make_obj(source_bucket="item-bucket")
        report = build_completion_report("param-bucket", [obj])
        assert report["source_bucket"] == "param-bucket"

    def test_item_count_equals_number_of_tracked_objects(self):
        items = [
            make_obj(object_key="a.txt", configs={"cfg-1": make_config_context(), "cfg-2": make_config_context(replication_config_id="cfg-2")}),
            make_obj(object_key="b.txt"),
        ]
        report = build_completion_report("example-source-bucket", items)
        assert report["item_count"] == 2

    def test_empty_items_list(self):
        report = build_completion_report("example-source-bucket", [])
        assert report["source_bucket"] == "example-source-bucket"
        assert report["item_count"] == 0
        assert report["outcome_counts"] == {}
        assert report["groups"] == []

    def test_item_reports_its_source_bucket(self):
        obj = make_obj(source_bucket="src-bucket")
        report = build_completion_report("src-bucket", [obj])
        assert report["groups"][0]["source_bucket"] == "src-bucket"

    def test_configs_are_never_reported_as_destinations(self):
        """`configs` holds the per-bucket sentinel, not a destination — it must
        not leak into `destinations`, which is the bug this field replaced."""
        obj = make_obj(
            source_bucket="src-bucket",
            configs={"src-bucket": make_config_context(replication_config_id="src-bucket")},
            destinations=frozenset({"dest-bucket"}),
        )
        report = build_completion_report("src-bucket", [obj])
        assert report["groups"][0]["destinations"] == ["dest-bucket"]

    def test_destinations_absent_when_unknown(self):
        obj = make_obj(destinations=frozenset())
        report = build_completion_report("my-bucket", [obj])
        assert report["groups"][0]["destinations"] == []

    def test_multi_destination_item_lists_every_destination_sorted(self):
        obj = make_obj(destinations=frozenset({"dest-b", "dest-a"}))
        report = build_completion_report("my-bucket", [obj])
        assert report["groups"][0]["destinations"] == ["dest-a", "dest-b"]

    def test_matched_rules_reported_sorted_when_present(self):
        obj = make_obj(matched_rules=frozenset({"rule-z", "rule-a"}))
        report = build_completion_report("my-bucket", [obj])
        assert report["groups"][0]["matched_rules"] == ["rule-a", "rule-z"]

    def test_matched_rules_absent_when_unknown(self):
        obj = make_obj(matched_rules=frozenset())
        report = build_completion_report("my-bucket", [obj])
        assert report["groups"][0]["matched_rules"] == []

    def test_aggregate_outcome_is_not_attributed_per_destination(self):
        """A FAILED item with two destinations reports one aggregate outcome,
        not one per destination — the source header cannot distinguish them."""
        obj = make_obj(
            replication_outcome="FAILED",
            destinations=frozenset({"dest-a", "dest-b"}),
        )
        report = build_completion_report("my-bucket", [obj])
        group = report["groups"][0]
        assert group["outcome_counts"] == {"FAILED": 1}
        assert group["destinations"] == ["dest-a", "dest-b"]
        assert report["outcome_counts"] == {"FAILED": 1}

    def test_single_aggregate_outcome_per_item(self):
        obj = make_obj(replication_outcome="FAILED")
        report = build_completion_report("my-bucket", [obj])
        assert report["groups"][0]["outcome_counts"] == {"FAILED": 1}

    def test_outcome_counts_one_per_item_not_per_config(self):
        """One item with 2 routing configs still contributes 1 to
        outcome_counts, since the outcome is aggregate/object-level."""
        obj = make_obj(
            replication_outcome="COMPLETE",
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a"),
                "cfg-b": make_config_context(replication_config_id="cfg-b"),
            },
        )
        report = build_completion_report("my-bucket", [obj])
        assert report["outcome_counts"] == {"COMPLETE": 1}
        assert report["item_count"] == 1

    def test_outcome_counts_across_multiple_items(self):
        items = [
            make_obj(object_key="a.txt", replication_outcome="COMPLETE"),
            make_obj(object_key="b.txt", replication_outcome="FAILED"),
            make_obj(object_key="c.txt", replication_outcome="COMPLETE"),
        ]
        report = build_completion_report("my-bucket", items)
        assert report["outcome_counts"] == {"COMPLETE": 2, "FAILED": 1}
        assert report["item_count"] == 3

    def test_outcome_counts_only_include_present_outcomes(self):
        obj = make_obj(replication_outcome="COMPLETE")
        report = build_completion_report("my-bucket", [obj])
        assert "UNKNOWN" not in report["outcome_counts"]
        assert "FAILED" not in report["outcome_counts"]

    def test_report_never_attributes_destinations_to_wrong_item(self):
        """Items with different destinations land in different groups."""
        item_a = make_obj(object_key="a.txt", destinations=frozenset({"dest-a"}))
        item_b = make_obj(object_key="b.txt", destinations=frozenset({"dest-b"}))
        report = build_completion_report("my-bucket", [item_a, item_b])
        by_dest = {tuple(g["destinations"]): g for g in report["groups"]}
        assert ("dest-a",) in by_dest
        assert ("dest-b",) in by_dest
        assert by_dest[("dest-a",)]["count"] == 1
        assert by_dest[("dest-b",)]["count"] == 1


# ---------------------------------------------------------------------------
# report["groups"] — the aggregate structure that replaces per-object entries
# ---------------------------------------------------------------------------


class TestGroups:
    def test_no_per_object_detail_reaches_the_report(self):
        """Requirement 1.2: object keys and version IDs are not in the email.

        Asserted against the serialized body rather than a field name, so a
        reintroduced per-object entry is caught wherever it is nested.
        """
        items = [
            make_obj(object_key="secret/project-x/report.pdf", version_id="ver-abc"),
            make_obj(object_key="secret/project-y/notes.txt", version_id="ver-def"),
        ]
        body = json.dumps(build_completion_report("my-bucket", items))
        assert "secret/project-x/report.pdf" not in body
        assert "ver-abc" not in body
        assert "object_key" not in body
        assert "version_id" not in body
        # Nor the per-object fields the flat format carried.
        assert "confidence" not in body
        assert '"outcome"' not in body

    def test_items_sharing_a_key_collapse_to_one_group(self):
        """The common case — one bucket, one rule, one destination — is a
        single group carrying the whole batch's statistics."""
        early = datetime(2024, 5, 1, tzinfo=timezone.utc)
        late = datetime(2024, 5, 3, tzinfo=timezone.utc)
        items = [
            make_obj(
                object_key=f"{i}.txt",
                matched_rules=frozenset({"rule-a"}),
                destinations=frozenset({"dest-a"}),
                tagged_at=ts,
                last_modified=ts,
            )
            for i, ts in enumerate((late, early, late))
        ]
        report = build_completion_report("my-bucket", items)
        assert len(report["groups"]) == 1
        group = report["groups"][0]
        assert group["count"] == 3
        assert group["outcome_counts"] == {"COMPLETE": 3}
        assert group["tagged_at_range"] == [early.isoformat(), late.isoformat()]
        assert group["last_modified_range"] == [early.isoformat(), late.isoformat()]

    def test_group_count_sums_to_item_count(self):
        items = [
            make_obj(object_key="a.txt", destinations=frozenset({"dest-a"})),
            make_obj(object_key="b.txt", destinations=frozenset({"dest-b"})),
            make_obj(object_key="c.txt", destinations=frozenset({"dest-b"})),
        ]
        report = build_completion_report("my-bucket", items)
        assert len(report["groups"]) == 2
        assert sum(g["count"] for g in report["groups"]) == report["item_count"]

    def test_a_group_reports_its_mixed_outcomes(self):
        """One rule can still produce different outcomes, so a failure inside
        an otherwise-successful group stays visible without listing objects."""
        items = [
            make_obj(object_key="a.txt", matched_rules=frozenset({"rule-a"})),
            make_obj(
                object_key="b.txt",
                matched_rules=frozenset({"rule-a"}),
                replication_outcome="FAILED",
            ),
        ]
        report = build_completion_report("my-bucket", items)
        assert len(report["groups"]) == 1
        assert report["groups"][0]["outcome_counts"] == {"COMPLETE": 1, "FAILED": 1}

    def test_groups_follow_first_appearance_order(self):
        """Group order is the order each key is first seen, so the same batch
        always serializes to the same body."""
        items = [
            make_obj(object_key="z.txt", destinations=frozenset({"dest-z"})),
            make_obj(object_key="a.txt", destinations=frozenset({"dest-a"})),
            make_obj(object_key="m.txt", destinations=frozenset({"dest-z"})),
        ]
        report = build_completion_report("my-bucket", items)
        assert [g["destinations"] for g in report["groups"]] == [["dest-z"], ["dest-a"]]

    def test_datetime_ranges_omitted_when_no_object_holds_the_value(self):
        report = build_completion_report("my-bucket", [make_obj()])
        group = report["groups"][0]
        assert "tagged_at_range" not in group
        assert "last_modified_range" not in group

    def test_range_spans_only_the_objects_holding_the_value(self):
        """One object without a timestamp does not collapse or void the range
        for the objects that have one."""
        ts = datetime(2024, 5, 1, tzinfo=timezone.utc)
        items = [
            make_obj(object_key="a.txt", tagged_at=ts),
            make_obj(object_key="b.txt", tagged_at=None),
        ]
        group = build_completion_report("my-bucket", items)["groups"][0]
        assert group["tagged_at_range"] == [ts.isoformat(), ts.isoformat()]

    def test_group_is_absent_for_a_bucket_with_no_items(self):
        report = build_completion_report("my-bucket", [])
        assert report["groups"] == []


# ---------------------------------------------------------------------------
# report["summary"] — human-readable headline, first key in the dict
# ---------------------------------------------------------------------------


class TestSummaryField:
    def test_summary_is_the_first_key(self):
        """Dict insertion order (preserved through json.dumps) must put
        summary first, so it's the first visible line of a pretty-printed
        JSON message body."""
        report = build_completion_report("my-bucket", [make_obj()])
        assert next(iter(report.keys())) == "summary"

    def test_includes_source_bucket_and_item_count(self):
        report = build_completion_report("my-bucket", [make_obj(), make_obj(object_key="b.txt")])
        assert "my-bucket" in report["summary"]
        assert "2 objects" in report["summary"]

    def test_describes_outcomes_in_plain_english_not_enum_names(self):
        """The summary is read in an email, so it must not leak the internal
        outcome enum at the reader."""
        items = [
            make_obj(object_key="a.txt", replication_outcome="COMPLETE"),
            make_obj(object_key="b.txt", replication_outcome="FAILED"),
            make_obj(object_key="c.txt", replication_outcome="COMPLETE"),
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert "2 replicated successfully" in summary
        assert "1 failed to replicate" in summary
        assert "COMPLETE" not in summary
        assert "FAILED" not in summary

    def test_failures_lead_even_when_in_the_minority(self):
        """Severity order, not count order: a reader must meet the failures
        first."""
        items = [
            make_obj(object_key=f"ok-{i}.txt", replication_outcome="COMPLETE")
            for i in range(9)
        ] + [make_obj(object_key="bad.txt", replication_outcome="FAILED")]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert summary.index("failed to replicate") < summary.index(
            "replicated successfully"
        )

    def test_states_whether_action_is_needed(self):
        failed = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="FAILED")]
        )["summary"]
        assert "Action needed: 1 of 1 did not replicate." in failed

        unknown = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="UNKNOWN")]
        )["summary"]
        assert "Action needed: 1 of 1 did not replicate." in unknown

        complete = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="COMPLETE")]
        )["summary"]
        assert "No action needed." in complete

    def test_all_complete_reads_as_a_single_clause(self):
        """The common case should not repeat the count either side of a dash."""
        items = [
            make_obj(object_key=f"{i}.txt", replication_outcome="COMPLETE")
            for i in range(3)
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert summary == (
            "my-bucket: 3 objects replicated successfully. No action needed."
        )

    def test_large_counts_carry_thousands_separators(self):
        items = [
            make_obj(object_key=f"{i}.txt", replication_outcome="UNKNOWN")
            for i in range(1057)
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert "1,057" in summary
        assert "1057" not in summary

    def test_singular_object_is_not_pluralised(self):
        summary = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="COMPLETE")]
        )["summary"]
        assert "1 object " in summary
        assert "1 objects" not in summary

    def test_single_outcome_absorbs_the_total_without_repeating_it(self):
        """Avoids repeating the count in a single-outcome summary."""
        items = [
            make_obj(object_key=f"{i}.txt", replication_outcome="COMPLETE")
            for i in range(1057)
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert summary == (
            "my-bucket: 1,057 objects replicated successfully. No action needed."
        )
        assert summary.count("1,057") == 1

    def test_mixed_outcomes_are_semicolon_separated_after_a_dash(self):
        items = [
            make_obj(object_key="a.txt", replication_outcome="FAILED"),
            make_obj(object_key="b.txt", replication_outcome="COMPLETE"),
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert summary == (
            "my-bucket: 2 objects — 1 failed to replicate; 1 replicated "
            "successfully. Action needed: 1 of 2 did not replicate."
        )

    def test_subject_object_noun_agrees_with_count(self):
        one = format_completion_report_subject(
            build_completion_report("my-bucket", [make_obj(replication_outcome="COMPLETE")])
        )
        assert "1 object," in one
        assert "1 objects" not in one

    def test_empty_report_still_produces_readable_summary(self):
        report = build_completion_report("my-bucket", [])
        assert report["summary"] == "my-bucket: no objects to report."

    def test_none_outcome_rendered_in_words_not_python_none(self):
        obj = make_obj(replication_outcome=None)
        summary = build_completion_report("my-bucket", [obj])["summary"]
        assert "reported an unrecognized task status" in summary
        assert "None" not in summary
        assert "Action needed" in summary

    def test_summary_has_no_embedded_newline(self):
        """A multi-line summary would render as an escaped \\n inside the
        JSON string value rather than a real line break — keep it one line."""
        items = [
            make_obj(object_key="a.txt", replication_outcome="COMPLETE"),
            make_obj(object_key="b.txt", replication_outcome="FAILED"),
        ]
        report = build_completion_report("my-bucket", items)
        assert "\n" not in report["summary"]

    def test_summary_round_trips_through_json(self):
        report = build_completion_report("my-bucket", [make_obj()])
        restored = json.loads(json.dumps(report))
        assert restored["summary"] == report["summary"]
        assert next(iter(restored.keys())) == "summary"



# ---------------------------------------------------------------------------
# The report is decoupled from `configs` (design.md D4): whatever shape the
# configs dict has, it never reaches the reported destinations.
# ---------------------------------------------------------------------------


class TestReportIsDecoupledFromConfigs:
    """Under the one-job-per-bucket design (design.md D4),
    ``merge_completion_configs`` merges exactly one ``ConfigContext`` per
    object, keyed by the per-bucket sentinel — the bucket's own name. The
    report therefore does not read ``configs`` at all: it reports
    ``source_bucket`` directly and takes ``destinations`` from the object's
    matched replication rules.

    These tests pin that decoupling for the current sentinel shape and for the
    legacy multi-config shape a state object written before the migration can
    still carry."""

    def test_sentinel_config_does_not_appear_as_a_destination(self):
        bucket_sentinel = "my-bucket"
        obj = make_obj(
            source_bucket=bucket_sentinel,
            configs={bucket_sentinel: make_config_context(replication_config_id=bucket_sentinel, job_id="job-42")},
            destinations=frozenset({"dest-bucket"}),
        )
        report = build_completion_report(bucket_sentinel, [obj])
        group = report["groups"][0]
        assert group["source_bucket"] == bucket_sentinel
        assert group["destinations"] == ["dest-bucket"]

    def test_legacy_multi_config_keys_do_not_appear_as_destinations(self):
        """A legacy-shaped TrackedObject whose configs dict still carries
        multiple per-rule keys reports none of them as destinations — those
        keys are rule/config identifiers, never destination buckets."""
        obj = make_obj(
            configs={
                "rule-a": make_config_context(replication_config_id="rule-a"),
                "rule-b": make_config_context(replication_config_id="rule-b"),
                "rule-c": make_config_context(replication_config_id="rule-c"),
            },
            destinations=frozenset({"dest-only"}),
        )
        report = build_completion_report("my-bucket", [obj])
        assert report["groups"][0]["destinations"] == ["dest-only"]

    def test_legacy_object_without_routing_omits_both_routing_fields(self):
        """An object tracked before matched_rules/destinations were recorded
        reports empty lists rather than populated fields."""
        obj = make_obj(
            configs={"rule-a": make_config_context(replication_config_id="rule-a")},
        )
        report = build_completion_report("my-bucket", [obj])
        group = report["groups"][0]
        assert group["destinations"] == []
        assert group["matched_rules"] == []
        assert group["source_bucket"] == "example-source-bucket"

    def test_outcome_and_outcome_counts_unaffected_by_sentinel_collapse(self):
        """Requirement 3.2: the aggregate outcome (per-group and in
        outcome_counts) is unchanged by the configs-dict collapsing to a
        single sentinel entry — it is derived solely from
        obj.replication_outcome, independent of configs."""
        bucket_sentinel = "my-bucket"
        items = [
            make_obj(
                object_key="a.txt",
                source_bucket=bucket_sentinel,
                configs={bucket_sentinel: make_config_context(replication_config_id=bucket_sentinel)},
                replication_outcome="COMPLETE",
            ),
            make_obj(
                object_key="b.txt",
                source_bucket=bucket_sentinel,
                configs={bucket_sentinel: make_config_context(replication_config_id=bucket_sentinel)},
                replication_outcome="FAILED",
            ),
        ]
        report = build_completion_report(bucket_sentinel, items)
        assert report["outcome_counts"] == {"COMPLETE": 1, "FAILED": 1}



# ---------------------------------------------------------------------------
# Property 9: A published report's grouping and content exactly match the
# batch of Tracked_Objects it covers
# Feature: source-status-completion-tracking, Property 9: A published report's grouping and content exactly match the batch of Tracked_Objects it covers
# Validates: Requirements 4.2, 4.3
# ---------------------------------------------------------------------------


@st.composite
def _tracked_objects_with_mixed_configs(draw) -> list[TrackedObject]:
    keys = draw(
        st.lists(
            st.text(min_size=1, max_size=20).filter(lambda s: "\x00" not in s),
            min_size=1,
            max_size=8,
            unique=True,
        )
    )
    items = []
    for key in keys:
        version_id = draw(st.one_of(st.none(), st.text(min_size=1, max_size=10)))
        num_configs = draw(st.integers(min_value=1, max_value=3))
        configs = {}
        for d in range(num_configs):
            config_id = f"cfg-{key}-{d}"
            configs[config_id] = make_config_context(replication_config_id=config_id)
        outcome = draw(st.sampled_from(_ALL_REPLICATION_OUTCOMES))
        # Drawn independently of `configs`, including the empty case, so the
        # properties below cannot pass by accidentally coupling the two.
        num_destinations = draw(st.integers(min_value=0, max_value=3))
        destinations = frozenset(f"dest-{key}-{d}" for d in range(num_destinations))
        matched_rules = frozenset(f"rule-{key}-{d}" for d in range(num_destinations))
        # Generate optional datetime fields for range testing.
        tagged_at = draw(st.one_of(
            st.none(),
            st.datetimes(
                min_value=datetime(2023, 1, 1),
                max_value=datetime(2025, 1, 1),
                timezones=st.just(timezone.utc),
            ),
        ))
        last_modified = draw(st.one_of(
            st.none(),
            st.datetimes(
                min_value=datetime(2023, 1, 1),
                max_value=datetime(2025, 1, 1),
                timezones=st.just(timezone.utc),
            ),
        ))
        items.append(
            TrackedObject(
                source_bucket="my-bucket",
                object_key=key,
                version_id=version_id,
                configs=configs,
                state=CompletionState.RESOLVED,
                resolved_at=_NOW,
                resolution_method="source_status_header",
                replication_outcome=outcome,
                matched_rules=matched_rules,
                destinations=destinations,
                tagged_at=tagged_at,
                last_modified=last_modified,
            )
        )
    return items



class TestProperty9ReportGroupingAndContentMatchBatch:
    """# Feature: source-status-completion-tracking, Property 9: A published report's grouping and content exactly match the batch of Tracked_Objects it covers

    Validates: Requirements 4.2, 4.3
    """

    @given(
        items=_tracked_objects_with_mixed_configs(),
        source_bucket=st.from_regex(r"^[a-z][a-z0-9\-]{2,20}$", fullmatch=True),
    )
    @settings(max_examples=100)
    def test_groups_aggregate_correctly_by_key(
        self,
        items: list[TrackedObject],
        source_bucket: str,
    ) -> None:
        """Items sharing (source_bucket, matched_rules, destinations) land in
        one group with the correct count and outcome_counts."""
        report = build_completion_report(source_bucket, items)

        assert report["source_bucket"] == source_bucket
        assert report["item_count"] == len(items)
        assert report["format_version"] == 3

        # Rebuild expected groups from items.
        GroupKey = tuple[str, tuple[str, ...], tuple[str, ...]]
        expected: dict[GroupKey, list[TrackedObject]] = {}
        for obj in items:
            key: GroupKey = (
                obj.source_bucket,
                tuple(sorted(obj.matched_rules)),
                tuple(sorted(obj.destinations)),
            )
            expected.setdefault(key, []).append(obj)

        assert len(report["groups"]) == len(expected)

        # Build a lookup from group key to the reported group.
        reported_by_key: dict[GroupKey, dict] = {}
        for group in report["groups"]:
            gk: GroupKey = (
                group["source_bucket"],
                tuple(group["matched_rules"]),
                tuple(group["destinations"]),
            )
            reported_by_key[gk] = group

        for key, group_items in expected.items():
            assert key in reported_by_key, f"Missing group for key {key}"
            group = reported_by_key[key]

            # Count matches.
            assert group["count"] == len(group_items)

            # Outcome counts match.
            exp_outcomes: dict[str, int] = {}
            for obj in group_items:
                exp_outcomes[obj.replication_outcome] = (
                    exp_outcomes.get(obj.replication_outcome, 0) + 1
                )
            assert group["outcome_counts"] == exp_outcomes

            # The configs dict must never leak into the reported destinations.
            for obj in group_items:
                assert set(group["destinations"]).isdisjoint(obj.configs.keys())

            # tagged_at_range correctness.
            tagged_ats = [obj.tagged_at for obj in group_items if obj.tagged_at is not None]
            if tagged_ats:
                assert "tagged_at_range" in group
                assert group["tagged_at_range"] == [
                    min(tagged_ats).isoformat(),
                    max(tagged_ats).isoformat(),
                ]
            else:
                assert "tagged_at_range" not in group

            # last_modified_range correctness.
            last_mods = [obj.last_modified for obj in group_items if obj.last_modified is not None]
            if last_mods:
                assert "last_modified_range" in group
                assert group["last_modified_range"] == [
                    min(last_mods).isoformat(),
                    max(last_mods).isoformat(),
                ]
            else:
                assert "last_modified_range" not in group

        # Top-level outcome_counts is the sum across all groups.
        expected_counts: dict[str, int] = {}
        for item in items:
            expected_counts[item.replication_outcome] = expected_counts.get(item.replication_outcome, 0) + 1
        assert report["outcome_counts"] == expected_counts

    @given(
        items_a=_tracked_objects_with_mixed_configs(),
        items_b=_tracked_objects_with_mixed_configs(),
    )
    @settings(max_examples=100)
    def test_report_never_leaks_items_or_configs_outside_the_input_batch(
        self,
        items_a: list[TrackedObject],
        items_b: list[TrackedObject],
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 9: A published report's grouping and content exactly match the batch of Tracked_Objects it covers"""
        report_a = build_completion_report("bucket-a", items_a)

        def group_keys(items: list[TrackedObject]) -> set[tuple]:
            return {
                (
                    obj.source_bucket,
                    tuple(sorted(obj.matched_rules)),
                    tuple(sorted(obj.destinations)),
                )
                for obj in items
            }

        reported = {
            (
                group["source_bucket"],
                tuple(group["matched_rules"]),
                tuple(group["destinations"]),
            )
            for group in report_a["groups"]
        }
        # The reported group set is exactly items_a's, not a superset or a
        # subset, and carries nothing that only items_b would have produced.
        assert reported == group_keys(items_a)
        assert reported.isdisjoint(group_keys(items_b) - group_keys(items_a))

        # Every object in items_a is counted exactly once, in its own group.
        assert sum(g["count"] for g in report_a["groups"]) == len(items_a)
        for group in report_a["groups"]:
            key = (
                group["source_bucket"],
                tuple(group["matched_rules"]),
                tuple(group["destinations"]),
            )
            own = [
                obj
                for obj in items_a
                if (
                    obj.source_bucket,
                    tuple(sorted(obj.matched_rules)),
                    tuple(sorted(obj.destinations)),
                )
                == key
            ]
            assert group["count"] == len(own)



# ---------------------------------------------------------------------------
# SNS subject line (Requirement 4.9)
#
# Without a subject every report arrives titled with the SNS topic name, so an
# inbox of them cannot be triaged without opening each one.
# ---------------------------------------------------------------------------


class TestCompletionReportSubject:
    def test_carries_bucket_count_and_verdict(self):
        report = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="COMPLETE")]
        )
        subject = format_completion_report_subject(report)
        assert "my-bucket" in subject
        assert "1 object," in subject
        assert "no failures" in subject

    def test_flags_action_needed_when_something_did_not_replicate(self):
        report = build_completion_report(
            "my-bucket",
            [
                make_obj(object_key="a.txt", replication_outcome="COMPLETE"),
                make_obj(object_key="b.txt", replication_outcome="FAILED"),
            ],
        )
        assert "action needed" in format_completion_report_subject(report)

    def test_unknown_outcome_is_action_needed(self):
        report = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="UNKNOWN")]
        )
        assert "action needed" in format_completion_report_subject(report)

    def test_within_sns_length_limit_for_a_long_bucket_name(self):
        """SNS rejects a subject of 101 characters or more; 100 is accepted."""
        report = build_completion_report(
            "a" * 63, [make_obj(replication_outcome="FAILED")]
        )
        subject = format_completion_report_subject(report)
        assert len(subject) <= 100

    def test_truncation_preserves_the_verdict(self):
        """The bucket name is sacrificed, never the reason to open the mail."""
        report = build_completion_report(
            "b" * 63, [make_obj(replication_outcome="FAILED")]
        )
        subject = format_completion_report_subject(report)
        assert subject.endswith("action needed")
        assert "..." in subject

    def test_is_ascii_and_single_line(self):
        """SNS rejects a non-ASCII subject or one containing a newline, which
        would lose the notification entirely."""
        report = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="UNKNOWN")]
        )
        subject = format_completion_report_subject(report)
        subject.encode("ascii")  # raises if non-ASCII
        assert "\n" not in subject

    def test_tolerates_a_report_missing_its_fields(self):
        """Stays total: a malformed report must not break the publish."""
        subject = format_completion_report_subject({})
        assert subject
        subject.encode("ascii")



# ---------------------------------------------------------------------------
# No stored-item count in the payload. format_version 2's "outstanding" field is
# removed and nothing replaces its name. An earlier draft of 1.1.0 carried an
# "outstanding_items" count of resolved items awaiting quiescence; it was removed
# before release because it is always zero in any report a subscriber receives.
# Quiescence is keyed per bucket, so every item is tested against the same
# ScanState, and a report is only built when at least one item is publishable — a
# run that matched anything publishes nothing, and a run that matched nothing makes
# everything publishable. See build_completion_report's docstring.
# ---------------------------------------------------------------------------


class TestNoStoredItemCount:
    def test_the_old_field_name_is_gone(self):
        """A subscriber parsing `outstanding` sees format_version 3 and knows to
        look again, rather than reading a field whose meaning changed silently."""
        report = build_completion_report("my-bucket", [make_obj()])
        assert "outstanding" not in report

    def test_no_stored_item_count_under_any_name(self):
        """Nothing took `outstanding`'s place. A count of stored items cannot
        answer the question it answered, because an object enters tracking only
        after its job's report has been read."""
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=0
        )
        assert "outstanding_items" not in report
        assert [k for k in report if k.startswith("outstanding")] == [
            "outstanding_jobs"
        ]

    def test_the_summary_makes_no_claim_about_items_in_tracking(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=0
        )
        assert "in tracking" not in report["summary"]
        assert "still tracking" not in format_completion_report_subject(report)

    def test_build_completion_report_rejects_the_removed_argument(self):
        """Kept so a caller reintroducing the field fails loudly rather than
        having the keyword silently absorbed."""
        with pytest.raises(TypeError):
            build_completion_report(
                "my-bucket", [make_obj()], outstanding_items=5,
            )


# ---------------------------------------------------------------------------
# report["outstanding_jobs"] and report["submission_deferred"] — the fields
# that answer "is replication still in progress". No count of stored items can:
# an object enters tracking only after its job's report has been read, so by the
# time it is counted the question is already settled for it.
# ---------------------------------------------------------------------------


class TestOutstandingJobs:
    def test_both_fields_are_always_present(self):
        """The keys always exist, so a subscriber can read them without guarding.
        outstanding_jobs uses null for unknown rather than being omitted, which is
        what lets "unknown" be told apart from "zero"."""
        report = build_completion_report("my-bucket", [make_obj()])
        assert "outstanding_jobs" in report
        assert report["outstanding_jobs"] is None
        assert report["submission_deferred"] is False

    def test_an_unknown_count_is_null_rather_than_zero(self):
        """A bucket that failed before its jobs could be checked, or was skipped as
        disabled, has no count. Zero would be a claim, and a bucket is disabled
        because its jobs kept failing — the worst place for a false all-clear."""
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=None
        )
        assert report["outstanding_jobs"] is None
        assert "remain outstanding" not in report["summary"]

    def test_an_unknown_count_states_nothing_rather_than_guessing(self):
        """The null in the payload carries the reason; the summary does not
        over-explain an edge case to someone reading an email, and above all does
        not describe the bucket as clear."""
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=None
        )
        assert report["summary"].endswith("No action needed.")
        assert "outstanding" not in report["summary"]
        assert "unknown" not in report["summary"]

    def test_an_unknown_count_adds_no_subject_marker(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=None
        )
        assert "running" not in format_completion_report_subject(report)

    def test_the_count_is_reported(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=2
        )
        assert report["outstanding_jobs"] == 2

    def test_the_deferred_flag_is_reported(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], submission_deferred=True
        )
        assert report["submission_deferred"] is True

    def test_a_running_job_suppresses_the_all_clear_clause(self):
        """The specific false reassurance this exists to remove: zero items in
        tracking while a job is still replicating is not completeness."""
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=1
        )
        assert "remain outstanding" not in report["summary"]
        assert report["summary"].endswith("1 replication job is still running.")

    def test_a_known_zero_earns_the_all_clear(self):
        """The only thing that does. Unknown and non-zero both withhold it."""
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=0
        )
        assert report["summary"].endswith(
            "No replication jobs remain outstanding."
        )

    def test_plural_jobs_are_thousands_separated(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=1234
        )
        assert report["summary"].endswith("1,234 replication jobs are still running.")

    def test_the_jobs_clause_follows_the_verdict_rather_than_replacing_it(self):
        items = [
            make_obj(object_key="a.txt", replication_outcome="COMPLETE"),
            make_obj(object_key="b.txt", replication_outcome="FAILED"),
        ]
        report = build_completion_report("my-bucket", items, outstanding_jobs=2)
        summary = report["summary"]
        assert "Action needed" in summary
        assert "2 replication jobs are still running." in summary
        assert summary.index("Action needed") < summary.index("still running")
        assert "\n" not in summary

    def test_an_empty_report_still_reads_coherently(self):
        """An empty batch is not published, but the summary must still make sense
        if one is ever built."""
        report = build_completion_report("my-bucket", [], outstanding_jobs=0)
        assert report["summary"] == (
            "my-bucket: no objects to report."
            " No replication jobs remain outstanding."
        )


class TestOutstandingCountInSubject:
    def test_zero_adds_no_marker_so_a_bare_subject_means_the_wave_is_done(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=0
        )
        subject = format_completion_report_subject(report)
        assert "running" not in subject
        assert "still tracking" not in subject

    def test_omitted_count_adds_no_marker(self):
        report = build_completion_report("my-bucket", [make_obj()])
        assert "still tracking" not in format_completion_report_subject(report)

    def test_a_running_job_is_flagged_so_a_bare_subject_cannot_mislead(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=2
        )
        assert "2 jobs running" in format_completion_report_subject(report)

    def test_one_running_job_uses_the_singular(self):
        report = build_completion_report(
            "my-bucket", [make_obj()], outstanding_jobs=1
        )
        assert "1 job running" in format_completion_report_subject(report)

    def test_subject_stays_within_the_sns_limit_with_a_long_bucket_and_marker(self):
        report = build_completion_report(
            "a" * 200, [make_obj() for _ in range(1000)],
            outstanding_jobs=10,
        )
        subject = format_completion_report_subject(report)
        assert len(subject) <= 100
        assert "10 jobs running" in subject
        assert subject.isascii()
        assert "\n" not in subject


# ---------------------------------------------------------------------------
# Report chunking
# ---------------------------------------------------------------------------


class TestReportChunking:
    """Preserve the SNS report-size guards independently of HEAD tracking."""

    def _objs(self, count: int, key_length: int = 40) -> list[TrackedObject]:
        return [
            make_obj(object_key=f"{index:0{key_length}d}", version_id=f"v{index}")
            for index in range(count)
        ]

    def _group_objs(self, group_count: int, per_group: int = 1, name_length: int = 20):
        return [
            make_obj(
                object_key=f"{group}-{index}.txt",
                version_id=f"v{index}",
                matched_rules=frozenset({f"rule-{group:0{name_length}d}"}),
                destinations=frozenset({f"dest-{group:0{name_length}d}"}),
            )
            for group in range(group_count)
            for index in range(per_group)
        ]

    def test_empty_input_yields_no_batches(self):
        from src.core.completion_tracker import chunk_items_for_report

        assert chunk_items_for_report([]) == []

    def test_small_run_is_a_single_batch(self):
        from src.core.completion_tracker import chunk_items_for_report

        assert len(chunk_items_for_report(self._objs(10))) == 1

    def test_every_item_appears_exactly_once(self):
        from src.core.completion_tracker import chunk_items_for_report

        objects = self._objs(5_000)
        batched = [item for batch in chunk_items_for_report(objects) for item in batch]
        assert len(batched) == len(objects)
        assert [id(item) for item in batched] == [id(item) for item in objects]

    def test_each_batch_fits_the_sns_message_limit(self):
        from src.core.completion_tracker import chunk_items_for_report

        for batch in chunk_items_for_report(self._objs(5_000)):
            body = json.dumps(build_completion_report("my-bucket", batch), indent=2)
            assert len(body) < 262_144

    def test_object_count_alone_never_splits_a_batch(self):
        from src.core.completion_tracker import chunk_items_for_report

        assert len(chunk_items_for_report(self._objs(5_000, key_length=10))) == 1
        assert len(chunk_items_for_report(self._objs(5_000, key_length=200))) == 1

    def test_many_groups_split_into_multiple_batches_without_loss(self):
        from src.core.completion_tracker import chunk_items_for_report

        objects = self._group_objs(group_count=1_200)
        batches = chunk_items_for_report(objects)
        assert len(batches) > 1
        assert [id(item) for batch in batches for item in batch] == [id(item) for item in objects]

    def test_many_groups_each_batch_fits_the_sns_message_limit(self):
        from src.core.completion_tracker import chunk_items_for_report

        for batch in chunk_items_for_report(self._group_objs(group_count=1_200)):
            body = json.dumps(build_completion_report("my-bucket", batch), indent=2)
            assert len(body) < 262_144

    def test_a_group_is_never_split_across_batches(self):
        from src.core.completion_tracker import chunk_items_for_report

        batches = chunk_items_for_report(self._group_objs(group_count=1_200, per_group=3))
        seen: set[tuple[str, ...]] = set()
        for batch in batches:
            group_keys = {tuple(sorted(item.matched_rules)) for item in batch}
            assert seen.isdisjoint(group_keys)
            seen |= group_keys

    def test_single_oversized_group_is_emitted_alone(self):
        from src.core.completion_tracker import chunk_items_for_report

        batches = chunk_items_for_report(self._objs(1), max_group_bytes=1)
        assert len(batches) == 1
        assert len(batches[0]) == 1
