"""Unit and property tests for ``build_completion_report`` (task 6.4).

Feature: source-status-completion-tracking.

This REPLACES the superseded per-destination ``build_completion_report``
test file entirely: there is now ONE aggregate ``outcome`` per item and
``destinations`` is a plain list of routing ``replication_config_id``s
(context only). The ``confidence`` field and the
``DELETED_AT_DESTINATION`` outcome are gone.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import (
    build_completion_report,
    format_completion_report_subject,
)
from src.core.models import CompletionState, ConfigContext, TrackedObject

_MANIFEST_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_NOW = datetime(2024, 1, 3, 0, 0, 0, tzinfo=timezone.utc)

_ALL_REPLICATION_OUTCOMES = ("COMPLETE", "PENDING", "FAILED", "UNKNOWN")


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
    )


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestBuildCompletionReport:
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
        assert report["items"] == []

    def test_single_config_item_has_one_element_destinations_list(self):
        obj = make_obj(configs={"cfg-1": make_config_context(replication_config_id="cfg-1")})
        report = build_completion_report("my-bucket", [obj])
        assert report["items"][0]["destinations"] == ["cfg-1"]

    def test_multi_config_item_lists_all_routing_configs(self):
        obj = make_obj(
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a"),
                "cfg-b": make_config_context(replication_config_id="cfg-b"),
            }
        )
        report = build_completion_report("my-bucket", [obj])
        assert set(report["items"][0]["destinations"]) == {"cfg-a", "cfg-b"}

    def test_single_aggregate_outcome_per_item(self):
        obj = make_obj(replication_outcome="FAILED")
        report = build_completion_report("my-bucket", [obj])
        assert report["items"][0]["outcome"] == "FAILED"

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

    def test_no_confidence_field_anywhere(self):
        obj = make_obj()
        report = build_completion_report("my-bucket", [obj])
        assert "confidence" not in report["items"][0]

    def test_object_key_and_version_id_including_none(self):
        items = [
            make_obj(object_key="a.txt", version_id="v1"),
            make_obj(object_key="b.txt", version_id=None),
        ]
        report = build_completion_report("my-bucket", items)
        assert report["items"][0]["object_key"] == "a.txt"
        assert report["items"][0]["version_id"] == "v1"
        assert report["items"][1]["object_key"] == "b.txt"
        assert report["items"][1]["version_id"] is None

    def test_items_preserve_input_order(self):
        items = [make_obj(object_key=k) for k in ("z.txt", "a.txt", "m.txt")]
        report = build_completion_report("my-bucket", items)
        assert [entry["object_key"] for entry in report["items"]] == ["z.txt", "a.txt", "m.txt"]

    def test_report_never_includes_item_outside_input_batch(self):
        item_a = make_obj(object_key="a.txt")
        report = build_completion_report("my-bucket", [item_a])
        keys = {entry["object_key"] for entry in report["items"]}
        assert keys == {"a.txt"}
        assert "b.txt" not in keys

    def test_report_never_attributes_configs_to_wrong_item(self):
        item_a = make_obj(object_key="a.txt", configs={"cfg-a": make_config_context(replication_config_id="cfg-a")})
        item_b = make_obj(object_key="b.txt", configs={"cfg-b": make_config_context(replication_config_id="cfg-b")})
        report = build_completion_report("my-bucket", [item_a, item_b])
        by_key = {entry["object_key"]: entry["destinations"] for entry in report["items"]}
        assert by_key["a.txt"] == ["cfg-a"]
        assert by_key["b.txt"] == ["cfg-b"]


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

        gone = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="GONE")]
        )["summary"]
        assert "No failures." in gone

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
            make_obj(object_key=f"{i}.txt", replication_outcome="GONE")
            for i in range(1057)
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert "1,057" in summary
        assert "1057" not in summary

    def test_singular_object_is_not_pluralised(self):
        summary = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="GONE")]
        )["summary"]
        assert "1 object " in summary
        assert "object(s)" not in summary

    def test_verb_agrees_with_a_single_object(self):
        """"1 were deleted" and "1 are still replicating" are the failure modes
        here — the count precedes the phrase, so both forms are needed."""
        gone = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="GONE")]
        )["summary"]
        assert "1 object was deleted" in gone
        assert "were deleted" not in gone

        pending = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="PENDING")]
        )["summary"]
        assert "is still replicating" in pending
        assert "are still replicating" not in pending

    def test_verb_agrees_with_many_objects(self):
        items = [
            make_obj(object_key=f"{i}.txt", replication_outcome="GONE")
            for i in range(3)
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert "3 objects were deleted" in summary

    def test_single_outcome_absorbs_the_total_without_repeating_it(self):
        """Avoids "1,057 objects — 1,057 were deleted ..."."""
        items = [
            make_obj(object_key=f"{i}.txt", replication_outcome="GONE")
            for i in range(1057)
        ]
        summary = build_completion_report("my-bucket", items)["summary"]
        assert summary == (
            "my-bucket: 1,057 objects were deleted before replication could be "
            "confirmed. No failures."
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
            build_completion_report("my-bucket", [make_obj(replication_outcome="GONE")])
        )
        assert "1 object," in one
        assert "1 objects" not in one

    def test_empty_report_still_produces_readable_summary(self):
        report = build_completion_report("my-bucket", [])
        assert report["summary"] == "my-bucket: no objects to report."

    def test_none_outcome_rendered_in_words_not_python_none(self):
        obj = make_obj(replication_outcome=None)
        summary = build_completion_report("my-bucket", [obj])["summary"]
        assert "reported no replication status" in summary
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
# single-batch-job-per-bucket, task 5.3 (design.md D4): destinations
# collapses to the per-bucket sentinel; aggregate outcome unchanged.
# ---------------------------------------------------------------------------


class TestPerBucketSentinelDestinations:
    """design.md D4 (single-batch-job-per-bucket): under the current
    one-job-per-bucket design, ``merge_completion_configs`` (task 5.1)
    merges exactly one ``ConfigContext`` per object, keyed by the per-bucket
    sentinel (the bucket's own name). ``build_completion_report`` itself is
    unchanged and generic — these tests document its behavior against both
    the current post-migration shape and the legacy multi-config shape it
    must still tolerate on read."""

    def test_post_migration_object_reports_single_element_sentinel_destinations(self):
        """A TrackedObject whose configs dict holds exactly one entry keyed
        by the bucket-name sentinel (the current, post-migration shape
        produced by merge_completion_configs) reports destinations as a
        single-element list containing that sentinel."""
        bucket_sentinel = "my-bucket"
        obj = make_obj(
            source_bucket=bucket_sentinel,
            configs={bucket_sentinel: make_config_context(replication_config_id=bucket_sentinel, job_id="job-42")},
        )
        report = build_completion_report(bucket_sentinel, [obj])
        assert report["items"][0]["destinations"] == [bucket_sentinel]

    def test_legacy_multi_rule_object_still_lists_every_config_key(self):
        """A legacy-shaped TrackedObject (mid-migration, not yet resolved
        and republished since upgrade) whose configs dict still carries
        multiple per-rule keys continues to report all of those keys —
        build_completion_report's logic is generic over configs and was
        never changed to assume exactly one entry."""
        obj = make_obj(
            configs={
                "rule-a": make_config_context(replication_config_id="rule-a"),
                "rule-b": make_config_context(replication_config_id="rule-b"),
                "rule-c": make_config_context(replication_config_id="rule-c"),
            }
        )
        report = build_completion_report("my-bucket", [obj])
        assert set(report["items"][0]["destinations"]) == {"rule-a", "rule-b", "rule-c"}

    def test_outcome_and_outcome_counts_unaffected_by_sentinel_collapse(self):
        """Requirement 3.2: the aggregate outcome (per-item and in
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
        assert report["items"][0]["outcome"] == "COMPLETE"
        assert report["items"][1]["outcome"] == "FAILED"
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
    def test_report_matches_batch_identity_and_content(
        self,
        items: list[TrackedObject],
        source_bucket: str,
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 9: A published report's grouping and content exactly match the batch of Tracked_Objects it covers"""
        report = build_completion_report(source_bucket, items)

        assert report["source_bucket"] == source_bucket
        assert report["item_count"] == len(items)
        assert len(report["items"]) == len(items)

        for item, entry in zip(items, report["items"]):
            assert entry["object_key"] == item.object_key
            assert entry["version_id"] == item.version_id
            assert entry["outcome"] == item.replication_outcome
            assert set(entry["destinations"]) == set(item.configs.keys())

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

        keys_a = {item.object_key for item in items_a}
        keys_b = {item.object_key for item in items_b}
        report_keys = {entry["object_key"] for entry in report_a["items"]}

        assert report_keys == keys_a
        assert report_keys.isdisjoint(keys_b - keys_a)

        items_a_by_key = {item.object_key: item for item in items_a}
        for entry in report_a["items"]:
            own_item = items_a_by_key[entry["object_key"]]
            assert set(entry["destinations"]) == set(own_item.configs.keys())


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

    def test_gone_alone_is_not_action_needed(self):
        """A deleted object is not a replication failure to chase."""
        report = build_completion_report(
            "my-bucket", [make_obj(replication_outcome="GONE")]
        )
        assert "no failures" in format_completion_report_subject(report)

    def test_within_sns_length_limit_for_a_long_bucket_name(self):
        """SNS rejects a subject of 100 characters or more."""
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
            "my-bucket", [make_obj(replication_outcome="GONE")]
        )
        subject = format_completion_report_subject(report)
        subject.encode("ascii")  # raises if non-ASCII
        assert "\n" not in subject

    def test_tolerates_a_report_missing_its_fields(self):
        """Stays total: a malformed report must not break the publish."""
        subject = format_completion_report_subject({})
        assert subject
        subject.encode("ascii")
