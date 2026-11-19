"""S3 Replication exclusions (``Filter.Not`` and ``Filter.And.Not``).

Batch Replication fails an excluded object with ``SrcObjectNotEligible``, so
the Solution must keep it out of the manifest. These tests cover the three
places that decide what goes in: rule derivation, in-memory matching, and the
Athena preflight predicate, plus the adapter that reads exclusions from the
response XML so an SDK too old to model them cannot drop them.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import boto3
import pytest
from botocore.awsrequest import AWSResponse

from src.adapters import replication_config_adapter as adapter
from src.adapters.preflight_counter import build_rule_predicate
from src.core.models import (
    DerivedReplicationRule,
    DestinationRef,
    MonitoredBucket,
    TaggingOperation,
)
from src.core.rule_deriver import UnsupportedFilterError, derive_rules
from src.core.rule_matcher import match

_FIXTURE = (
    Path(__file__).parent.parent
    / "fixtures"
    / "replication"
    / "get_bucket_replication_exclusions.xml"
)
_DEST = "arn:aws:s3:::dest-bucket"


def _rule(filter_block: dict, rule_id: str = "r1", status: str = "Enabled") -> dict:
    return {
        "ID": rule_id,
        "Status": status,
        "Filter": filter_block,
        "Destination": {"Bucket": _DEST},
    }


def _derive(*rules: dict) -> list[DerivedReplicationRule]:
    return derive_rules("src", {"Rules": list(rules)})


def _op(key: str, tags: dict[str, str]) -> TaggingOperation:
    return TaggingOperation(
        source_bucket="src",
        object_key=key,
        resulting_tag_set=tags,
        sequence_number="1",
        operation="PutObjectTagging",
        event_time=datetime(2026, 11, 1, tzinfo=UTC),
        operation_version="v1",
    )


# ---------------------------------------------------------------------------
# rule_deriver
# ---------------------------------------------------------------------------


class TestDeriveExclusions:
    def test_and_not_prefix(self):
        [r] = _derive(_rule({"And": {"Tags": [{"Key": "rot", "Value": "yes"}],
                                     "Not": {"Prefix": "logs/"}}}))
        assert r.tag_filter == {"rot": "yes"}
        assert r.exclude_prefixes == ("logs/",)
        assert r.exclude_tags == ()

    def test_and_not_tag(self):
        [r] = _derive(_rule({"And": {"Tags": [{"Key": "rot", "Value": "yes"}],
                                     "Not": {"Tag": {"Key": "secret", "Value": "yes"}}}}))
        assert r.exclude_tags == (("secret", "yes"),)

    def test_and_not_or(self):
        [r] = _derive(_rule({"And": {
            "Prefix": "data/",
            "Tags": [{"Key": "rot2", "Value": "yes"}],
            "Not": {"Or": {"Prefixes": ["data/tmp/", "data/scratch/"],
                           "Tags": [{"Key": "secret", "Value": "yes"}]}},
        }}))
        assert r.key_prefix == "data/"
        assert r.exclude_prefixes == ("data/scratch/", "data/tmp/")
        assert r.exclude_tags == (("secret", "yes"),)

    def test_rule_without_exclusions_has_none(self):
        [r] = _derive(_rule({"Tag": {"Key": "rot", "Value": "yes"}}))
        assert r.exclude_prefixes == ()
        assert r.exclude_tags == ()

    def test_not_only_rule_is_not_tag_scoped(self):
        assert _derive(_rule({"Not": {"Tag": {"Key": "rot", "Value": "no"}}})) == []

    def test_unknown_filter_element_raises(self):
        with pytest.raises(UnsupportedFilterError, match=r"Filter\.And\.Not\.And"):
            _derive(_rule({"And": {"Tags": [{"Key": "rot", "Value": "yes"}],
                                   "Not": {"And": {"Prefix": "x/"}}}}))

    def test_unknown_top_level_element_raises_even_without_tags(self):
        with pytest.raises(UnsupportedFilterError, match=r"Filter\.Size"):
            _derive(_rule({"Size": "10"}))

    def test_unknown_element_in_disabled_rule_is_ignored(self):
        assert _derive(_rule({"Size": "10"}, status="Disabled")) == []


# ---------------------------------------------------------------------------
# rule_matcher
# ---------------------------------------------------------------------------


class TestMatchExclusions:
    rules = derive_rules("src", {"Rules": [
        _rule({"And": {"Tags": [{"Key": "rot", "Value": "yes"}],
                       "Not": {"Prefix": "logs/"}}}, rule_id="a"),
        _rule({"And": {"Prefix": "data/", "Tags": [{"Key": "rot2", "Value": "yes"}],
                       "Not": {"Or": {"Prefixes": ["data/tmp/"],
                                      "Tags": [{"Key": "secret", "Value": "yes"}]}}}},
              rule_id="b"),
    ]})

    @pytest.mark.parametrize("key,tags,expected", [
        ("keep/a.txt", {"rot": "yes"}, True),
        ("logs/b.txt", {"rot": "yes"}, False),
        ("data/c.txt", {"rot2": "yes"}, True),
        ("data/d.txt", {"rot2": "yes", "secret": "yes"}, False),
        ("data/d2.txt", {"rot2": "yes", "secret": "no"}, True),
        ("data/tmp/e.txt", {"rot2": "yes"}, False),
    ])
    def test_matches_batch_replication_outcome(self, key, tags, expected):
        # Expected values are the per-task results of a live Batch Replication
        # job against this configuration (d2 aside, which checks the value).
        matched, errors = match(_op(key, tags), self.rules)
        assert errors == []
        assert bool(matched) is expected

    def test_exclusion_on_one_rule_leaves_another_rule_matching(self):
        rules = derive_rules("src", {"Rules": [
            _rule({"And": {"Tags": [{"Key": "rot", "Value": "yes"}],
                           "Not": {"Prefix": "logs/"}}}, rule_id="a"),
            _rule({"Tag": {"Key": "rot", "Value": "yes"}}, rule_id="b"),
        ]})
        [m] = match(_op("logs/b.txt", {"rot": "yes"}), rules)[0]
        assert m.matched_rule_ids == frozenset({"b"})


# ---------------------------------------------------------------------------
# preflight predicate
# ---------------------------------------------------------------------------


class TestPredicateExclusions:
    def test_exclusions_are_negated_and_null_safe(self):
        rule = DerivedReplicationRule(
            source_bucket="src", replication_config_id="c", rule_id="c",
            tag_filter={"rot": "yes"}, destination=DestinationRef(bucket_arn=_DEST),
            exclude_prefixes=("lo_gs/",), exclude_tags=(("sec'ret", "yes"),),
        )
        pred = build_rule_predicate([rule])
        assert (
            "NOT (key LIKE 'lo\\_gs/%' ESCAPE '\\' OR "
            "COALESCE(element_at(object_tags, 'sec''ret') = 'yes', FALSE))"
        ) in pred

    def test_no_exclusions_emits_no_not(self):
        [rule] = _derive(_rule({"Tag": {"Key": "rot", "Value": "yes"}}))
        assert "NOT" not in build_rule_predicate([rule])


# ---------------------------------------------------------------------------
# replication_config_adapter: raw-XML filters
# ---------------------------------------------------------------------------


def _client_returning(body: bytes):
    """A real boto3 S3 client whose HTTP layer returns *body*.

    Unlike a Stubber, this runs botocore's parser and its before-parse event,
    so it exercises the same path the Lambda does with whatever botocore is
    installed.
    """
    client = boto3.client(
        "s3", region_name="us-west-2",
        aws_access_key_id="AKIDEXAMPLE", aws_secret_access_key="secret",
    )

    def respond(request, **_kwargs):
        return AWSResponse(request.url, 200, {}, _Raw(body))

    client.meta.events.register("before-send.s3.GetBucketReplication", respond)
    return client


class _Raw:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def stream(self, **_kwargs):
        yield self._body


class TestAdapterRawFilters:
    def test_exclusions_are_read_from_the_xml(self):
        client = _client_returning(_FIXTURE.read_bytes())
        rules, skips = adapter.get_replication_rules(client, MonitoredBucket(name="src", region="us-west-2"))
        assert skips == []
        by_id = {r.rule_id: r for r in rules}
        assert by_id["tag-rot-not-logs"].exclude_prefixes == ("logs/",)
        assert by_id["data-rot2-not-secret-or-tmp"].key_prefix == "data/"
        assert by_id["data-rot2-not-secret-or-tmp"].exclude_prefixes == ("data/tmp/",)
        assert by_id["data-rot2-not-secret-or-tmp"].exclude_tags == (("secret", "yes"),)

    def test_unsupported_element_skips_the_bucket_with_its_name(self):
        body = _FIXTURE.read_bytes().replace(
            b"<Not><Prefix>logs/</Prefix></Not>",
            b"<Not><Prefix>logs/</Prefix></Not><ObjectSizeGreaterThan>5</ObjectSizeGreaterThan>",
        )
        client = _client_returning(body)
        rules, [skip] = adapter.get_replication_rules(client, MonitoredBucket(name="src", region="us-west-2"))
        assert rules == []
        assert "tag-rot-not-logs" in skip.reason
        assert "Filter.And.ObjectSizeGreaterThan" in skip.reason

    def test_parse_raw_rule_filters_shapes(self):
        [(id_a, a), (id_b, b)] = adapter.parse_raw_rule_filters(_FIXTURE.read_bytes())
        assert id_a == "tag-rot-not-logs"
        assert a == {"And": {"Tags": [{"Key": "rot", "Value": "yes"}],
                             "Not": {"Prefix": "logs/"}}}
        assert b == {"And": {"Prefix": "data/",
                             "Tags": [{"Key": "rot2", "Value": "yes"}],
                             "Not": {"Or": {"Prefixes": ["data/tmp/"],
                                            "Tags": [{"Key": "secret", "Value": "yes"}]}}}}

    def test_rule_order_mismatch_is_unreadable(self):
        response = {
            "ReplicationConfiguration": {"Rules": [{"ID": "x", "Status": "Enabled"}]},
            adapter._RAW_FILTERS_KEY: [("y", None)],
        }
        with pytest.raises(ValueError, match="rule order"):
            adapter._apply_raw_filters(response)
