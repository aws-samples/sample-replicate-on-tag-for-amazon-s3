"""Unit tests for src/core/rule_deriver.py — task 3.1.

Covers the core derive_rules() function for:
- Single-tag filter rules (Filter.Tag)
- AND-filter rules with multiple tags and optional prefix (Filter.And)
- Prefix-only rules excluded (Filter.Prefix)
- Rules with no Filter key excluded
- Mixed configurations: only tag-filtered rules emitted
- Field preservation: tag pairs, prefix, destination ARN, role ARN
- Full boto3 response wrapper accepted (top-level ReplicationConfiguration key)
- Inner config dict accepted directly
- Empty Rules list produces empty result
"""
from __future__ import annotations

import pytest

from src.core.models import DerivedReplicationRule, DestinationRef
from src.core.rule_deriver import derive_rules

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

_ROLE_ARN = "arn:aws:iam::123456789012:role/replication-role"
_DEST_ARN = "arn:aws:s3:::dest-bucket"
_SRC_BUCKET = "source-bucket"


def _make_config(*rules: dict) -> dict:
    """Wrap raw rule dicts in a minimal GetBucketReplication response."""
    return {
        "ReplicationConfiguration": {
            "Role": _ROLE_ARN,
            "Rules": list(rules),
        }
    }


def _make_rule(
    rule_id: str,
    filter_block: dict | None,
    dest_arn: str = _DEST_ARN,
    status: str = "Enabled",
) -> dict:
    rule: dict = {
        "ID": rule_id,
        "Status": status,
        "Destination": {"Bucket": dest_arn},
    }
    if filter_block is not None:
        rule["Filter"] = filter_block
    return rule


# ---------------------------------------------------------------------------
# Happy-path: single-tag filter (Filter.Tag)
# ---------------------------------------------------------------------------


class TestSingleTagFilter:
    def test_emits_one_rule(self):
        config = _make_config(
            _make_rule("rule-1", {"Tag": {"Key": "env", "Value": "prod"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1

    def test_tag_filter_preserved(self):
        config = _make_config(
            _make_rule("rule-1", {"Tag": {"Key": "env", "Value": "prod"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].tag_filter == {"env": "prod"}

    def test_no_prefix_when_absent(self):
        config = _make_config(
            _make_rule("rule-1", {"Tag": {"Key": "tier", "Value": "hot"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].key_prefix is None

    def test_rule_id_used_as_replication_config_id(self):
        config = _make_config(
            _make_rule("my-rule", {"Tag": {"Key": "k", "Value": "v"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].replication_config_id == "my-rule"
        assert result[0].rule_id == "my-rule"

    def test_source_bucket_preserved(self):
        config = _make_config(
            _make_rule("rule-1", {"Tag": {"Key": "k", "Value": "v"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].source_bucket == _SRC_BUCKET

    def test_destination_arn_preserved(self):
        config = _make_config(
            _make_rule("rule-1", {"Tag": {"Key": "k", "Value": "v"}}, dest_arn=_DEST_ARN)
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].destination == DestinationRef(bucket_arn=_DEST_ARN)


# ---------------------------------------------------------------------------
# The configuration's Role is not consulted
# ---------------------------------------------------------------------------


class TestRoleIsNotConsulted:
    """``Role`` no longer reaches the derived rules.

    These are the exact inputs that previously caused ``_resolve_rules`` to
    skip the bucket and mark it errored, so they are the regression cases
    proving the field is not read.
    """

    _RULE = {"Tag": {"Key": "k", "Value": "v"}}

    def test_absent_role_still_derives_rules(self):
        config = {
            "ReplicationConfiguration": {"Rules": [_make_rule("rule-1", self._RULE)]}
        }
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1
        assert result[0].rule_id == "rule-1"

    def test_malformed_role_still_derives_rules(self):
        config = {
            "ReplicationConfiguration": {
                "Role": "definitely-not-an-arn",
                "Rules": [_make_rule("rule-1", self._RULE)],
            }
        }
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1

    def test_cross_account_role_still_derives_rules(self):
        config = {
            "ReplicationConfiguration": {
                "Role": "arn:aws:iam::999988887777:role/someone-elses-role",
                "Rules": [_make_rule("rule-1", self._RULE)],
            }
        }
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1

    def test_no_derived_rule_carries_a_role_field(self):
        config = _make_config(_make_rule("rule-1", self._RULE))
        rule = derive_rules(_SRC_BUCKET, config)[0]
        assert not hasattr(rule, "replication_role_arn")


# ---------------------------------------------------------------------------
# Happy-path: AND filter with multiple tags (Filter.And.Tags)
# ---------------------------------------------------------------------------


class TestAndTagFilter:
    def test_multiple_tags_emitted(self):
        config = _make_config(
            _make_rule(
                "rule-and",
                {
                    "And": {
                        "Tags": [
                            {"Key": "env", "Value": "prod"},
                            {"Key": "tier", "Value": "hot"},
                        ]
                    }
                },
            )
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1
        assert result[0].tag_filter == {"env": "prod", "tier": "hot"}

    def test_and_filter_with_prefix_preserved(self):
        config = _make_config(
            _make_rule(
                "rule-and",
                {
                    "And": {
                        "Prefix": "logs/",
                        "Tags": [{"Key": "env", "Value": "prod"}],
                    }
                },
            )
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1
        assert result[0].key_prefix == "logs/"
        assert result[0].tag_filter == {"env": "prod"}

    def test_and_filter_without_prefix_is_none(self):
        config = _make_config(
            _make_rule(
                "rule-and",
                {"And": {"Tags": [{"Key": "k", "Value": "v"}]}},
            )
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].key_prefix is None

    def test_and_filter_with_empty_prefix_string_treated_as_none(self):
        """An empty-string prefix in And should be normalized to None."""
        config = _make_config(
            _make_rule(
                "rule-and",
                {"And": {"Prefix": "", "Tags": [{"Key": "k", "Value": "v"}]}},
            )
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result[0].key_prefix is None


# ---------------------------------------------------------------------------
# Excluded: rules with no tag filter (Req. 3.3)
# ---------------------------------------------------------------------------


class TestNoTagFilterExcluded:
    def test_prefix_only_filter_excluded(self):
        config = _make_config(
            _make_rule("rule-prefix", {"Prefix": "data/"})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result == []

    def test_no_filter_key_excluded(self):
        config = _make_config(
            _make_rule("rule-nofilter", filter_block=None)
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result == []

    def test_and_filter_prefix_only_no_tags_excluded(self):
        """Filter.And with only Prefix and no tags should be excluded."""
        config = _make_config(
            _make_rule("rule-and-prefix", {"And": {"Prefix": "data/"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert result == []

    def test_empty_rules_list_returns_empty(self):
        config = {"ReplicationConfiguration": {"Role": _ROLE_ARN, "Rules": []}}
        result = derive_rules(_SRC_BUCKET, config)
        assert result == []


# ---------------------------------------------------------------------------
# Mixed configuration: only tag-filtered rules included
# ---------------------------------------------------------------------------


class TestMixedConfiguration:
    def setup_method(self):
        self.config = _make_config(
            # Included: single tag
            _make_rule("rule-tag", {"Tag": {"Key": "env", "Value": "prod"}}),
            # Excluded: prefix only
            _make_rule("rule-prefix", {"Prefix": "data/"}),
            # Included: AND with tags
            _make_rule(
                "rule-and",
                {
                    "And": {
                        "Prefix": "logs/",
                        "Tags": [{"Key": "app", "Value": "svc"}],
                    }
                },
            ),
            # Excluded: no filter
            _make_rule("rule-none", filter_block=None),
        )

    def test_only_tag_filtered_rules_returned(self):
        result = derive_rules(_SRC_BUCKET, self.config)
        assert len(result) == 2

    def test_rule_ids_match_expected(self):
        result = derive_rules(_SRC_BUCKET, self.config)
        ids = [r.rule_id for r in result]
        assert "rule-tag" in ids
        assert "rule-and" in ids
        assert "rule-prefix" not in ids
        assert "rule-none" not in ids

    def test_ordering_preserved(self):
        """Derived rules should appear in the same order as in the configuration."""
        result = derive_rules(_SRC_BUCKET, self.config)
        assert result[0].rule_id == "rule-tag"
        assert result[1].rule_id == "rule-and"


# ---------------------------------------------------------------------------
# Input shape: full boto3 response vs. inner config dict
# ---------------------------------------------------------------------------


class TestInputShape:
    def test_full_response_with_wrapper_key(self):
        config = {
            "ReplicationConfiguration": {
                "Role": _ROLE_ARN,
                "Rules": [
                    _make_rule("r1", {"Tag": {"Key": "k", "Value": "v"}})
                ],
            },
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1

    def test_inner_config_dict_accepted_directly(self):
        config = {
            "Role": _ROLE_ARN,
            "Rules": [
                _make_rule("r1", {"Tag": {"Key": "k", "Value": "v"}})
            ],
        }
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 1

    def test_multiple_rules_all_preserved(self):
        config = _make_config(
            _make_rule("rule-a", {"Tag": {"Key": "k1", "Value": "v1"}}),
            _make_rule("rule-b", {"Tag": {"Key": "k2", "Value": "v2"}}),
            _make_rule("rule-c", {"Tag": {"Key": "k3", "Value": "v3"}}),
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert len(result) == 3
        assert [r.rule_id for r in result] == ["rule-a", "rule-b", "rule-c"]


# ---------------------------------------------------------------------------
# DerivedReplicationRule type correctness
# ---------------------------------------------------------------------------


class TestResultTypes:
    def test_returns_list_of_derived_replication_rules(self):
        config = _make_config(
            _make_rule("r1", {"Tag": {"Key": "k", "Value": "v"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert isinstance(result, list)
        assert all(isinstance(r, DerivedReplicationRule) for r in result)

    def test_destination_is_destination_ref(self):
        config = _make_config(
            _make_rule("r1", {"Tag": {"Key": "k", "Value": "v"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert isinstance(result[0].destination, DestinationRef)

    def test_tag_filter_is_dict(self):
        config = _make_config(
            _make_rule("r1", {"Tag": {"Key": "k", "Value": "v"}})
        )
        result = derive_rules(_SRC_BUCKET, config)
        assert isinstance(result[0].tag_filter, dict)
        assert len(result[0].tag_filter) >= 1


# ---------------------------------------------------------------------------
# Property 1: Rule derivation selects exactly the tag-scoped rules (task 3.2)
# Feature: tag-based-s3-replication, Property 1: Rule derivation selects exactly
#          the tag-scoped rules
# ---------------------------------------------------------------------------

from hypothesis import given, settings
from hypothesis import strategies as st


# Strategies for rule construction
_TAG_KEY_ST = st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789-")
_TAG_VALUE_ST = st.text(min_size=1, max_size=20, alphabet="abcdefghijklmnopqrstuvwxyz0123456789")
_ROLE_ARN_ST = st.just("arn:aws:iam::123456789012:role/rep-role")
_DEST_ARN_ST = st.just("arn:aws:s3:::dest-bucket")


def _build_tag_filter_rule(rule_id: str, tags: dict, prefix: str | None) -> dict:
    """Build a replication rule with a tag filter."""
    if prefix and len(tags) > 1:
        filter_block = {"And": {"Prefix": prefix, "Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}}
    elif len(tags) == 1:
        k, v = next(iter(tags.items()))
        filter_block = {"Tag": {"Key": k, "Value": v}}
    else:
        filter_block = {"And": {"Tags": [{"Key": k, "Value": v} for k, v in tags.items()]}}
    return {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": filter_block,
        "Destination": {"Bucket": "arn:aws:s3:::dest-bucket"},
    }


def _build_no_tag_rule(rule_id: str) -> dict:
    """Build a replication rule with no tag filter (prefix-only)."""
    return {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {"Prefix": "data/"},
        "Destination": {"Bucket": "arn:aws:s3:::dest-bucket"},
    }


class TestProperty1RuleDerivation:
    """Derived set contains exactly the tag-filtered rules; non-tag-filtered excluded.

    # Feature: tag-based-s3-replication, Property 1: Rule derivation selects exactly the tag-scoped rules
    Validates: Requirements 3.2, 3.3
    """

    @given(
        tag_rule_count=st.integers(min_value=1, max_value=5),
        no_tag_rule_count=st.integers(min_value=0, max_value=3),
        tags=st.fixed_dictionaries({"env": st.just("prod"), "tier": st.just("hot")}),
    )
    @settings(max_examples=100)
    def test_derived_set_contains_exactly_tag_filtered_rules(
        self,
        tag_rule_count: int,
        no_tag_rule_count: int,
        tags: dict,
    ) -> None:
        """Derived set has exactly one rule per tag-filtered input rule.

        # Feature: tag-based-s3-replication, Property 1: Rule derivation selects exactly the tag-scoped rules
        """
        from src.core.rule_deriver import derive_rules

        # Build rules: some with tags, some without.
        rules = []
        tag_rule_ids = set()
        for i in range(tag_rule_count):
            rule_id = f"tag-rule-{i}"
            tag_rule_ids.add(rule_id)
            tag_subset = dict(list(tags.items())[:1])  # use first tag pair
            rules.append(_build_tag_filter_rule(rule_id, tag_subset, None))
        for i in range(no_tag_rule_count):
            rules.append(_build_no_tag_rule(f"no-tag-rule-{i}"))

        config = {
            "ReplicationConfiguration": {
                "Role": "arn:aws:iam::123456789012:role/role",
                "Rules": rules,
            }
        }
        derived = derive_rules("src-bucket", config)

        # Derived count equals tag rule count.
        assert len(derived) == tag_rule_count

        # All derived rules come from tag-filtered rules only.
        derived_ids = {r.rule_id for r in derived}
        assert derived_ids == tag_rule_ids

    @given(
        no_tag_rule_count=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=100)
    def test_no_tag_rules_produces_empty_derived_set(
        self, no_tag_rule_count: int
    ) -> None:
        """Config with zero tag-scoped rules → empty derived set (Req 3.3).

        # Feature: tag-based-s3-replication, Property 1: Rule derivation selects exactly the tag-scoped rules
        """
        from src.core.rule_deriver import derive_rules

        rules = [_build_no_tag_rule(f"rule-{i}") for i in range(no_tag_rule_count)]
        config = {
            "ReplicationConfiguration": {
                "Role": "arn:aws:iam::123456789012:role/role",
                "Rules": rules,
            }
        }
        derived = derive_rules("src-bucket", config)
        assert derived == []

    @given(
        tag_pairs=st.dictionaries(
            st.text(min_size=1, max_size=10, alphabet="abcde"),
            st.text(min_size=1, max_size=10, alphabet="xyz012"),
            min_size=1,
            max_size=3,
        ),
        # The configuration's Role is not consulted at all: a well-formed
        # ARN, a cross-account ARN, a malformed value, and an absent key
        # (None) must all derive the same rules.
        role_arn=st.sampled_from(
            [
                "arn:aws:iam::111122223333:role/my-role",
                "arn:aws:iam::999988887777:role/other-account-role",
                "not-an-arn",
                "",
                None,
            ]
        ),
        dest_arn=st.just("arn:aws:s3:::my-dest-bucket"),
    )
    @settings(max_examples=100)
    def test_tag_filter_fields_preserved_exactly(
        self, tag_pairs: dict, role_arn: str | None, dest_arn: str
    ) -> None:
        """Tag key-value pairs and destination ARN are preserved (Req 3.2).

        # Feature: tag-based-s3-replication, Property 1: Rule derivation selects exactly the tag-scoped rules
        """
        from src.core.models import DestinationRef
        from src.core.rule_deriver import derive_rules

        rule_id = "my-rule"
        if len(tag_pairs) == 1:
            k, v = next(iter(tag_pairs.items()))
            filter_block = {"Tag": {"Key": k, "Value": v}}
        else:
            filter_block = {
                "And": {
                    "Tags": [{"Key": k, "Value": v} for k, v in tag_pairs.items()]
                }
            }
        inner: dict = {
            "Rules": [
                {
                    "ID": rule_id,
                    "Status": "Enabled",
                    "Filter": filter_block,
                    "Destination": {"Bucket": dest_arn},
                }
            ],
        }
        if role_arn is not None:
            inner["Role"] = role_arn
        config = {"ReplicationConfiguration": inner}
        derived = derive_rules("src-bucket", config)
        assert len(derived) == 1
        rule = derived[0]

        assert rule.tag_filter == tag_pairs
        assert rule.destination == DestinationRef(bucket_arn=dest_arn)

    @given(
        prefix=st.one_of(st.none(), st.text(min_size=1, max_size=20, alphabet="abcde/")),
    )
    @settings(max_examples=100)
    def test_optional_prefix_preserved_or_none(self, prefix) -> None:
        """Optional key prefix is preserved; absent prefix is None (Req 3.2).

        # Feature: tag-based-s3-replication, Property 1: Rule derivation selects exactly the tag-scoped rules
        """
        from src.core.rule_deriver import derive_rules

        if prefix:
            filter_block = {
                "And": {
                    "Prefix": prefix,
                    "Tags": [{"Key": "k", "Value": "v"}],
                }
            }
        else:
            filter_block = {"Tag": {"Key": "k", "Value": "v"}}

        config = {
            "ReplicationConfiguration": {
                "Role": "arn:aws:iam::123456789012:role/r",
                "Rules": [
                    {
                        "ID": "rule-1",
                        "Status": "Enabled",
                        "Filter": filter_block,
                        "Destination": {"Bucket": "arn:aws:s3:::d"},
                    }
                ],
            }
        }
        derived = derive_rules("src-bucket", config)
        assert len(derived) == 1
        expected_prefix = prefix if prefix else None
        assert derived[0].key_prefix == expected_prefix
