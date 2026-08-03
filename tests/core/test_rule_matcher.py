"""Unit tests for src/core/rule_matcher.py.

Covers the acceptance criteria for Requirements 5.1–5.7 and 9.2:
- Source-bucket filtering (5.1, 5.6)
- Tag-filter matching — all pairs required (5.2)
- Prefix matching — exact, absent, near-miss (5.3)
- Empty result when no rule satisfied (5.4)
- Multiple matching rules → one MatchedObject per config_id with union of rule IDs (5.5)
- Indeterminate tag set → error indication, empty result, continue (5.7)
- Determinism / idempotency (9.2)
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.core.models import (
    DestinationRef,
    DerivedReplicationRule,
    MatchedObject,
    TaggingOperation,
)
from src.core.rule_matcher import MatchError, match

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
_DEST = DestinationRef(bucket_arn="arn:aws:s3:::dest-bucket")
_ROLE = "arn:aws:iam::123456789012:role/rep-role"


def make_op(
    source_bucket: str = "src-bucket",
    object_key: str = "path/to/obj.txt",
    resulting_tag_set: object = None,
    sequence_number: str = "seq-1",
) -> TaggingOperation:
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key=object_key,
        resulting_tag_set=resulting_tag_set if resulting_tag_set is not None else {"env": "prod"},
        sequence_number=sequence_number,
        operation="PutObjectTagging",
        event_time=_NOW,
    )


def make_rule(
    source_bucket: str = "src-bucket",
    rule_id: str = "rule-1",
    replication_config_id: str = "cfg-1",
    tag_filter: dict[str, str] | None = None,
    key_prefix: str | None = None,
) -> DerivedReplicationRule:
    return DerivedReplicationRule(
        source_bucket=source_bucket,
        replication_config_id=replication_config_id,
        rule_id=rule_id,
        tag_filter=tag_filter or {"env": "prod"},
        destination=_DEST,
        replication_role_arn=_ROLE,
        key_prefix=key_prefix,
    )


# ---------------------------------------------------------------------------
# Req 5.6 — no matching source bucket → empty set, no error
# ---------------------------------------------------------------------------


class TestSourceBucketFiltering:
    def test_no_rules_for_source_bucket_returns_empty(self):
        """No rule has a matching source bucket → empty set (Req 5.6)."""
        op = make_op(source_bucket="bucket-a")
        rules = [make_rule(source_bucket="bucket-b")]
        matched, errors = match(op, rules)
        assert matched == set()
        assert errors == []

    def test_empty_rule_list_returns_empty(self):
        matched, errors = match(make_op(), [])
        assert matched == set()
        assert errors == []

    def test_only_matching_source_bucket_rules_are_considered(self):
        """Rules for other buckets are ignored even when tag filter would match."""
        op = make_op(source_bucket="bucket-a", resulting_tag_set={"env": "prod"})
        rules = [
            make_rule(source_bucket="bucket-a", rule_id="r1"),   # matches bucket
            make_rule(source_bucket="bucket-b", rule_id="r2"),   # different bucket — ignored
        ]
        matched, errors = match(op, rules)
        assert len(matched) == 1
        m = next(iter(matched))
        assert frozenset(["r1"]) == m.matched_rule_ids


# ---------------------------------------------------------------------------
# Req 5.2 — tag-filter matching
# ---------------------------------------------------------------------------


class TestTagFilterMatching:
    def test_exact_tag_match(self):
        """All filter tags present in tag set → match (Req 5.2)."""
        op = make_op(resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"})]
        matched, errors = match(op, rules)
        assert len(matched) == 1
        assert errors == []

    def test_tag_set_superset_of_filter_matches(self):
        """Extra tags on the object do not prevent a match (Req 5.2)."""
        op = make_op(resulting_tag_set={"env": "prod", "tier": "hot", "region": "us-east-1"})
        rules = [make_rule(tag_filter={"env": "prod"})]
        matched, errors = match(op, rules)
        assert len(matched) == 1

    def test_all_filter_pairs_required(self):
        """Every tag in the filter must be present — partial match fails (Req 5.2)."""
        op = make_op(resulting_tag_set={"env": "prod"})           # missing "tier"
        rules = [make_rule(tag_filter={"env": "prod", "tier": "hot"})]
        matched, errors = match(op, rules)
        assert matched == set()

    def test_wrong_tag_value_does_not_match(self):
        """Tag key present but wrong value → no match (Req 5.2)."""
        op = make_op(resulting_tag_set={"env": "staging"})
        rules = [make_rule(tag_filter={"env": "prod"})]
        matched, errors = match(op, rules)
        assert matched == set()

    def test_empty_tag_set_no_match(self):
        """Empty resulting tag set satisfies no non-empty filter."""
        op = make_op(resulting_tag_set={})
        rules = [make_rule(tag_filter={"env": "prod"})]
        matched, errors = match(op, rules)
        assert matched == set()


# ---------------------------------------------------------------------------
# Req 5.3 — key prefix matching
# ---------------------------------------------------------------------------


class TestPrefixMatching:
    def test_no_prefix_matches_any_key(self):
        """No key_prefix → prefix constraint is absent; key is irrelevant (Req 5.3)."""
        op = make_op(object_key="any/path/object.txt", resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"}, key_prefix=None)]
        matched, errors = match(op, rules)
        assert len(matched) == 1

    def test_key_begins_with_prefix_matches(self):
        """Object key starts with prefix → match (Req 5.3)."""
        op = make_op(object_key="logs/2024/file.gz", resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"}, key_prefix="logs/")]
        matched, errors = match(op, rules)
        assert len(matched) == 1

    def test_key_equals_prefix_exactly_matches(self):
        """Object key that equals the prefix (no suffix) still matches (Req 5.3)."""
        op = make_op(object_key="logs/", resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"}, key_prefix="logs/")]
        matched, errors = match(op, rules)
        assert len(matched) == 1

    def test_near_miss_prefix_does_not_match(self):
        """Key that is a strict prefix of the required prefix → no match (Req 5.3)."""
        op = make_op(object_key="log/2024/file.gz", resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"}, key_prefix="logs/")]
        matched, errors = match(op, rules)
        assert matched == set()

    def test_prefix_mismatch_prevents_match(self):
        """Key that does not start with the required prefix → no match (Req 5.3)."""
        op = make_op(object_key="data/file.txt", resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"}, key_prefix="logs/")]
        matched, errors = match(op, rules)
        assert matched == set()

    def test_tag_matches_but_prefix_fails(self):
        """Both conditions required: tag match AND prefix match (Req 5.3)."""
        op = make_op(object_key="other/path", resulting_tag_set={"env": "prod"})
        rules = [make_rule(tag_filter={"env": "prod"}, key_prefix="logs/")]
        matched, errors = match(op, rules)
        assert matched == set()


# ---------------------------------------------------------------------------
# Req 5.4 — no match → empty result
# ---------------------------------------------------------------------------


class TestNoMatch:
    def test_no_satisfied_rule_returns_empty_set(self):
        """When no rule is satisfied, the matched set is empty (Req 5.4)."""
        op = make_op(resulting_tag_set={"env": "staging"})
        rules = [make_rule(tag_filter={"env": "prod"})]
        matched, errors = match(op, rules)
        assert matched == set()
        assert errors == []


# ---------------------------------------------------------------------------
# Req 5.5 — multiple rules / multiple configs
# ---------------------------------------------------------------------------


class TestMultipleRulesAndConfigs:
    def test_multiple_rules_same_config_consolidated(self):
        """Multiple matching rules in the same config → one MatchedObject (Req 5.5)."""
        op = make_op(resulting_tag_set={"env": "prod", "tier": "hot"})
        rules = [
            make_rule(rule_id="r1", replication_config_id="cfg-1", tag_filter={"env": "prod"}),
            make_rule(rule_id="r2", replication_config_id="cfg-1", tag_filter={"tier": "hot"}),
        ]
        matched, errors = match(op, rules)
        assert len(matched) == 1
        m = next(iter(matched))
        assert m.replication_config_id == "cfg-1"
        assert m.matched_rule_ids == frozenset(["r1", "r2"])

    def test_rules_across_configs_produce_separate_matched_objects(self):
        """Rules from different configs each produce their own MatchedObject (Req 5.5)."""
        op = make_op(resulting_tag_set={"env": "prod"})
        rules = [
            make_rule(rule_id="r1", replication_config_id="cfg-1", tag_filter={"env": "prod"}),
            make_rule(rule_id="r2", replication_config_id="cfg-2", tag_filter={"env": "prod"}),
        ]
        matched, errors = match(op, rules)
        assert len(matched) == 2
        config_ids = {m.replication_config_id for m in matched}
        assert config_ids == {"cfg-1", "cfg-2"}

    def test_partial_rule_satisfaction_only_satisfied_rules_appear(self):
        """Only rules that fully satisfy the filter appear in results."""
        op = make_op(resulting_tag_set={"env": "prod"})  # missing "tier"
        rules = [
            make_rule(rule_id="r1", tag_filter={"env": "prod"}),          # matches
            make_rule(rule_id="r2", tag_filter={"env": "prod", "tier": "hot"}),  # doesn't match
        ]
        matched, errors = match(op, rules)
        assert len(matched) == 1
        m = next(iter(matched))
        assert m.matched_rule_ids == frozenset(["r1"])

    def test_matched_object_fields_correct(self):
        """MatchedObject carries correct source_bucket and object_key (Req 5.5)."""
        op = make_op(
            source_bucket="my-src-bucket",
            object_key="images/photo.jpg",
            resulting_tag_set={"env": "prod"},
        )
        rules = [make_rule(source_bucket="my-src-bucket", tag_filter={"env": "prod"})]
        matched, errors = match(op, rules)
        assert len(matched) == 1
        m = next(iter(matched))
        assert m.source_bucket == "my-src-bucket"
        assert m.object_key == "images/photo.jpg"


# ---------------------------------------------------------------------------
# Req 5.7 — indeterminate resulting tag set
# ---------------------------------------------------------------------------


class TestIndeterminateTagSet:
    def test_none_tag_set_returns_error_and_empty_matched(self):
        """None tag set → empty matched set + MatchError (Req 5.7)."""
        op = TaggingOperation(
            source_bucket="src-bucket",
            object_key="some/key",
            resulting_tag_set=None,  # type: ignore[arg-type]  # simulating bad runtime data
            sequence_number="seq-1",
            operation="PutObjectTagging",
            event_time=_NOW,
        )
        matched, errors = match(op, [make_rule()])
        assert matched == set()
        assert len(errors) == 1
        assert isinstance(errors[0], MatchError)
        assert errors[0].source_bucket == "src-bucket"
        assert errors[0].object_key == "some/key"

    def test_non_dict_tag_set_returns_error_and_empty_matched(self):
        """Non-dict tag set (e.g. a string) → MatchError (Req 5.7)."""
        op = TaggingOperation(
            source_bucket="src-bucket",
            object_key="some/key",
            resulting_tag_set="not-a-dict",  # type: ignore[arg-type]
            sequence_number="seq-1",
            operation="PutObjectTagging",
            event_time=_NOW,
        )
        matched, errors = match(op, [make_rule()])
        assert matched == set()
        assert len(errors) == 1
        assert "str" in errors[0].reason

    def test_indeterminate_does_not_prevent_subsequent_ops(self):
        """Error for one operation does not prevent others from being evaluated."""
        bad_op = TaggingOperation(
            source_bucket="src-bucket",
            object_key="bad/key",
            resulting_tag_set=None,  # type: ignore[arg-type]
            sequence_number="seq-1",
            operation="PutObjectTagging",
            event_time=_NOW,
        )
        good_op = make_op(object_key="good/key", resulting_tag_set={"env": "prod"})
        rules = [make_rule()]

        bad_matched, bad_errors = match(bad_op, rules)
        good_matched, good_errors = match(good_op, rules)

        # Bad op: empty match + error.
        assert bad_matched == set()
        assert len(bad_errors) == 1
        # Good op: matched, no error — processing continued independently.
        assert len(good_matched) == 1
        assert good_errors == []


# ---------------------------------------------------------------------------
# Req 9.2 — determinism / idempotency
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_input_same_output(self):
        """Calling match twice with identical inputs returns the same result (Req 9.2)."""
        op = make_op(resulting_tag_set={"env": "prod"})
        rules = [make_rule()]
        r1, e1 = match(op, rules)
        r2, e2 = match(op, rules)
        assert r1 == r2
        assert e1 == e2

    def test_rule_order_does_not_affect_result(self):
        """Result is independent of the order of rules in the list (Req 9.2)."""
        op = make_op(resulting_tag_set={"env": "prod", "tier": "hot"})
        rule_a = make_rule(rule_id="r1", tag_filter={"env": "prod"})
        rule_b = make_rule(rule_id="r2", tag_filter={"tier": "hot"})

        matched_fwd, _ = match(op, [rule_a, rule_b])
        matched_rev, _ = match(op, [rule_b, rule_a])

        assert matched_fwd == matched_rev
        # Verify rule IDs are both present regardless of order.
        ids_fwd = next(iter(matched_fwd)).matched_rule_ids
        ids_rev = next(iter(matched_rev)).matched_rule_ids
        assert ids_fwd == ids_rev

    def test_repeated_calls_with_no_match_are_stable(self):
        """Repeated calls producing empty results are stable (Req 9.2)."""
        op = make_op(resulting_tag_set={"env": "staging"})
        rules = [make_rule(tag_filter={"env": "prod"})]
        for _ in range(5):
            matched, errors = match(op, rules)
            assert matched == set()
            assert errors == []


# ---------------------------------------------------------------------------
# Property 2: Tag-filter and prefix matching (task 4.2)
# Property 3: Idempotent matching (task 4.3)
# Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
# Feature: tag-based-s3-replication, Property 3: Idempotent matching
# ---------------------------------------------------------------------------

from hypothesis import assume, given, settings
from hypothesis import strategies as st

# Strategies shared across property tests.
_TAG_KEY_ST = st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnop")
_TAG_VALUE_ST = st.text(min_size=1, max_size=10, alphabet="0123456789")
_BUCKET_ST = st.from_regex(r"^[a-z][a-z]{3,10}$", fullmatch=True)
_KEY_CHAR_ST = st.text(min_size=1, max_size=30, alphabet="abcdefghij/.-_")
_ROLE = "arn:aws:iam::123456789012:role/rep-role"
_DEST = "arn:aws:s3:::dest-bucket"


def _make_rule_from_parts(
    source_bucket: str,
    tag_filter: dict,
    key_prefix=None,
    rule_id: str = "rule-1",
    config_id: str = "cfg-1",
):
    from src.core.models import DestinationRef, DerivedReplicationRule

    return DerivedReplicationRule(
        source_bucket=source_bucket,
        replication_config_id=config_id,
        rule_id=rule_id,
        tag_filter=tag_filter,
        destination=DestinationRef(bucket_arn=_DEST),
        replication_role_arn=_ROLE,
        key_prefix=key_prefix,
    )


def _make_op_from_parts(source_bucket: str, object_key: str, tag_set: dict, seq: str = "s1"):
    import datetime
    from src.core.models import TaggingOperation

    return TaggingOperation(
        source_bucket=source_bucket,
        object_key=object_key,
        resulting_tag_set=tag_set,
        sequence_number=seq,
        operation="PutObjectTagging",
        event_time=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    )


class TestProperty2TagFilterAndPrefixMatching:
    """An object is matched for exactly those rules where the conditions hold.

    # Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
    Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5, 5.6
    """

    @given(
        bucket=_BUCKET_ST,
        filter_tags=st.fixed_dictionaries({"env": st.just("prod")}),
        extra_tags=st.dictionaries(_TAG_KEY_ST, _TAG_VALUE_ST, min_size=0, max_size=3),
        key=_KEY_CHAR_ST,
    )
    @settings(max_examples=100)
    def test_object_matched_when_filter_subset_of_tag_set(
        self, bucket: str, filter_tags: dict, extra_tags: dict, key: str
    ) -> None:
        """Filter ⊆ tag_set → object is matched (Req 5.2).

        # Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
        """
        from src.core.rule_matcher import match

        # Build tag set that is a superset of the filter.
        full_tags = {**filter_tags, **extra_tags}
        op = _make_op_from_parts(bucket, key, full_tags)
        rule = _make_rule_from_parts(bucket, filter_tags)
        matched, errors = match(op, [rule])
        assert len(matched) == 1
        assert errors == []

    @given(
        bucket=_BUCKET_ST,
        required_key=_TAG_KEY_ST,
        required_value=_TAG_VALUE_ST,
        wrong_value=_TAG_VALUE_ST,
        key=_KEY_CHAR_ST,
    )
    @settings(max_examples=100)
    def test_object_not_matched_when_tag_value_wrong(
        self, bucket: str, required_key: str, required_value: str, wrong_value: str, key: str
    ) -> None:
        """Wrong tag value → no match (Req 5.2).

        # Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
        """
        from src.core.rule_matcher import match

        assume(wrong_value != required_value)
        filter_tags = {required_key: required_value}
        actual_tags = {required_key: wrong_value}
        op = _make_op_from_parts(bucket, key, actual_tags)
        rule = _make_rule_from_parts(bucket, filter_tags)
        matched, errors = match(op, [rule])
        assert matched == set()

    @given(
        bucket=_BUCKET_ST,
        prefix=_KEY_CHAR_ST,
        suffix=_KEY_CHAR_ST,
    )
    @settings(max_examples=100)
    def test_prefix_match_when_key_starts_with_prefix(
        self, bucket: str, prefix: str, suffix: str
    ) -> None:
        """Key that starts with prefix + tags satisfied → match (Req 5.3).

        # Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
        """
        from src.core.rule_matcher import match

        key = prefix + suffix
        op = _make_op_from_parts(bucket, key, {"k": "v"})
        rule = _make_rule_from_parts(bucket, {"k": "v"}, key_prefix=prefix)
        matched, errors = match(op, [rule])
        assert len(matched) == 1

    @given(
        bucket=_BUCKET_ST,
        prefix=st.text(min_size=2, max_size=15, alphabet="abcdefghij/"),
        key=st.text(min_size=1, max_size=15, alphabet="abcdefghij/"),
    )
    @settings(max_examples=100)
    def test_no_match_when_key_does_not_start_with_prefix(
        self, bucket: str, prefix: str, key: str
    ) -> None:
        """Key that does not start with prefix → no match (Req 5.3).

        # Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
        """
        from src.core.rule_matcher import match

        assume(not key.startswith(prefix))
        op = _make_op_from_parts(bucket, key, {"k": "v"})
        rule = _make_rule_from_parts(bucket, {"k": "v"}, key_prefix=prefix)
        matched, errors = match(op, [rule])
        assert matched == set()

    @given(
        op_bucket=_BUCKET_ST,
        rule_bucket=_BUCKET_ST,
        key=_KEY_CHAR_ST,
    )
    @settings(max_examples=100)
    def test_no_match_when_source_bucket_differs(
        self, op_bucket: str, rule_bucket: str, key: str
    ) -> None:
        """Different source buckets → no match (Req 5.1, 5.6).

        # Feature: tag-based-s3-replication, Property 2: Tag-filter and prefix matching
        """
        from src.core.rule_matcher import match

        assume(op_bucket != rule_bucket)
        op = _make_op_from_parts(op_bucket, key, {"k": "v"})
        rule = _make_rule_from_parts(rule_bucket, {"k": "v"})
        matched, errors = match(op, [rule])
        assert matched == set()


class TestProperty3IdempotentMatching:
    """Evaluating the same operation any number of times yields an identical set.

    # Feature: tag-based-s3-replication, Property 3: Idempotent matching
    Validates: Requirement 9.2
    """

    @given(
        bucket=_BUCKET_ST,
        key=_KEY_CHAR_ST,
        filter_tag=st.fixed_dictionaries({"k": st.just("v")}),
        n=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=100)
    def test_repeated_calls_yield_identical_results(
        self, bucket: str, key: str, filter_tag: dict, n: int
    ) -> None:
        """match(op, rules) is the same on every invocation (Req 9.2).

        # Feature: tag-based-s3-replication, Property 3: Idempotent matching
        """
        from src.core.rule_matcher import match

        op = _make_op_from_parts(bucket, key, {"k": "v"})
        rules = [_make_rule_from_parts(bucket, filter_tag)]
        first_matched, first_errors = match(op, rules)
        for _ in range(n - 1):
            subsequent_matched, subsequent_errors = match(op, rules)
            assert subsequent_matched == first_matched
            assert subsequent_errors == first_errors

    @given(
        bucket=_BUCKET_ST,
        key=_KEY_CHAR_ST,
        tag_key=_TAG_KEY_ST,
        tag_value=_TAG_VALUE_ST,
        n=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=100)
    def test_idempotent_when_no_match(
        self, bucket: str, key: str, tag_key: str, tag_value: str, n: int
    ) -> None:
        """No-match result is stable across repeated calls (Req 9.2).

        # Feature: tag-based-s3-replication, Property 3: Idempotent matching
        """
        from src.core.rule_matcher import match

        op = _make_op_from_parts(bucket, key, {tag_key: "wrong-value"})
        rules = [_make_rule_from_parts(bucket, {tag_key: tag_value})]
        for _ in range(n):
            matched, errors = match(op, rules)
            assert matched == set()
            assert errors == []

    @given(
        bucket=_BUCKET_ST,
        key=_KEY_CHAR_ST,
        n=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=100)
    def test_idempotent_rule_order_independence(
        self, bucket: str, key: str, n: int
    ) -> None:
        """Result is the same regardless of rule order (Req 9.2).

        # Feature: tag-based-s3-replication, Property 3: Idempotent matching
        """
        from src.core.rule_matcher import match

        op = _make_op_from_parts(bucket, key, {"env": "prod", "tier": "hot"})
        rule_a = _make_rule_from_parts(bucket, {"env": "prod"}, rule_id="r1", config_id="cfg-a")
        rule_b = _make_rule_from_parts(bucket, {"tier": "hot"}, rule_id="r2", config_id="cfg-b")

        for _ in range(n):
            m_fwd, e_fwd = match(op, [rule_a, rule_b])
            m_rev, e_rev = match(op, [rule_b, rule_a])
            assert m_fwd == m_rev
            assert e_fwd == e_rev
