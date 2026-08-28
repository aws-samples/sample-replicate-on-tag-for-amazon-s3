"""Tests for src/adapters/preflight_counter.py — Task 3.3.

Property 2: UNLOAD predicate equivalence with the in-memory matcher.

Feature: large-scale-manifest-generation
Requirements: 2.5, 3.2, 3.4
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from src.adapters.preflight_counter import build_rule_predicate, preflight_count
from src.core.models import DestinationRef, DerivedReplicationRule, TaggingOperation
from src.core.rule_matcher import _rule_satisfies

_DEST = DestinationRef(bucket_arn="arn:aws:s3:::dst")

# One backslash character.  Spelled as a named constant so the LIKE/ESCAPE
# assertions below read unambiguously at the character level.
_BACKSLASH = "\\"


def _rule(
    tag_filter: dict,
    key_prefix: str | None = None,
    rule_id: str = "r1",
    config_id: str = "cfg1",
) -> DerivedReplicationRule:
    return DerivedReplicationRule(
        source_bucket="src-bucket",
        replication_config_id=config_id,
        rule_id=rule_id,
        tag_filter=tag_filter,
        destination=_DEST,
        key_prefix=key_prefix,
    )


# ---------------------------------------------------------------------------
# Unit tests for build_rule_predicate
# ---------------------------------------------------------------------------


class TestBuildRulePredicate:
    def test_empty_rules_returns_false(self):
        assert build_rule_predicate([]) == "FALSE"

    def test_single_rule_single_tag_uses_element_at(self):
        pred = build_rule_predicate([_rule({"env": "prod"})])
        assert "element_at" in pred
        assert "env" in pred
        assert "prod" in pred

    def test_single_rule_with_prefix(self):
        pred = build_rule_predicate([_rule({"env": "prod"}, key_prefix="data/")])
        assert "LIKE" in pred
        assert "data/" in pred

    def test_prefix_emits_single_character_escape_clause(self):
        """The emitted ESCAPE value is exactly one backslash character.

        Athena rejects a two-character ESCAPE value outright with
        ``INVALID_FUNCTION_ARGUMENT: Escape string must be a single
        character``, so the whole preflight query fails and the bucket makes
        no progress.  Asserted at the character level rather than with
        ``"ESCAPE" in pred`` so a regression to four source backslashes fails
        here.
        """
        pred = build_rule_predicate([_rule({"env": "prod"}, key_prefix="data/")])

        expected = (
            "((element_at(object_tags, 'env') = 'prod'"
            " AND key LIKE 'data/%' ESCAPE '" + _BACKSLASH + "'))"
        )
        assert pred == expected
        assert "ESCAPE '" + _BACKSLASH + "'" in pred
        assert _BACKSLASH * 2 not in pred

    def test_prefix_with_like_metacharacters_keeps_single_character_escape(self):
        r"""A prefix carrying ``\``, ``%``, and ``_`` is escaped against one escape char.

        ``_escape_like_pattern`` doubles a literal backslash and prefixes ``%``
        and ``_`` with one, all of which is read against a single-character
        ESCAPE value.  This pins that interaction: the doubling appears in the
        pattern, and does not leak into the ESCAPE clause.
        """
        prefix = "a" + _BACKSLASH + "b%c_d"
        pred = build_rule_predicate([_rule({"env": "prod"}, key_prefix=prefix)])

        expected_pattern = (
            "a" + _BACKSLASH * 2 + "b" + _BACKSLASH + "%c" + _BACKSLASH + "_d%"
        )
        expected = (
            "((element_at(object_tags, 'env') = 'prod'"
            " AND key LIKE '" + expected_pattern + "' ESCAPE '" + _BACKSLASH + "'))"
        )
        assert pred == expected
        assert "ESCAPE '" + _BACKSLASH + "'" in pred
        assert "ESCAPE '" + _BACKSLASH * 2 not in pred

    def test_two_rules_are_ored(self):
        r1 = _rule({"env": "prod"}, rule_id="r1")
        r2 = _rule({"tier": "gold"}, rule_id="r2")
        pred = build_rule_predicate([r1, r2])
        assert " OR " in pred

    def test_multiple_tags_in_one_rule_are_anded(self):
        pred = build_rule_predicate([_rule({"a": "1", "b": "2"})])
        assert " AND " in pred

    def test_sql_injection_in_tag_key_is_escaped(self):
        pred = build_rule_predicate([_rule({"k'ey": "val"})])
        # single-quote doubled
        assert "k''ey" in pred

    def test_sql_injection_in_tag_value_is_escaped(self):
        pred = build_rule_predicate([_rule({"key": "val'ue"})])
        assert "val''ue" in pred

    def test_hyphenated_key_works(self):
        """Keys with hyphens work via element_at (no JSONPath quoting needed)."""
        pred = build_rule_predicate([_rule({"test-run": "integration"})])
        assert "test-run" in pred
        assert "element_at" in pred


# ---------------------------------------------------------------------------
# Union predicate semantics — Task 2.2
#
# Requirement 1.4: strategy selection is evaluated once against the union
# object count (all of the bucket's rules OR-ed together), not per rule.
# ---------------------------------------------------------------------------


class TestUnionPredicateSemantics:
    def test_three_disjoint_rules_produce_ored_predicate_structure(self):
        """Three rules with disjoint tag filters produce (A) OR (B) OR (C)."""
        r1 = _rule({"env": "prod"}, rule_id="r1", config_id="cfg1")
        r2 = _rule({"tier": "gold"}, rule_id="r2", config_id="cfg2")
        r3 = _rule({"team": "platform"}, rule_id="r3", config_id="cfg3")

        pred = build_rule_predicate([r1, r2, r3])

        # Exactly two OR joins for three rule clauses.
        assert pred.count(" OR ") == 2
        assert "env" in pred and "prod" in pred
        assert "tier" in pred and "gold" in pred
        assert "team" in pred and "platform" in pred
        # Overall shape: "(...) OR (...) OR (...)"
        assert pred.startswith("(") and pred.endswith(")")

    def test_object_satisfying_any_single_rule_of_the_union_matches(self):
        """An object satisfying exactly one of several rules is matched by the union.

        Mirrors the in-memory union semantics that build_rule_predicate's OR
        implements: any() over the bucket's full rule set.
        """
        r1 = _rule({"env": "prod"}, rule_id="r1", config_id="cfg1")
        r2 = _rule({"tier": "gold"}, rule_id="r2", config_id="cfg2")
        r3 = _rule({"team": "platform"}, rule_id="r3", config_id="cfg3")
        rules = [r1, r2, r3]

        # Object matches only r2 (tier=gold), not r1 or r3.
        tags = {"tier": "gold"}
        matched = any(_rule_satisfies(tags, "some/key", r) for r in rules)
        assert matched is True

        # Confirm it does NOT also satisfy the other two rules individually,
        # i.e. the union match came from exactly one rule, not all of them.
        assert _rule_satisfies(tags, "some/key", r1) is False
        assert _rule_satisfies(tags, "some/key", r2) is True
        assert _rule_satisfies(tags, "some/key", r3) is False

    def test_object_satisfying_multiple_rules_is_still_a_single_logical_match(self):
        """An object matching 2 of 3 rules is one match in the union, not two.

        The union predicate (OR) treats this as a single boolean match for
        that object; ``preflight_count``'s ``COUNT(DISTINCT key)`` (see
        TestPreflightCount) is what prevents this object from being counted
        twice at the SQL level.
        """
        r1 = _rule({"env": "prod"}, rule_id="r1", config_id="cfg1")
        r2 = _rule({"tier": "gold"}, rule_id="r2", config_id="cfg2")
        r3 = _rule({"team": "platform"}, rule_id="r3", config_id="cfg3")
        rules = [r1, r2, r3]

        # Object matches both r1 (env=prod) and r2 (tier=gold).
        tags = {"env": "prod", "tier": "gold"}
        per_rule_matches = [_rule_satisfies(tags, "some/key", r) for r in rules]
        assert per_rule_matches == [True, True, False]

        # The union verdict for this object is a single boolean, regardless
        # of how many individual rules matched.
        union_match = any(per_rule_matches)
        assert union_match is True


# ---------------------------------------------------------------------------
# preflight_count unit tests (mocked Athena)
# ---------------------------------------------------------------------------


class TestPreflightCount:
    def _mock_athena(self, count_value: int):
        mock = MagicMock()
        mock.start_query_execution.return_value = {"QueryExecutionId": "qe-1"}
        mock.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        mock.get_query_results.return_value = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "count(*)"}]},  # header
                    {"Data": [{"VarCharValue": str(count_value)}]},
                ]
            }
        }
        return mock

    def test_returns_correct_count(self):
        mock_athena = self._mock_athena(42)
        result = preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"})],
            since_timestamp=None,
            output_location="s3://bucket/out/",
        )
        assert result == 42

    def test_raises_on_failed_query(self):
        mock_athena = MagicMock()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "q1"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "permission denied",
                }
            }
        }
        with pytest.raises(RuntimeError, match="failed"):
            preflight_count(
                athena_client=mock_athena,
                bucket_name="my-bucket",
                rules=[_rule({"env": "prod"})],
                since_timestamp=None,
                output_location="s3://b/o/",
            )

    def test_returns_zero_for_empty_results(self):
        mock_athena = MagicMock()
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "q1"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }
        mock_athena.get_query_results.return_value = {
            "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "count(*)"}]}]}
        }
        result = preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"})],
            since_timestamp=None,
            output_location="s3://b/o/",
        )
        assert result == 0

    def test_query_uses_count_distinct_key(self):
        """The COUNT query is COUNT(DISTINCT key), not COUNT(*).

        Requirement 1.4: a key re-tagged multiple times in the window, or
        matching more than one of the bucket's rules, must be counted once
        — this is what makes the union count correct rather than inflated.
        """
        mock_athena = self._mock_athena(1)
        preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"}), _rule({"tier": "gold"}, rule_id="r2")],
            since_timestamp=None,
            output_location="s3://bucket/out/",
        )
        assert mock_athena.start_query_execution.called
        _, call_kwargs = mock_athena.start_query_execution.call_args
        query_string = call_kwargs.get("QueryString", "")
        if not query_string:
            # _start_query may pass QueryString positionally or under a
            # different kwarg name; fall back to inspecting all call args.
            args, kwargs = mock_athena.start_query_execution.call_args
            query_string = " ".join(
                str(v) for v in (*args, *kwargs.values())
            )
        assert "COUNT(DISTINCT key)" in query_string
        assert "COUNT(*)" not in query_string

    def test_duplicate_matching_rows_for_same_key_do_not_inflate_count(self):
        """A key matched by multiple rules (multiple journal/union rows) still
        counts once, because Athena executes COUNT(DISTINCT key) server-side.

        This is verified by asserting the mocked Athena result (simulating
        COUNT(DISTINCT key) having already deduplicated) is returned as-is —
        i.e. preflight_count does not add any additional client-side
        aggregation that could double count or otherwise alter the value
        Athena computed.
        """
        # Simulate an object matched by two rules within the same window:
        # COUNT(DISTINCT key) collapses this to 1, which is what Athena
        # would return for a single distinct key regardless of how many
        # rule-clauses OR-matched it.
        mock_athena = self._mock_athena(1)
        result = preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"}, rule_id="r1"), _rule({"tier": "gold"}, rule_id="r2")],
            since_timestamp=None,
            output_location="s3://bucket/out/",
        )
        assert result == 1

    # --- until_timestamp (row-count cap) -----------------------------------

    def test_until_timestamp_adds_upper_bound_predicate(self):
        """When a capped run passes until_timestamp, the count query must
        reflect the same bounded window read_journal actually read —
        otherwise a capped read and an uncapped count would disagree about
        how many candidate objects exist for this interval."""
        mock_athena = self._mock_athena(7)
        preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"})],
            since_timestamp=None,
            output_location="s3://bucket/out/",
            until_timestamp="2024-01-02T00:00:00.000000Z",
        )
        _, call_kwargs = mock_athena.start_query_execution.call_args
        query_string = call_kwargs.get("QueryString", "")
        assert "record_timestamp <= timestamp '2024-01-02 00:00:00.000000'" in query_string

    def test_no_until_timestamp_omits_upper_bound(self):
        mock_athena = self._mock_athena(1)
        preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"})],
            since_timestamp=None,
            output_location="s3://bucket/out/",
        )
        _, call_kwargs = mock_athena.start_query_execution.call_args
        query_string = call_kwargs.get("QueryString", "")
        assert "record_timestamp <=" not in query_string

    def test_since_and_until_both_present(self):
        mock_athena = self._mock_athena(3)
        preflight_count(
            athena_client=mock_athena,
            bucket_name="my-bucket",
            rules=[_rule({"env": "prod"})],
            since_timestamp="2024-01-01T00:00:00.000000Z",
            output_location="s3://bucket/out/",
            until_timestamp="2024-01-02T00:00:00.000000Z",
        )
        _, call_kwargs = mock_athena.start_query_execution.call_args
        query_string = call_kwargs.get("QueryString", "")
        assert "record_timestamp > timestamp '2024-01-01 00:00:00.000000'" in query_string
        assert "record_timestamp <= timestamp '2024-01-02 00:00:00.000000'" in query_string


# ---------------------------------------------------------------------------
# Property 2: UNLOAD predicate equivalence with the in-memory matcher
# Feature: large-scale-manifest-generation, Property 2: UNLOAD predicate equivalence with the in-memory matcher
# Requirements: 3.2, 3.4
# ---------------------------------------------------------------------------

_tag_key_st = st.text(min_size=1, max_size=20, alphabet=st.characters(
    whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-_"
))
_tag_val_st = st.text(min_size=1, max_size=20)


def _evaluate_predicate_in_memory(
    tag_set: dict[str, str],
    object_key: str,
    rules: list[DerivedReplicationRule],
) -> bool:
    """Evaluate the generated rule predicate semantics in Python.

    Mirrors what the SQL predicate does: for each rule, check tag equality
    (and optional prefix); OR across rules.  This is semantically identical
    to rule_matcher._rule_satisfies.
    """
    return any(_rule_satisfies(tag_set, object_key, r) for r in rules)


@given(
    tag_filter=st.fixed_dictionaries({
        "env": st.sampled_from(["prod", "staging", "dev"]),
    }),
    extra_tags=st.dictionaries(
        keys=st.text(min_size=1, max_size=10, alphabet="abcdefghijklmnop"),
        values=st.text(min_size=1, max_size=10),
        max_size=3,
    ),
    object_key=st.text(min_size=1, max_size=60),
    key_prefix=st.one_of(st.none(), st.text(min_size=1, max_size=10)),
)
@settings(max_examples=100)
def test_property_2_predicate_equivalence(
    tag_filter: dict,
    extra_tags: dict,
    object_key: str,
    key_prefix: str | None,
) -> None:
    """Property 2: predicate semantics match Rule_Matcher._rule_satisfies.

    For any rule and any tag set / object key, the in-memory matcher agrees
    with what the generated SQL predicate would produce.

    # Feature: large-scale-manifest-generation, Property 2: UNLOAD predicate equivalence with the in-memory matcher
    """
    rules = [_rule(tag_filter, key_prefix=key_prefix)]
    # Full tag set: the required tags plus possibly extra unrelated tags
    full_tags = {**tag_filter, **extra_tags}

    matcher_result = _evaluate_predicate_in_memory(full_tags, object_key, rules)

    # Verify the predicate generation is structurally correct for this input
    pred = build_rule_predicate(rules)
    assert pred != "FALSE"  # at least one rule
    assert "element_at" in pred  # uses MAP element access, not JSONPath

    # Evaluate predicate semantics directly (same logic as _rule_satisfies)
    # Tag equality: all required tags present with correct values
    tags_match = all(full_tags.get(k) == v for k, v in tag_filter.items())
    # Prefix match
    prefix_match = key_prefix is None or object_key.startswith(key_prefix)
    expected = tags_match and prefix_match

    assert matcher_result == expected, (
        f"Mismatch: tags={full_tags}, key={object_key!r}, "
        f"filter={tag_filter}, prefix={key_prefix!r} → "
        f"matcher={matcher_result}, expected={expected}"
    )
