"""Rule_Matcher: pure evaluator for tag-filter and prefix matching.

Implements the Rule_Matcher component described in the design (Requirements
5.1–5.7, 9.2).  This module is a pure function over in-memory models with no
AWS dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import DerivedReplicationRule, MatchedObject, TaggingOperation


# ---------------------------------------------------------------------------
# Error indication
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchError:
    """Error indication for an unprocessable TaggingOperation.

    Recorded when the resulting tag set is indeterminate (Req 5.7).
    The error describes which operation was unprocessable and why, so
    the caller can log a structured error entry (Req 11.4).
    """

    source_bucket: str
    object_key: str
    reason: str


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def match(
    op: TaggingOperation,
    rules: list[DerivedReplicationRule],
) -> tuple[set[MatchedObject], list[MatchError]]:
    """Evaluate *op* against *rules* and return matched objects plus any errors.

    Behavior (per design/requirements):

    * Considers only rules whose ``source_bucket`` exactly equals
      ``op.source_bucket`` (Req 5.1, 5.6).
    * Designates a :class:`~src.core.models.MatchedObject` for a rule iff
      every tag key-value pair in the rule's ``tag_filter`` is present in
      ``op.resulting_tag_set`` **and** (the rule has no ``key_prefix`` **or**
      ``op.object_key`` begins with that prefix) (Req 5.2, 5.3).
    * When several rules match, designates the object for each (Req 5.5).
    * When no rule is satisfied, returns an empty set (Req 5.4).
    * When ``op.resulting_tag_set`` is indeterminate (``None`` or not a
      ``dict``), returns an empty set and records a :class:`MatchError`
      without raising, then continues (Req 5.7).
    * Multiple rules belonging to the same ``replication_config_id`` are
      consolidated into a single :class:`~src.core.models.MatchedObject`
      whose ``matched_rule_ids`` is the union of the matching rule IDs
      (Req 5.5, identity: ``(source_bucket, object_key, replication_config_id)``).
    * Deterministic: identical inputs produce identical outputs regardless
      of rule list order or repeated calls (Req 9.2).

    Args:
        op: The :class:`~src.core.models.TaggingOperation` to evaluate.
        rules: All derived replication rules available for matching.

    Returns:
        ``(matched, errors)`` where *matched* is the set of
        :class:`~src.core.models.MatchedObject` instances and *errors* is a
        list of :class:`MatchError` (non-empty only when *op* has an
        indeterminate tag set).
    """
    # Req 5.7 — exclude and record error when resulting_tag_set is indeterminate.
    if not isinstance(op.resulting_tag_set, dict):
        error = MatchError(
            source_bucket=op.source_bucket,
            object_key=op.object_key,
            reason=(
                "resulting_tag_set is indeterminate: "
                f"expected dict, got {type(op.resulting_tag_set).__name__}"
            ),
        )
        return set(), [error]

    # Req 5.1, 5.6 — consider only rules whose source bucket matches exactly.
    bucket_rules = [r for r in rules if r.source_bucket == op.source_bucket]
    if not bucket_rules:
        return set(), []

    # Evaluate each rule and group matching rule IDs by replication_config_id.
    # This consolidates multiple matching rules from the same configuration
    # into one MatchedObject (Req 5.5; identity per design §Data Models).
    matched_by_config: dict[str, set[str]] = {}  # config_id -> set[rule_id]
    for rule in bucket_rules:
        if _rule_satisfies(op.resulting_tag_set, op.object_key, rule):
            matched_by_config.setdefault(rule.replication_config_id, set()).add(rule.rule_id)

    # Req 5.4 — when no rule is satisfied, return the empty set.
    if not matched_by_config:
        return set(), []

    # Build one MatchedObject per config_id.
    matched: set[MatchedObject] = {
        MatchedObject(
            source_bucket=op.source_bucket,
            object_key=op.object_key,
            replication_config_id=config_id,
            matched_rule_ids=frozenset(rule_ids),
            version_id=op.operation_version,  # thread version ID through to manifest
            tagged_at=op.event_time,
            last_modified=op.last_modified,
        )
        for config_id, rule_ids in matched_by_config.items()
    }
    return matched, []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _rule_satisfies(
    tag_set: dict[str, str],
    object_key: str,
    rule: DerivedReplicationRule,
) -> bool:
    """Return ``True`` iff *tag_set* and *object_key* satisfy *rule*'s filter.

    Req 5.2: every required tag key-value pair must be present in *tag_set*.
    Req 5.3: when ``rule.key_prefix`` is set, *object_key* must begin with it.
    """
    # Tag filter: every required key-value pair must appear in the resulting tag set.
    for key, value in rule.tag_filter.items():
        if tag_set.get(key) != value:
            return False

    # Prefix filter: when specified, the object key must start with it.
    if rule.key_prefix is not None and not object_key.startswith(rule.key_prefix):
        return False

    return True
