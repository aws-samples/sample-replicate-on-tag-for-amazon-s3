"""Preflight Counter — counts candidate matched objects for a bucket's window.

Issues a ``SELECT COUNT(DISTINCT key)`` Athena query with the same window
predicate and rule predicate used by the In_Memory generation path, so the
orchestrator can record the pre-delete-filter matched count for the
completion-tracker quiescence scan (see
``.kiro/specs/scale-threshold-and-drain-throughput/design.md``,
"`preflight_count` is retained, repurposed to feed quiescence only").

``build_rule_predicate`` already ORs every rule's conjuncts together, so
passing it a bucket's full rule set (rather than a single replication
config's subset) produces the "matches any of the bucket's rules" union
predicate the whole-bucket manifest needs (design.md D1 / O3). Counting
``DISTINCT key`` (not ``COUNT(*)``) keeps the result consistent with the
union manifest's dedup key of ``(source_bucket, object_key)``: an object
re-tagged more than once in the window, or matching more than one rule,
must count once, not once per journal row or per matching rule.

Reuses the Athena start/poll helpers from ``athena_journal_adapter``.

Requirements: 2.5, 3.2
"""
from __future__ import annotations

import logging

from botocore.exceptions import ClientError

from src.adapters.athena_journal_adapter import (
    _escape_like_pattern,
    _escape_sql_string,
    _get_query_failure_reason,
    _iter_result_rows,
    _poll_query,
    _start_query,
    _to_athena_timestamp_literal,
)
from src.core.models import DerivedReplicationRule

logger = logging.getLogger(__name__)

_COMPONENT = "Preflight_Counter"


# ---------------------------------------------------------------------------
# Rule-to-SQL predicate builder
# ---------------------------------------------------------------------------


def build_rule_predicate(rules: list[DerivedReplicationRule]) -> str:
    """Build an Athena WHERE predicate that implements the rule tag-filter logic.

    Produces the disjunction of per-rule conjuncts (Requirement 3.2):

    - Per tag key-value pair: ``element_at(object_tags, 'k') = 'v'``
      Using ``element_at`` on the MAP<VARCHAR, VARCHAR> ``object_tags`` column
      is more direct than CAST+json_extract_scalar and handles keys with
      special characters (hyphens, spaces) without JSONPath quoting issues.
    - Optional prefix:        ``key LIKE 'prefix%'``
    - Rules are OR-ed together; each rule's conjuncts are AND-ed.

    The generated predicate is semantically equivalent to the in-memory
    ``Rule_Matcher._rule_satisfies`` check (Property 2).

    Parameters
    ----------
    rules:
        Derived replication rules for one bucket replication configuration.
        Must be non-empty.

    Returns
    -------
    str
        An Athena SQL predicate fragment suitable for embedding in a WHERE
        clause.  Returns ``"FALSE"`` when *rules* is empty (no rule ⇒ no match).

    Requirements: 3.2, 3.4
    """
    if not rules:
        return "FALSE"

    rule_clauses: list[str] = []
    for rule in rules:
        conjuncts: list[str] = []

        # Tag equality conjuncts — element_at on MAP<VARCHAR, VARCHAR> (Req 3.2)
        # This avoids JSONPath quoting issues with CAST+json_extract_scalar.
        for tag_key, tag_value in sorted(rule.tag_filter.items()):
            escaped_key = _escape_sql_string(tag_key)
            escaped_val = _escape_sql_string(tag_value)
            conjuncts.append(
                f"element_at(object_tags, '{escaped_key}') = '{escaped_val}'"
            )

        # Optional key prefix conjunct
        if rule.key_prefix is not None:
            # Use _escape_like_pattern (not _escape_sql_string) so that % and _
            # in the prefix are treated as literals rather than LIKE wildcards.
            escaped_prefix = _escape_like_pattern(rule.key_prefix)
            conjuncts.append(f"key LIKE '{escaped_prefix}%' ESCAPE '\\\\'")

        if conjuncts:
            rule_clauses.append("(" + " AND ".join(conjuncts) + ")")
        else:
            # Rule with no tag filter or prefix — matches everything
            rule_clauses.append("TRUE")

    return "(" + " OR ".join(rule_clauses) + ")"


# ---------------------------------------------------------------------------
# Public preflight counter
# ---------------------------------------------------------------------------


def preflight_count(
    athena_client,
    bucket_name: str,
    rules: list[DerivedReplicationRule],
    since_timestamp: str | None,
    athena_workgroup: str = "primary",
    output_location: str = "",
    until_timestamp: str | None = None,
) -> int:
    """Count distinct candidate matched objects across a bucket's rules.

    Issues
    ``SELECT COUNT(DISTINCT key) FROM journal WHERE <window> AND <rule predicate>``
    and returns the integer count.

    *rules* should be the full set of rules for the bucket (every
    replication config's rules), not a single config's subset — the OR of
    those rules' conjuncts (via :func:`build_rule_predicate`) is the union
    predicate the whole-bucket manifest is built from (design.md D1 / O3),
    and ``COUNT(DISTINCT key)`` mirrors the manifest's ``(source_bucket,
    object_key)`` dedup so an object matched by multiple rules, or re-tagged
    multiple times in the window, is only counted once.

    This count reflects *distinct object keys with at least one tagging
    operation in the current lookback window* that satisfy the rule filter —
    NOT the total number of objects in the bucket, and NOT the total number
    of objects that have ever been tagged. For a bucket with 100 M objects
    that tags 50 K distinct keys per interval, this function returns 50 K.
    The 1 M default threshold is only reached during bulk-tagging operations
    (backfilling tags on millions of objects in a single interval).

    On any Athena failure, raises :class:`RuntimeError` so the orchestrator
    can leave the checkpoint unchanged (Requirement 2.5 — obtained before
    mode selection; failure leaves the interval to be retried).

    Parameters
    ----------
    athena_client:
        boto3 Athena client.
    bucket_name:
        Source bucket name.
    rules:
        Derived replication rules to count against. Pass the bucket's full
        rule set (all replication configs) to get the union count used for
        whole-bucket strategy selection (design.md D1 / O3).
    since_timestamp:
        Lookback-window start as a canonical watermark string, or ``None``
        for a first run (count all records).
    athena_workgroup:
        Athena workgroup.
    output_location:
        S3 URI for Athena query results.
    until_timestamp:
        Optional inclusive upper bound as a canonical watermark string —
        the same row-count-cap boundary passed to
        :func:`~src.adapters.athena_journal_adapter.read_journal`'s
        ``until_timestamp`` parameter for a capped run (see
        :func:`~src.adapters.athena_journal_adapter.find_row_count_boundary`).
        Must be set whenever the calling run is capped, so this count
        reflects the same bounded window ``read_journal`` actually read —
        otherwise a capped read and an uncapped count would disagree about
        how many candidate objects exist for this interval. Pass ``None``
        (the default) for an uncapped count — the common case.

    Returns
    -------
    int
        Count of distinct object keys satisfying the window and rule
        predicates.

    Raises
    ------
    RuntimeError
        On Athena start/poll/result failure.

    Requirements: 2.5, 3.2
    """
    bucket_namespace = "b_" + bucket_name.replace(".", "_")
    table_path = f'"s3tablescatalog/aws-s3"."{bucket_namespace}"."journal"'
    bucket_escaped = _escape_sql_string(bucket_name)

    rule_pred = build_rule_predicate(rules)
    window_clause = f"bucket = '{bucket_escaped}' AND record_type = 'UPDATE_METADATA'"
    if since_timestamp:
        ts = _to_athena_timestamp_literal(since_timestamp)
        window_clause += f" AND record_timestamp > timestamp '{ts}'"
    if until_timestamp:
        until_ts = _to_athena_timestamp_literal(until_timestamp)
        window_clause += f" AND record_timestamp <= timestamp '{until_ts}'"

    # COUNT(DISTINCT key) rather than COUNT(*): the query already scopes to a
    # single bucket (via the `bucket = '<bucket>'` window clause), so `key`
    # alone is the natural per-object identity here. This keeps the count
    # consistent with the union manifest's dedup key of (source_bucket,
    # object_key) — a key with multiple UPDATE_METADATA rows in the window
    # (re-tagged more than once) or matching more than one of the bucket's
    # rules must still count as one candidate object, not one per row/rule.
    query = (
        f"SELECT COUNT(DISTINCT key) FROM {table_path} "
        f"WHERE {window_clause} AND {rule_pred}"
    )

    try:
        qeid = _start_query(
            athena_client, query, athena_workgroup, output_location
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Preflight_Counter start_query failed for {bucket_name!r}: {exc}"
        ) from exc

    try:
        state = _poll_query(athena_client, qeid)
    except (ClientError, ValueError) as exc:
        raise RuntimeError(
            f"Preflight_Counter poll failed for {bucket_name!r}: {exc}"
        ) from exc

    if state in ("FAILED", "CANCELLED"):
        reason = _get_query_failure_reason(athena_client, qeid)
        raise RuntimeError(
            f"Preflight_Counter query {state.lower()} for {bucket_name!r}: {reason}"
        )

    try:
        rows = list(_iter_result_rows(athena_client, qeid))
    except ClientError as exc:
        raise RuntimeError(
            f"Preflight_Counter result fetch failed for {bucket_name!r}: {exc}"
        ) from exc

    if not rows or not rows[0]:
        return 0
    try:
        return int(rows[0][0])
    except (ValueError, IndexError):
        return 0
