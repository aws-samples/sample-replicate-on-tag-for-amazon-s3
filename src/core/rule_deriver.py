"""Pure rule derivation logic for tag-based S3 replication.

Accepts a parsed ``GetBucketReplication`` response dict and emits one
``DerivedReplicationRule`` for every enabled replication rule that carries a
tag filter.  Rules with no tag filter are excluded per Requirement 3.3, and
rules whose ``Status`` is not ``Enabled`` are excluded because S3 will not
replicate against them: a Batch Replication task for an object matched only by
a ``Disabled`` rule fails with ``SrcObjectNotEligible`` and moves no data, so
submitting one is a billed failure that also feeds the consecutive-failure
circuit breaker.

Results are naturally grouped by ``(source_bucket, replication_config_id)``
via the fields on each returned ``DerivedReplicationRule`` — the
``replication_config_id`` equals the rule's own ``ID``, giving downstream
components (Rule_Matcher, Manifest_Generator, Batch_Job_Manager) a stable
job-grouping key (Req. 3.7).
"""
from __future__ import annotations

from src.core.models import DerivedReplicationRule, DestinationRef


def derive_rules(
    source_bucket: str,
    replication_config: dict,
) -> list[DerivedReplicationRule]:
    """Derive tag-scoped replication rules from a ``GetBucketReplication`` response.

    Parameters
    ----------
    source_bucket:
        The name of the source S3 bucket whose configuration is being read.
    replication_config:
        The dict returned by the ``s3.get_bucket_replication`` boto3 call.
        Both the full API response (top-level key ``"ReplicationConfiguration"``)
        and the inner config dict (``{"Rules": [...]}``) are accepted. The
        configuration's ``Role`` is not read: the role passed to S3 Batch
        Operations is the stack-created Batch Operations role, supplied to the
        runtime as an environment variable.

    Returns
    -------
    list[DerivedReplicationRule]
        One entry per rule in *replication_config* that is ``Enabled`` and
        specifies at least one tag key-value pair in its filter.  Rules with
        no tag filter (prefix-only or absent filter) are excluded (Req. 3.3),
        as are rules whose ``Status`` is not ``Enabled``.

        Each ``DerivedReplicationRule`` carries:

        * ``source_bucket`` — passed-in bucket name
        * ``replication_config_id`` — the rule's own ``ID`` (job-grouping key)
        * ``rule_id`` — same as ``replication_config_id``
        * ``tag_filter`` — non-empty ``{key: value}`` dict (Req. 3.2)
        * ``key_prefix`` — optional prefix from the rule's filter (Req. 3.2)
        * ``destination`` — opaque ``DestinationRef`` (Req. 3.2, 12.2)

        Results preserve the ordering of rules in the configuration and are
        grouped implicitly by ``(source_bucket, replication_config_id)``.
    """
    rules: list[dict] = _inner_config(replication_config).get("Rules", [])

    derived: list[DerivedReplicationRule] = []
    for rule in rules:
        if not is_rule_enabled(rule):
            # Status is not "Enabled" — S3 replicates nothing against this
            # rule, so exclude it (Req. 3.1).
            continue

        rule_id: str = rule.get("ID", "")
        tag_filter, key_prefix = _extract_tag_filter_and_prefix(rule)

        if not tag_filter:
            # No tag key-value pairs — exclude this rule (Req. 3.3).
            continue

        destination_bucket_arn: str = rule.get("Destination", {}).get("Bucket", "")

        derived.append(
            DerivedReplicationRule(
                source_bucket=source_bucket,
                # Each rule's ID becomes the job-grouping key (Req. 3.7).
                replication_config_id=rule_id,
                rule_id=rule_id,
                tag_filter=tag_filter,
                destination=DestinationRef(bucket_arn=destination_bucket_arn),
                key_prefix=key_prefix,
            )
        )

    return derived


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _inner_config(replication_config: dict) -> dict:
    """Accept both the full boto3 response and the inner config dict."""
    return replication_config.get("ReplicationConfiguration", replication_config)


def count_disabled_tag_scoped_rules(replication_config: dict) -> int:
    """Count rules that carry a tag filter but are excluded for not being enabled.

    This module is pure and logs nothing, so a rule dropped by
    :func:`is_rule_enabled` leaves no trace. When the dropped rule was the
    bucket's only tag-scoped rule, the bucket is skipped for the interval and
    the only operator-facing signal is the caller's skip report — which would
    otherwise say the configuration carries no tag-scoped rules at all, naming
    the wrong cause. ``replication_config_adapter`` calls this to distinguish
    the two (Req. 3.1).
    """
    count = 0
    for rule in _inner_config(replication_config).get("Rules", []):
        if is_rule_enabled(rule):
            continue
        tag_filter, _ = _extract_tag_filter_and_prefix(rule)
        if tag_filter:
            count += 1
    return count


def is_rule_enabled(rule: dict) -> bool:
    """Return ``True`` iff *rule* carries ``Status: "Enabled"`` (Req. 3.1).

    ``GetBucketReplication`` always returns ``Status`` for every rule, and the
    API accepts only ``"Enabled"`` and ``"Disabled"``. Anything else —
    including an absent ``Status`` — is treated as not enabled, so an
    unexpected rendering excludes the rule rather than driving replication
    against a rule S3 will not honour. Comparison is whitespace-tolerant and
    case-insensitive: a value differing only in case still names the enabled
    state, and reading it as disabled would drop rules the Solution is
    supposed to act on.
    """
    status = rule.get("Status")
    if not isinstance(status, str):
        return False
    return status.strip().upper() == "ENABLED"


def _extract_tag_filter_and_prefix(rule: dict) -> tuple[dict[str, str], str | None]:
    """Extract tag key-value pairs and optional key prefix from a replication rule.

    Handles the three S3 filter shapes:

    * ``Filter.Tag`` — a single ``{"Key": ..., "Value": ...}`` tag; no prefix.
    * ``Filter.And`` — AND of an optional ``Prefix`` and one or more tags in
      ``Tags`` (list) or ``Tag`` (singular, handled defensively).
    * ``Filter.Prefix`` — prefix-only; no tags.
    * No ``Filter`` key / legacy top-level ``Prefix`` — no tags.

    Returns
    -------
    tuple[dict[str, str], str | None]
        ``(tag_filter, key_prefix)`` where *tag_filter* is ``{}`` when no tags
        are present (caller should exclude the rule), and *key_prefix* is
        ``None`` when absent.
    """
    filter_block: dict = rule.get("Filter", {})

    # --- Single-tag shorthand: Filter.Tag --------------------------------
    if "Tag" in filter_block:
        tag: dict = filter_block["Tag"]
        return {tag["Key"]: tag["Value"]}, None

    # --- AND filter: Filter.And ------------------------------------------
    if "And" in filter_block:
        and_block: dict = filter_block["And"]
        prefix: str | None = and_block.get("Prefix") or None
        tags: dict[str, str] = {}

        # Multiple tags: And.Tags (list)
        for t in and_block.get("Tags", []):
            tags[t["Key"]] = t["Value"]

        # Single tag nested under And.Tag (defensive; non-standard but possible)
        if "Tag" in and_block:
            t = and_block["Tag"]
            tags[t["Key"]] = t["Value"]

        # Return whatever tags were found (may be empty if And has only Prefix).
        return tags, prefix

    # --- Prefix-only filter or no filter — no tags -----------------------
    return {}, None
