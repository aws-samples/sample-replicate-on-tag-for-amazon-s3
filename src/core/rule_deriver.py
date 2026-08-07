"""Pure rule derivation logic for tag-based S3 replication.

Accepts a parsed ``GetBucketReplication`` response dict and emits one
``DerivedReplicationRule`` for every replication rule that carries a tag
filter.  Rules with no tag filter are excluded per Requirement 3.3.

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
        One entry per rule in *replication_config* that specifies at least
        one tag key-value pair in its filter.  Rules with no tag filter
        (prefix-only or absent filter) are excluded (Req. 3.3).

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
    # Accept both the full boto3 response and the inner config dict.
    inner: dict = replication_config.get("ReplicationConfiguration", replication_config)
    rules: list[dict] = inner.get("Rules", [])

    derived: list[DerivedReplicationRule] = []
    for rule in rules:
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
# Internal helper
# ---------------------------------------------------------------------------


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
