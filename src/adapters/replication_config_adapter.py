"""Source-side adapter for reading a bucket's S3 replication configuration.

Calls ``GetBucketReplication`` per monitored bucket, maps the response into
the parsed configuration dict that ``rule_deriver.derive_rules`` consumes, and
returns the derived tag-scoped rules together with any skip reports produced
along the way.

Report-and-skip conditions (Requirements 3.4, 3.5, 3.6, 12.5, 13.5):
- No Replication_Configuration attached to the bucket.
- Zero enabled tag-scoped replication rules in the configuration.
- Configuration is unreadable (any non-permission exception).
- Missing ``s3:GetBucketReplication`` permission (AccessDenied).
- An enabled rule's filter carries an element the Solution cannot evaluate.

Each rule's ``Filter`` is read from the response XML rather than from the
SDK's parsed output, so exclusions are seen whatever botocore version the
Lambda runtime ships (see "Raw-XML filter reading" below).

In all cases, processing of the remaining monitored buckets continues
unaffected — per-bucket fault isolation (Requirements 3.4–3.6, 12.5, 13.5).
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import botocore.exceptions

from src.core.models import DerivedReplicationRule, MonitoredBucket
from src.core.rule_deriver import (
    UnsupportedFilterError,
    count_disabled_tag_scoped_rules,
    derive_rules,
)

# Component identifier embedded in every SkipReport emitted by this adapter.
_COMPONENT = "GetBucketReplication adapter"

# Response key under which the raw-XML rule filters are attached to the parsed
# GetBucketReplication response. Not an S3 field.
_RAW_FILTERS_KEY = "_RawRuleFilters"
_RAW_HOOK_EVENT = "before-parse.s3.GetBucketReplication"
_RAW_HOOK_ID = "s3-replicate-on-tag-raw-replication-filters"


# ---------------------------------------------------------------------------
# Raw-XML filter reading
# ---------------------------------------------------------------------------
#
# The SDK parses a response against its bundled service model and silently
# drops any element the model does not declare. The python3.12 Lambda
# runtime's botocore predates S3 Replication exclusions (Filter.Not and
# Filter.And.Not), so through the SDK alone an exclusion vanishes and the rule
# looks like a plain tag filter. Reading each rule's Filter from the response
# XML makes the Solution see exactly what S3 returned, whatever the SDK
# version, and lets rule_deriver reject an element it cannot evaluate instead
# of never seeing it.


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _tag_dict(elem: ET.Element) -> dict:
    out = {"Key": "", "Value": ""}
    for child in elem:
        out[_local(child.tag)] = child.text or ""
    return out


def _parse_filter_block(elem: ET.Element, level: str) -> dict:
    """Map one Filter / And / Not / Or element to the SDK's dict shape.

    Repeated ``Tag`` elements under ``And`` and ``Or`` become ``Tags``, and
    repeated ``Prefix`` under ``Or`` becomes ``Prefixes``, as in the SDK's
    flattened lists. An element with no mapping here is kept under its own
    name so that rule_deriver reports it.
    """
    out: dict = {}
    for child in elem:
        name = _local(child.tag)
        if name == "Prefix":
            if level == "Or":
                out.setdefault("Prefixes", []).append(child.text or "")
            else:
                out["Prefix"] = child.text or ""
        elif name == "Tag":
            if level in ("And", "Or"):
                out.setdefault("Tags", []).append(_tag_dict(child))
            else:
                out["Tag"] = _tag_dict(child)
        elif name in ("And", "Not", "Or"):
            out[name] = _parse_filter_block(child, name)
        else:
            out[name] = child.text or ""
    return out


def parse_raw_rule_filters(body: bytes) -> list[tuple[str, dict | None]]:
    """Return ``(rule_id, filter)`` for each Rule in a GetBucketReplication body.

    *filter* is ``None`` for a rule with no ``Filter`` element (the legacy
    top-level ``Prefix`` form).
    """
    root = ET.fromstring(body)
    rules: list[tuple[str, dict | None]] = []
    for rule in root:
        if _local(rule.tag) != "Rule":
            continue
        rule_id = ""
        filt: dict | None = None
        for child in rule:
            name = _local(child.tag)
            if name == "ID":
                rule_id = child.text or ""
            elif name == "Filter":
                filt = _parse_filter_block(child, "Filter")
        rules.append((rule_id, filt))
    return rules


def _attach_raw_filters(response_dict, customized_response_dict, **_kwargs) -> None:
    """botocore ``before-parse`` handler: parse the body and attach the filters.

    Only a 200 body is a configuration; an error body is left to the SDK.
    """
    if response_dict.get("status_code") != 200:
        return
    customized_response_dict[_RAW_FILTERS_KEY] = parse_raw_rule_filters(
        response_dict["body"]
    )


def _register_raw_filter_hook(s3_client) -> None:
    events = getattr(getattr(s3_client, "meta", None), "events", None)
    if events is not None:
        events.register(_RAW_HOOK_EVENT, _attach_raw_filters, unique_id=_RAW_HOOK_ID)


def _apply_raw_filters(response: dict) -> dict:
    """Replace each parsed rule's Filter with the one read from the XML.

    Returns *response* unchanged when no raw filters are attached (a client
    without botocore's event system). Raises ``ValueError`` when the raw and
    parsed rule lists disagree, since then neither can be trusted.
    """
    raw = response.get(_RAW_FILTERS_KEY)
    if raw is None:
        return response
    config = response.get("ReplicationConfiguration", response)
    rules: list[dict] = config.get("Rules", [])
    if len(raw) != len(rules):
        raise ValueError(
            f"response XML has {len(raw)} rule(s), the SDK parsed {len(rules)}"
        )
    for rule, (raw_id, raw_filter) in zip(rules, raw, strict=True):
        if rule.get("ID", "") != raw_id:
            raise ValueError(
                f"rule order differs between the response XML ({raw_id!r})"
                f" and the SDK ({rule.get('ID', '')!r})"
            )
        if raw_filter is not None:
            rule["Filter"] = raw_filter
    return response


@dataclass
class SkipReport:
    """Records that a bucket was skipped and explains why.

    Produced by :func:`get_replication_rules` when a monitored bucket cannot
    contribute derived rules to the current run.

    Attributes
    ----------
    source_bucket:
        The name of the monitored bucket that was skipped.
    reason:
        Human-readable description of why the bucket was skipped.
    component:
        The adapter component that generated the report.
    """

    source_bucket: str
    reason: str
    component: str = field(default=_COMPONENT)


def get_replication_rules(
    s3_client,
    bucket: MonitoredBucket,
) -> tuple[list[DerivedReplicationRule], list[SkipReport]]:
    """Read a bucket's replication configuration and derive tag-scoped rules.

    Calls ``s3_client.get_bucket_replication(Bucket=bucket.name)`` and maps
    the response through :func:`~src.core.rule_deriver.derive_rules`.

    On any of the following conditions the bucket is skipped with a report
    rather than raising an exception, so that processing continues for the
    remaining monitored buckets (Requirements 3.4–3.6, 12.5, 13.5):

    * **No Replication_Configuration** (``ReplicationConfigurationNotFoundError``):
      The bucket has no replication configuration attached. (Req. 3.4, 13.5)
    * **Missing permission** (``AccessDenied``):
      The caller lacks ``s3:GetBucketReplication`` on the bucket.
      (Req. 12.5)
    * **Unreadable configuration** (any other exception):
      The configuration could not be read for any other reason. (Req. 3.6)
    * **Zero enabled tag-scoped rules**: The configuration exists but contains
      no ``Enabled`` replication rules that specify a tag filter. (Req. 3.5)
      When rules were excluded only for not being ``Enabled``, the skip reason
      says so, since that is a different operator action from an untagged
      configuration. (Req. 3.1)

    Parameters
    ----------
    s3_client:
        A ``boto3`` S3 client (any object exposing
        ``get_bucket_replication(Bucket=...)``).
    bucket:
        The monitored bucket whose configuration is to be read.

    Returns
    -------
    tuple[list[DerivedReplicationRule], list[SkipReport]]
        * On success: ``(rules, [])`` where ``rules`` is a non-empty list of
          derived tag-scoped replication rules.
        * On any skip condition: ``([], [SkipReport(...)])`` identifying the
          bucket and the reason it was skipped.
    """
    bucket_name = bucket.name

    # ------------------------------------------------------------------
    # 1. Call GetBucketReplication and handle all error conditions.
    # ------------------------------------------------------------------
    try:
        _register_raw_filter_hook(s3_client)
        response = _apply_raw_filters(
            s3_client.get_bucket_replication(Bucket=bucket_name)
        )
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")

        if error_code == "ReplicationConfigurationNotFoundError":
            # Req. 3.4 / 13.5 — bucket has no replication configuration.
            return [], [
                SkipReport(
                    source_bucket=bucket_name,
                    reason=(
                        "Bucket has no Replication_Configuration"
                        " (ReplicationConfigurationNotFoundError)."
                    ),
                )
            ]

        if error_code in ("AccessDenied", "403"):
            # Req. 12.5 — missing s3:GetBucketReplication permission.
            return [], [
                SkipReport(
                    source_bucket=bucket_name,
                    reason=(
                        f"Missing permission to read Replication_Configuration: {exc}"
                    ),
                )
            ]

        # Any other ClientError — treat as an unreadable configuration (Req. 3.6).
        return [], [
            SkipReport(
                source_bucket=bucket_name,
                reason=f"Unreadable Replication_Configuration: {exc}",
            )
        ]
    except Exception as exc:  # noqa: BLE001 — intentional broad catch (Req. 3.6)
        # Req. 3.6 — configuration could not be read for any other reason.
        return [], [
            SkipReport(
                source_bucket=bucket_name,
                reason=f"Unreadable Replication_Configuration: {exc}",
            )
        ]

    # ------------------------------------------------------------------
    # 2. Derive tag-scoped rules from the parsed configuration.
    # ------------------------------------------------------------------
    try:
        derived = derive_rules(bucket_name, response)
    except UnsupportedFilterError as exc:
        # The bucket is skipped rather than processed with the element
        # ignored, which would submit objects S3 will not replicate or miss
        # ones it would.
        return [], [SkipReport(source_bucket=bucket_name, reason=str(exc))]

    if not derived:
        # Req. 3.5 — configuration exists but has zero tag-scoped rules that
        # can drive replication. A rule carrying a tag filter but not Enabled
        # is excluded by derive_rules (Req. 3.1), and rule_deriver is pure and
        # logs nothing, so name that cause here rather than leaving the
        # operator to read this as an untagged configuration.
        disabled_tag_scoped = count_disabled_tag_scoped_rules(response)
        reason = (
            "Replication_Configuration contains zero enabled"
            " tag-scoped replication rules."
        )
        if disabled_tag_scoped:
            reason += (
                f" {disabled_tag_scoped} tag-scoped rule(s) were excluded for"
                " not having Status Enabled; S3 replicates nothing against a"
                " Disabled rule."
            )
        return [], [
            SkipReport(source_bucket=bucket_name, reason=reason)
        ]

    return derived, []
