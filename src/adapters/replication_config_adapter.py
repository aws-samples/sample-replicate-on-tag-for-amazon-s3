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

In all cases, processing of the remaining monitored buckets continues
unaffected — per-bucket fault isolation (Requirements 3.4–3.6, 12.5, 13.5).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import botocore.exceptions

from src.core.models import DerivedReplicationRule, MonitoredBucket
from src.core.rule_deriver import count_disabled_tag_scoped_rules, derive_rules

# Component identifier embedded in every SkipReport emitted by this adapter.
_COMPONENT = "GetBucketReplication adapter"


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
        response = s3_client.get_bucket_replication(Bucket=bucket_name)
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
    derived = derive_rules(bucket_name, response)

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
