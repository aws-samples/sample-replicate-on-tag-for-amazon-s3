"""Source-side adapter for reading a bucket's S3 replication configuration.

Calls ``GetBucketReplication`` per monitored bucket, maps the response into
the parsed configuration dict that ``rule_deriver.derive_rules`` consumes, and
returns the derived tag-scoped rules together with any skip reports produced
along the way.

Report-and-skip conditions (Requirements 3.4, 3.5, 3.6, 12.5, 13.5):
- No Replication_Configuration attached to the bucket.
- Zero tag-scoped replication rules in the configuration.
- Configuration is unreadable (any non-permission exception).
- Missing ``s3:GetBucketReplication`` permission (AccessDenied).

In all cases, processing of the remaining monitored buckets continues
unaffected — per-bucket fault isolation (Requirements 3.4–3.6, 12.5, 13.5).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import botocore.exceptions

from src.core.models import DerivedReplicationRule, MonitoredBucket
from src.core.rule_deriver import derive_rules

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
    * **Zero tag-scoped rules**: The configuration exists but contains no
      replication rules that specify a tag filter. (Req. 3.5)

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
        # Req. 3.5 — configuration exists but has zero tag-scoped rules.
        return [], [
            SkipReport(
                source_bucket=bucket_name,
                reason=(
                    "Replication_Configuration contains zero"
                    " tag-scoped replication rules."
                ),
            )
        ]

    return derived, []
