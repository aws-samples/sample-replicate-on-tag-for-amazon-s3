"""State_Bucket bucket-policy adapter — pre-submission priming (Decision 10).

This is the thin I/O shell for Requirement 9's pre-submission bucket-policy
priming step: immediately before a Batch_Replication_Job is submitted for a
`replication_config_id`, the Batch_Job_Manager ensures that config's
Completion_Report_Bucket_Policy_Statement is present on the State_Bucket's
bucket policy, so the customer's replication role has write access to its
own BOPS_Completion_Report location before the job runs.

This adapter contains no diff/merge logic of its own — that decision is made
entirely by the pure `completion_tracker.ensure_completion_report_bucket_policy_statement`
function. This module is exactly the `GetBucketPolicy`/`PutBucketPolicy` I/O
shell around that decision, mirroring the thin-adapter style of
`bops_report_reader.py` and `source_status_adapter.py`.

Requirements: 9.1, 9.3, 9.4, 9.5, 9.6
"""
from __future__ import annotations

import json
import logging

from botocore.exceptions import ClientError

from src.core import completion_tracker, observability

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ensure_completion_report_bucket_policy — public interface
# ---------------------------------------------------------------------------


def ensure_completion_report_bucket_policy(
    s3_client,
    state_bucket: str,
    replication_config_id: str,
    replication_role_arn: str,
    account_id: str,
) -> bool:
    """Ensure this `replication_config_id`'s Completion_Report_Bucket_Policy_Statement
    is present on `state_bucket`'s bucket policy.

    Calls `GetBucketPolicy` against `state_bucket`, treating a
    `NoSuchBucketPolicy` error as `current_policy=None` (Requirement 9.6 —
    no policy exists yet). Parses the existing `Policy` string as JSON when
    the call succeeds. Delegates the diff/merge decision entirely to
    `completion_tracker.ensure_completion_report_bucket_policy_statement`
    (Requirements 9.3, 9.4, 9.5, 9.6, 11.1, 11.2) and calls `PutBucketPolicy`
    with the merged document only when that function reports
    `write_needed=True`.

    When `replication_role_arn` fails validation (not a well-formed IAM role
    ARN, or an account other than `account_id` — security-scan-remediation
    Requirement 11), the pure core reports `write_needed=False` and this
    function logs the bucket and the rejected value before returning `False`
    (Requirement 11.3), skipping the `PutBucketPolicy` call entirely rather
    than placing an unvalidated value in `Principal.AWS`.

    This function performs no other business logic of its own — the
    statement's shape, the diff against the current policy, and the merge
    are entirely the pure core's responsibility.

    Parameters
    ----------
    s3_client:
        A boto3 `s3` client scoped to the account/region that owns
        `state_bucket`.
    state_bucket:
        The State_Bucket whose bucket policy is being primed.
    replication_config_id:
        The replication rule about to submit a Batch_Replication_Job.
    replication_role_arn:
        That rule's own replication IAM role ARN — the sole `Principal`
        granted by the statement, once validated.
    account_id:
        The deployment's own AWS account ID. `replication_role_arn` must
        belong to this account (Requirement 11.2).

    On a successful write, emits an audit entry
    (`action="completion_report_bucket_policy_granted"`) recording the
    State_Bucket, the granted role ARN, and the account — the grant hands an
    external principal write access, so it belongs in the audit trail
    alongside the `iam:PassRole` record. Nothing is emitted when no write was
    needed.

    Returns
    -------
    bool
        `True` if `PutBucketPolicy` was called (a write occurred), `False`
        if the desired statement was already present verbatim, or
        `replication_role_arn` failed validation, so no write was needed.

    Raises
    ------
    Exception
        Any `ClientError` from `GetBucketPolicy` other than
        `NoSuchBucketPolicy`, and any exception from `PutBucketPolicy`, is
        propagated unchanged. This adapter never swallows an error — the
        caller (the orchestrator's pre-submission call site) is responsible
        for isolating this failure per Requirement 9.7.

    Requirements: 9.1, 9.3, 9.4, 9.5, 9.6, 11.1, 11.2, 11.3
    """
    if not completion_tracker.validate_replication_role_arn(
        replication_role_arn, account_id
    ):
        _logger.error(
            "Rejecting replication role ARN %r for bucket %r: not a "
            "well-formed IAM role ARN in account %r. Skipping this "
            "config's Completion_Report_Bucket_Policy_Statement.",
            replication_role_arn,
            replication_config_id,
            account_id,
        )
        return False

    current_policy = _get_current_policy(s3_client, state_bucket)

    merged_document, write_needed = completion_tracker.ensure_completion_report_bucket_policy_statement(
        current_policy, replication_config_id, replication_role_arn, state_bucket, account_id
    )

    if not write_needed:
        return False

    s3_client.put_bucket_policy(Bucket=state_bucket, Policy=json.dumps(merged_document))

    # Audit: this write grants an *external* principal — the customer's
    # replication role — s3:PutObject on the State_Bucket. That is a
    # privilege-relevant mutation of the same class as the iam:PassRole
    # recorded by batch_operations_adapter, so it is audited here at the point
    # of the mutation rather than at the call site, where a future caller could
    # forget it. Emitted only when a write actually occurred: `write_needed`
    # is False when the statement is already present verbatim, and a no-op
    # would otherwise log a grant that did not happen.
    observability.emit(
        observability.log_audit(
            action="completion_report_bucket_policy_granted",
            source_bucket=replication_config_id,
            details={
                "state_bucket": state_bucket,
                "replication_role_arn": replication_role_arn,
                "account_id": account_id,
            },
        )
    )
    return True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_current_policy(s3_client, state_bucket: str) -> dict | None:
    """Read and parse `state_bucket`'s current bucket policy, or `None` when
    no policy exists yet (`NoSuchBucketPolicy`).

    Any other `ClientError` propagates unchanged.
    """
    try:
        response = s3_client.get_bucket_policy(Bucket=state_bucket)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code == "NoSuchBucketPolicy":
            return None
        raise
    return json.loads(response["Policy"])
