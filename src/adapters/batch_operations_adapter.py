"""Batch_Job_Manager adapter — creates S3 Batch Operations replication jobs.

This is the thin I/O shell for the Batch_Job_Manager component.  All business
logic for determining *which* objects to replicate lives in the pure core; this
module is responsible only for the AWS API calls and translating results into the
``SubmissionResult`` the orchestrator uses to drive checkpoint advancement.

Jobs are created with ``ConfirmationRequired=False`` so they transition directly
to ``Ready`` and run immediately without a separate confirmation step.

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 8.1, 10.1, 10.2, 10.3, 12.6
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from botocore.exceptions import ClientError, ParamValidationError

from src.adapters._aws_call_helpers import call_with_timeout as _call_with_timeout
from src.adapters._aws_call_helpers import client_error_reason as _client_error_reason
from src.core import observability
from src.core.models import FailureClass, S3Location, SubmissionStatus

# ---------------------------------------------------------------------------
# Timeout budget (Requirements 10.1, 10.3)
# ---------------------------------------------------------------------------
#
# Maximum wall-clock seconds to spend waiting for a single AWS API call.
# Keeping each call within this budget ensures failure reports are emitted
# within 60 s of detecting a problem. The real bound against a TCP-level
# stall is the socket-level connect/read timeout configured on the client by
# ClientFactory (code-review-remediation spec Req 5); _TIMEOUT_SECONDS here
# is a defense-in-depth thread-pool bound re-exported from
# ``src.adapters._aws_call_helpers`` (formerly duplicated in this module).


# ---------------------------------------------------------------------------
# SubmissionResult
# ---------------------------------------------------------------------------


@dataclass
class SubmissionResult:
    """Return value of :func:`submit_batch_job`.

    Carries the outcome of the create call and all information the orchestrator
    needs to decide whether to advance the checkpoint and emit a failure report.

    Attributes
    ----------
    status:
        Terminal outcome for this submission attempt.

        * ``SKIPPED``       — manifest absent or empty; no job created (7.5).
        * ``SUBMITTED``     — job created and running immediately (7.3, 7.4).
        * ``CREATE_FAILED`` — ``create_job`` call failed; manifest retained (7.6).
    config_id:
        Replication configuration identifier for correlation in failure reports.
    object_count:
        Number of entries in the manifest; 0 for skips; used in failure
        reports (10.1, 10.3).
    job_id:
        AWS-assigned job identifier; ``None`` unless ``status == SUBMITTED``.
    error_reason:
        Human-readable rejection reason from S3 Batch Operations, or a
        description of the failure when no AWS reason was returned (10.1, 10.3).
        ``None`` for ``SUBMITTED`` and ``SKIPPED``.
    """

    status: SubmissionStatus
    config_id: str
    object_count: int
    job_id: str | None = None
    error_reason: str | None = None
    failure_class: FailureClass | None = None

    # ------------------------------------------------------------------
    # Convenience predicates
    # ------------------------------------------------------------------

    @property
    def was_submitted(self) -> bool:
        """True iff the job was successfully created and submitted."""
        return self.status is SubmissionStatus.SUBMITTED

    @property
    def failed(self) -> bool:
        """True iff creation or submission failed and the manifest was retained."""
        return self.status in (
            SubmissionStatus.CREATE_FAILED,
            SubmissionStatus.SUBMIT_FAILED,
        )


# ---------------------------------------------------------------------------
# submit_batch_job — public interface
# ---------------------------------------------------------------------------


def submit_batch_job(
    s3control_client,
    account_id: str,
    manifest_location: S3Location,
    manifest_etag: str,
    batch_operations_role_arn: str,
    config_id: str,
    object_count: int,
    source_bucket: str,
    has_version_ids: bool = False,
    manifest_format: str = "S3BatchOperations_CSV_20180820",
    completion_report_prefix: str = "completion-reports/default",
    state_bucket: str = "state-bucket",
) -> SubmissionResult:
    """Create one S3 Batch Operations replication job.

    Implements the Batch_Job_Manager submission step (Requirements 7.1–7.6,
    8.1, 10.1–10.3, 12.6).  The function is intentionally synchronous and
    returns a :class:`SubmissionResult` immediately after each call completes
    or times out, so the orchestrator always receives a result within
    ``_TIMEOUT_SECONDS`` seconds of the call returning.

    The job is configured for the ``S3ReplicateObject`` operation, which
    instructs S3 Batch Operations to replicate each listed object using the
    source bucket's **existing** Replication_Configuration.  No
    customer-supplied destination is required or accepted (7.2).

    Two distinct roles are involved. ``batch_operations_role_arn`` is the job
    role: it authorizes initiating replication and reading the manifest, and is
    created by this Solution's own stack. The bucket's replication
    configuration role, which performs the delivery, is never named here.

    The job is created with ``ConfirmationRequired=False`` so it transitions
    immediately to ``Ready`` and runs without a separate confirmation step.

    Parameters
    ----------
    s3control_client:
        A boto3 ``s3control`` client scoped to the source account and region.
    account_id:
        AWS account ID that owns the source bucket and will own the batch job.
    manifest_location:
        :class:`~src.core.models.S3Location` (bucket + key) of the manifest
        object.  For ``Inventory_Report``, this is the ``manifest.json`` key.
    manifest_etag:
        ETag of the manifest object; required by S3 Batch Operations to
        validate the manifest hasn't changed since it was written.
    batch_operations_role_arn:
        ARN of the stack-created S3 Batch Operations job role, supplied to the
        runtime as the ``BATCH_OPERATIONS_ROLE_ARN`` environment variable.
    config_id:
        Bucket replication configuration identifier.
    object_count:
        Number of objects in the manifest.  ``0`` triggers an immediate skip.
    source_bucket:
        Source bucket name.
    has_version_ids:
        When ``True`` and format is ``CSV_Manifest``, the manifest fields are
        set to ``["Bucket", "Key", "VersionId"]``; otherwise ``["Bucket", "Key"]``.
        Ignored for ``Inventory_Report`` format (field schema is in the envelope).
    manifest_format:
        S3 Batch Operations manifest format string.  Defaults to
        ``"S3BatchOperations_CSV_20180820"`` (CSV_Manifest).  Pass
        ``"S3InventoryReport_CSV_20161130"`` for an Inventory_Report_Manifest
        (Requirements 7.4, 8.1, 8.4).
    completion_report_prefix:
        The BOPS_Completion_Report prefix. Every job is created with an
        enabled report (``Report.Enabled=True``, ``Report.ReportScope=
        "AllTasks"``, ``Report.Format="Report_CSV_20180820"``) written to
        this prefix in ``state_bucket`` (Requirement 1.1, 1.2).
    state_bucket:
        The State_Bucket name the BOPS_Completion_Report is written to.
        Used to build the ``Report.Bucket`` ARN.

    Returns
    -------
    SubmissionResult
        One of three outcomes: ``SKIPPED``, ``SUBMITTED``, or ``CREATE_FAILED``.

    Requirements: 1.1, 1.2, 1.3, 1.4, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 8.1, 8.2,
                  8.3, 8.4, 8.5, 10.1, 10.2, 10.3, 12.6
    """
    # ------------------------------------------------------------------
    # 7.5 — skip creation for an absent or empty manifest
    # ------------------------------------------------------------------
    if object_count == 0:
        return SubmissionResult(
            status=SubmissionStatus.SKIPPED,
            config_id=config_id,
            object_count=0,
        )

    # Construct the manifest object ARN from its S3 location.
    # S3 Batch Operations requires the ARN form rather than a plain URI.
    manifest_object_arn = (
        f"arn:aws:s3:::{manifest_location.bucket}/{manifest_location.key}"
    )

    # Unique idempotency token — prevents duplicate jobs if the caller
    # retries due to a transient network error before receiving the response.
    client_token = str(uuid.uuid4())

    # Select manifest fields based on whether version IDs are present.
    manifest_fields = ["Bucket", "Key", "VersionId"] if has_version_ids else ["Bucket", "Key"]

    # ------------------------------------------------------------------
    # Build the manifest spec for the chosen format (Req 8.1, 8.4, 8.5)
    # ------------------------------------------------------------------
    is_inventory = manifest_format == "S3InventoryReport_CSV_20161130"

    if is_inventory:
        # Inventory_Report: no Fields (schema is in the manifest.json envelope),
        # reference the manifest.json key (Req 8.1, 8.4)
        manifest_spec: dict = {
            "Spec": {
                "Format": manifest_format,
                # No "Fields" for inventory format (Req 8.4)
            },
            "Location": {
                "ObjectArn": manifest_object_arn,
                "ETag": manifest_etag,
            },
        }
        # No manifest-encryption declaration. S3 Control's JobManifestLocation
        # accepts only ObjectArn, ObjectVersionId, and ETag — there is no
        # ManifestEncryption member, and passing one fails botocore's own
        # parameter validation before the request is signed, so on a
        # KmsKeyArn deployment every submission returned CREATE_FAILED, on
        # every interval, with no metric and no circuit-breaker reaction to it.
        # An SSE-KMS manifest
        # needs nothing declared here: S3 Batch Operations decrypts it with
        # the job's RoleArn, and the stack grants that role kms:Decrypt on
        # KmsKeyArn via BatchOperationsRoleKmsPolicy.
    else:
        # CSV_Manifest: standard manifest location and schema (Req 8.5)
        manifest_spec = {
            "Spec": {
                "Format": manifest_format,
                "Fields": manifest_fields,
            },
            "Location": {
                "ObjectArn": manifest_object_arn,
                "ETag": manifest_etag,
            },
        }

    # ------------------------------------------------------------------
    # create_job: Requirements 7.1 and 7.2
    # ------------------------------------------------------------------
    # ConfirmationRequired=False means the job transitions directly to Ready
    # and starts running immediately after creation.
    try:
        description = f"s3rot: {source_bucket} ({object_count} objects)"
        # S3 Batch Operations limits Description to 256 characters.
        if len(description) > 256:
            description = description[:253] + "..."

        create_response = _call_with_timeout(
            lambda: s3control_client.create_job(
                AccountId=account_id,
                ConfirmationRequired=False,
                Description=description,
                # 7.2 — S3ReplicateObject with no destination
                Operation={"S3ReplicateObject": {}},
                Manifest=manifest_spec,
                Report=_build_report_spec(completion_report_prefix, state_bucket),
                Priority=10,
                # 7.2 — pass the stack-created job role (iam:PassRole)
                RoleArn=batch_operations_role_arn,
                ClientRequestToken=client_token,
                Tags=[
                    {"Key": "replication_config_id", "Value": config_id},
                ],
            ),
        )
    except TimeoutError as exc:
        # API call hung beyond _TIMEOUT_SECONDS; report immediately (10.1, 10.3)
        return SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id=config_id,
            object_count=object_count,
            error_reason=str(exc),
            failure_class=FailureClass.TIMEOUT,
        )
    except ParamValidationError as exc:
        # botocore rejected the request before signing — a code defect in the
        # Solution, not a condition in the customer's account. Classified as
        # PERMANENT_CLIENT so the orchestrator can escalate immediately and
        # count toward the disable threshold.
        #
        # ParamValidationError is NOT a subclass of ClientError, so the
        # ordering of these except clauses is a readability choice, not a
        # correctness one — but it is stated explicitly because reordering
        # them later would silently reclassify every validation error as
        # SERVICE, which changes escalation and disable behaviour.
        return SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id=config_id,
            object_count=object_count,
            error_reason=str(exc),
            failure_class=FailureClass.PERMANENT_CLIENT,
        )
    except ClientError as exc:
        # AWS returned an explicit error response (permission denied, etc.)
        return SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id=config_id,
            object_count=object_count,
            error_reason=_client_error_reason(exc),
            failure_class=FailureClass.SERVICE,
        )
    except Exception as exc:  # noqa: BLE001
        # Network error, unexpected SDK error, etc.
        return SubmissionResult(
            status=SubmissionStatus.CREATE_FAILED,
            config_id=config_id,
            object_count=object_count,
            error_reason=str(exc) or "unknown error during create_job",
            failure_class=FailureClass.UNKNOWN,
        )

    job_id: str = create_response["JobId"]

    # Audit: record the iam:PassRole action — which job role was passed to
    # which job for which config/bucket. This is the privilege-relevant fact a
    # security investigation needs to detect anomalous role usage; create_job
    # alone (in CloudTrail) does not capture the application-level correlation.
    observability.emit(
        observability.log_audit(
            action="batch_job_created",
            source_bucket=source_bucket,
            details={
                "job_id": job_id,
                "config_id": config_id,
                "batch_operations_role_arn": batch_operations_role_arn,
                "object_count": object_count,
            },
        )
    )

    # 7.4 — job submitted; return job id for the orchestrator to record
    return SubmissionResult(
        status=SubmissionStatus.SUBMITTED,
        config_id=config_id,
        object_count=object_count,
        job_id=job_id,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_report_spec(
    completion_report_prefix: str, state_bucket: str
) -> dict:
    """Build the CreateJob ``Report`` field.

    Every job now requests an enabled completion report (Requirement 1.1).
    Returns the spec with ``ReportScope="AllTasks"`` so every processed object
    version is listed regardless of per-task status (Requirement 1.2), and
    ``Format="Report_CSV_20180820"``. ``Report.Bucket`` is an ARN
    (``arn:aws:s3:::<state_bucket>``), per S3 Batch Operations' CreateJob
    contract.

    ``Format`` selects the report schema version, and the report's column
    layout is a property of that version: under ``Report_CSV_20180820`` the
    ``ErrorCode`` and ``HTTPStatusCode`` columns are emitted in the reverse of
    the documented order. A version this Solution has not seen could order them
    either way. ``bops_report_reader`` resolves those two columns by content
    rather than position and so handles either layout, but re-read
    ``_REPORT_COLUMN_ORDER_NOTE`` there before changing this value, since the
    rest of the report contract may move with it.

    Requirements: 1.1, 1.2, 1.3
    """
    return {
        "Bucket": f"arn:aws:s3:::{state_bucket}",
        "Prefix": completion_report_prefix,
        "Format": "Report_CSV_20180820",
        "Enabled": True,
        "ReportScope": "AllTasks",
    }

