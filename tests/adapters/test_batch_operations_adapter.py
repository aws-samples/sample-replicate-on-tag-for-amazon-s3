"""Tests for src/adapters/batch_operations_adapter.py — tasks 15.2 and 15.3.

Task 15.2 — mocked integration tests for create/submit:
  - CreateJob called with manifest and replication role, no destination parameter.
  - Job id captured on success (SUBMITTED).

Task 15.3 — unit tests for job failure reporting and skips:
  - Empty/absent manifest (object_count=0) → SKIPPED indication (7.5).
  - Creation failure → CREATE_FAILED, manifest retained (7.6).
  - Submission rejection emits report within 60 s with config/reason/count (10.1).
  - Failure without rejection reason reports accordingly (10.3).
  - Missing permission → CREATE_FAILED, manifest retained for retry (12.6).

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 10.1, 10.3, 12.6
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.adapters.batch_operations_adapter import SubmissionResult, submit_batch_job
from src.core.models import S3Location, SubmissionStatus

_ACCOUNT_ID = "123456789012"
_ROLE_ARN = "arn:aws:iam::123456789012:role/replication-role"
_CONFIG_ID = "cfg-replication-1"
_SRC_BUCKET = "my-source-bucket"
_MANIFEST_LOC = S3Location(bucket="scratch-bucket", key="manifests/cfg-1/ts.csv")
_MANIFEST_ETAG = "d41d8cd98f00b204e9800998ecf8427e"
_JOB_ID = "job-00112233-4455-6677-8899-aabbccddeeff"


def _mock_s3control(job_id: str = _JOB_ID) -> MagicMock:
    client = MagicMock()
    client.create_job.return_value = {"JobId": job_id}
    return client


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}}, "CreateJob"
    )


def _call_submit(
    client=None,
    account_id: str = _ACCOUNT_ID,
    manifest_location: S3Location = _MANIFEST_LOC,
    manifest_etag: str = _MANIFEST_ETAG,
    role_arn: str = _ROLE_ARN,
    config_id: str = _CONFIG_ID,
    object_count: int = 5,
    source_bucket: str = _SRC_BUCKET,
    has_version_ids: bool = False,
) -> SubmissionResult:
    if client is None:
        client = _mock_s3control()
    return submit_batch_job(
        s3control_client=client,
        account_id=account_id,
        manifest_location=manifest_location,
        manifest_etag=manifest_etag,
        replication_role_arn=role_arn,
        config_id=config_id,
        object_count=object_count,
        source_bucket=source_bucket,
        has_version_ids=has_version_ids,
    )


# ---------------------------------------------------------------------------
# Task 15.2: create/submit integration tests
# ---------------------------------------------------------------------------


class TestCreateJobIntegration:
    def test_create_job_called_once_on_success(self):
        """CreateJob is invoked exactly once (Req 7.1)."""
        client = _mock_s3control()
        _call_submit(client=client)
        client.create_job.assert_called_once()

    def test_create_job_uses_replication_role_arn(self):
        """RoleArn in CreateJob is the existing replication role (Req 7.2)."""
        client = _mock_s3control()
        _call_submit(client=client, role_arn=_ROLE_ARN)
        kwargs = client.create_job.call_args[1]
        assert kwargs["RoleArn"] == _ROLE_ARN

    def test_create_job_has_no_destination_parameter(self):
        """S3ReplicateObject op dict must be empty (no destination) (Req 7.2)."""
        client = _mock_s3control()
        _call_submit(client=client)
        kwargs = client.create_job.call_args[1]
        operation = kwargs["Operation"]
        assert "S3ReplicateObject" in operation
        assert operation["S3ReplicateObject"] == {}

    def test_create_job_uses_manifest_location(self):
        """Manifest ARN includes bucket and key from S3Location (Req 7.1)."""
        client = _mock_s3control()
        loc = S3Location(bucket="my-scratch", key="manifests/cfg/ts.csv")
        _call_submit(client=client, manifest_location=loc)
        kwargs = client.create_job.call_args[1]
        manifest = kwargs["Manifest"]
        assert "my-scratch" in manifest["Location"]["ObjectArn"]
        assert "manifests/cfg/ts.csv" in manifest["Location"]["ObjectArn"]

    def test_create_job_uses_manifest_etag(self):
        """Manifest ETag passed in Location (Req 7.1)."""
        client = _mock_s3control()
        _call_submit(client=client, manifest_etag="abc123")
        kwargs = client.create_job.call_args[1]
        assert kwargs["Manifest"]["Location"]["ETag"] == "abc123"

    def test_job_id_captured_on_success(self):
        """Job id from CreateJob response is captured (Req 7.4)."""
        client = _mock_s3control(job_id=_JOB_ID)
        result = _call_submit(client=client)
        assert result.status == SubmissionStatus.SUBMITTED
        assert result.job_id == _JOB_ID

    def test_confirmation_not_required(self):
        """ConfirmationRequired=False so job runs immediately (Req 7.3)."""
        client = _mock_s3control()
        _call_submit(client=client)
        kwargs = client.create_job.call_args[1]
        assert kwargs.get("ConfirmationRequired") is False

    def test_account_id_passed_to_create_job(self):
        client = _mock_s3control()
        _call_submit(client=client, account_id="999888777666")
        kwargs = client.create_job.call_args[1]
        assert kwargs["AccountId"] == "999888777666"

    def test_unversioned_manifest_uses_bucket_key_fields(self):
        """has_version_ids=False → Fields=[Bucket, Key] (Req 7.1)."""
        client = _mock_s3control()
        _call_submit(client=client, has_version_ids=False)
        kwargs = client.create_job.call_args[1]
        fields = kwargs["Manifest"]["Spec"]["Fields"]
        assert "VersionId" not in fields
        assert "Bucket" in fields
        assert "Key" in fields

    def test_versioned_manifest_uses_bucket_key_versionid_fields(self):
        """has_version_ids=True → Fields=[Bucket, Key, VersionId]."""
        client = _mock_s3control()
        _call_submit(client=client, has_version_ids=True)
        kwargs = client.create_job.call_args[1]
        fields = kwargs["Manifest"]["Spec"]["Fields"]
        assert "VersionId" in fields


# ---------------------------------------------------------------------------
# Task 15.3: failure reporting and skips
# ---------------------------------------------------------------------------


class TestSkipAndFailureReporting:
    def test_zero_object_count_returns_skipped(self):
        """object_count=0 → SKIPPED, no API calls made (Req 7.5)."""
        client = _mock_s3control()
        result = _call_submit(client=client, object_count=0)
        assert result.status is SubmissionStatus.SKIPPED
        client.create_job.assert_not_called()

    def test_skipped_result_has_no_job_id(self):
        result = _call_submit(object_count=0)
        assert result.job_id is None

    def test_skipped_result_carries_config_id(self):
        result = _call_submit(config_id="my-cfg", object_count=0)
        assert result.config_id == "my-cfg"

    def test_client_error_returns_create_failed(self):
        """ClientError from create_job → CREATE_FAILED (Req 7.6)."""
        client = MagicMock()
        client.create_job.side_effect = _client_error("InternalError", "internal")
        result = _call_submit(client=client)
        assert result.status == SubmissionStatus.CREATE_FAILED
        assert result.failed

    def test_create_failed_has_no_job_id(self):
        """No job id on failure (Req 7.6)."""
        client = MagicMock()
        client.create_job.side_effect = _client_error("InternalError")
        result = _call_submit(client=client)
        assert result.job_id is None

    def test_access_denied_returns_create_failed_with_reason(self):
        """AccessDenied → CREATE_FAILED with permission reason (Req 12.6)."""
        client = MagicMock()
        client.create_job.side_effect = _client_error("AccessDenied", "Not authorized")
        result = _call_submit(client=client)
        assert result.status == SubmissionStatus.CREATE_FAILED
        assert result.error_reason is not None
        assert "AccessDenied" in result.error_reason

    def test_create_failed_carries_object_count(self):
        """Failure report includes object count for operator (Req 10.1)."""
        client = MagicMock()
        client.create_job.side_effect = _client_error("AccessDenied")
        result = _call_submit(client=client, object_count=42)
        assert result.object_count == 42

    def test_create_failed_carries_config_id(self):
        """Failure report includes config_id for correlation (Req 10.1)."""
        client = MagicMock()
        client.create_job.side_effect = _client_error("AccessDenied")
        result = _call_submit(client=client, config_id="cfg-special")
        assert result.config_id == "cfg-special"

    def test_generic_exception_returns_create_failed_with_reason(self):
        """Non-ClientError exception → CREATE_FAILED with reason string (Req 10.3)."""
        client = MagicMock()
        client.create_job.side_effect = ConnectionError("network stall")
        result = _call_submit(client=client)
        assert result.status == SubmissionStatus.CREATE_FAILED
        assert result.error_reason is not None
        assert len(result.error_reason) > 0

    def test_submitted_result_has_job_id(self):
        """Successful submission carries the job id (Req 7.4)."""
        result = _call_submit()
        assert result.was_submitted
        assert result.job_id == _JOB_ID

    def test_submitted_result_has_no_error_reason(self):
        result = _call_submit()
        assert result.error_reason is None


# ---------------------------------------------------------------------------
# Task 14.2: conditionally-enabled BOPS_Completion_Report (design.md Decision 4)
# ---------------------------------------------------------------------------

_STATE_BUCKET = "example-state-bucket"
_REPORT_PREFIX = "completion-reports/cfg-replication-1/token-abc/"


class TestConditionallyEnabledCompletionReport:
    def test_feature_on_report_enabled_with_all_tasks_scope(self):
        """completion_report_prefix set -> Report.Enabled=True, ReportScope=AllTasks."""
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=_MANIFEST_LOC,
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            completion_report_prefix=_REPORT_PREFIX,
            state_bucket=_STATE_BUCKET,
        )
        kwargs = client.create_job.call_args[1]
        report = kwargs["Report"]
        assert report["Enabled"] is True
        assert report["ReportScope"] == "AllTasks"

    def test_feature_on_report_format_and_arn_bucket(self):
        """Report.Format is Report_CSV_20180820 and Report.Bucket is an ARN."""
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=_MANIFEST_LOC,
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            completion_report_prefix=_REPORT_PREFIX,
            state_bucket=_STATE_BUCKET,
        )
        kwargs = client.create_job.call_args[1]
        report = kwargs["Report"]
        assert report["Format"] == "Report_CSV_20180820"
        assert report["Bucket"] == f"arn:aws:s3:::{_STATE_BUCKET}"

    def test_feature_on_report_prefix_matches_derived_prefix(self):
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=_MANIFEST_LOC,
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            completion_report_prefix=_REPORT_PREFIX,
            state_bucket=_STATE_BUCKET,
        )
        kwargs = client.create_job.call_args[1]
        assert kwargs["Report"]["Prefix"] == _REPORT_PREFIX

    def test_feature_off_report_disabled_unchanged(self):
        """completion_report_prefix=None (default) -> Report == {"Enabled": False}."""
        client = _mock_s3control()
        _call_submit(client=client)
        kwargs = client.create_job.call_args[1]
        assert kwargs["Report"] == {"Enabled": False}

    def test_feature_off_explicit_none_report_disabled(self):
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=_MANIFEST_LOC,
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            completion_report_prefix=None,
            state_bucket=_STATE_BUCKET,
        )
        kwargs = client.create_job.call_args[1]
        assert kwargs["Report"] == {"Enabled": False}


# ---------------------------------------------------------------------------
# The create_job kwargs must be valid against the real S3 Control API shape.
#
# Every other test in this module asserts against a MagicMock, which accepts
# any keyword and any nested key. That is how an invented
# Manifest.Location.ManifestEncryption member survived to a deployment: the
# S3 Control API has no such field, so on a KmsKeyArn deployment botocore's
# parameter validation rejected the call before signing and every submission
# returned CREATE_FAILED, on every interval, with nothing but an ERROR log to
# show for it. These tests validate the exact kwargs against botocore's own
# service model, so an unsupported parameter fails here instead of only in an
# account with a KMS key configured.
# ---------------------------------------------------------------------------


def _validate_against_api(client) -> None:
    """Assert every s3control call recorded on *client* is valid API input.

    Delegates to the shared helper so this module and
    ``tests/test_api_param_shapes.py`` validate by one mechanism.
    """
    from tests.api_shape import assert_calls_match_api

    assert_calls_match_api(client, "s3control", expected=1)


class TestCreateJobParamsMatchApiShape:
    def test_csv_manifest_params_are_valid(self):
        client = _mock_s3control()
        _call_submit(client=client, has_version_ids=True)
        _validate_against_api(client)

    def test_inventory_manifest_params_are_valid(self):
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=S3Location(
                bucket="scratch-bucket",
                key="manifests/src/ts/manifest.json",
            ),
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            manifest_format="S3InventoryReport_CSV_20161130",
        )
        _validate_against_api(client)

    def test_inventory_manifest_with_completion_report_params_are_valid(self):
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=S3Location(
                bucket="scratch-bucket",
                key="manifests/src/ts/manifest.json",
            ),
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            manifest_format="S3InventoryReport_CSV_20161130",
            completion_report_prefix="completion-reports/src/manifest/",
            state_bucket="scratch-bucket",
        )
        _validate_against_api(client)

    def test_manifest_location_declares_no_encryption(self):
        """A regression guard on the specific member that was invented: the
        manifest may be SSE-KMS encrypted, but that is not declared to
        CreateJob — S3 Batch Operations decrypts it using the job's RoleArn."""
        client = _mock_s3control()
        submit_batch_job(
            s3control_client=client,
            account_id=_ACCOUNT_ID,
            manifest_location=S3Location(
                bucket="scratch-bucket",
                key="manifests/src/ts/manifest.json",
            ),
            manifest_etag=_MANIFEST_ETAG,
            replication_role_arn=_ROLE_ARN,
            config_id=_CONFIG_ID,
            object_count=5,
            source_bucket=_SRC_BUCKET,
            manifest_format="S3InventoryReport_CSV_20161130",
        )
        location = client.create_job.call_args.kwargs["Manifest"]["Location"]
        assert set(location) <= {"ObjectArn", "ObjectVersionId", "ETag"}
