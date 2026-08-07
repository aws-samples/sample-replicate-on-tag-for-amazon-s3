"""Tests for src/adapters/source_status_adapter.py — task 15.2.

Covers:
  - PENDING / COMPLETED / FAILED ReplicationStatus header values.
  - Header-absent (call succeeds, no ReplicationStatus field) (Req 3.4).
  - A transient AWS error outcome (Req 3.6).
  - The function signature accepts no destination-region or
    destination-account parameter (architectural note in requirements.md).

Requirements: 3.1, 3.4, 3.6
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.adapters.source_status_adapter import (
    SourceStatusCheckKind,
    SourceStatusResult,
    check_source_replication_status,
)

_SRC_BUCKET = "my-source-bucket"
_KEY = "path/to/object.txt"
_VERSION_ID = "v1.abc123"


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "HeadObject")


class TestHeaderValuePresent:
    @pytest.mark.parametrize("status", ["PENDING", "COMPLETED", "FAILED"])
    def test_header_value_returned_verbatim(self, status: str):
        """A present ReplicationStatus header is returned verbatim (Req 3.1)."""
        client = MagicMock()
        client.head_object.return_value = {"ReplicationStatus": status}

        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )

        assert result.kind is SourceStatusCheckKind.HEADER_VALUE
        assert result.value == status
        assert result.error_reason is None

    def test_head_object_called_with_version_id(self):
        """VersionId is passed through when not None (Req 3.1)."""
        client = MagicMock()
        client.head_object.return_value = {"ReplicationStatus": "COMPLETED"}

        check_source_replication_status(client, _SRC_BUCKET, _KEY, _VERSION_ID)

        client.head_object.assert_called_once_with(
            Bucket=_SRC_BUCKET, Key=_KEY, VersionId=_VERSION_ID
        )

    def test_head_object_omits_version_id_when_none(self):
        """VersionId is omitted entirely for the null-version marker (Req 1.3 interplay)."""
        client = MagicMock()
        client.head_object.return_value = {"ReplicationStatus": "COMPLETED"}

        check_source_replication_status(client, _SRC_BUCKET, _KEY, None)

        client.head_object.assert_called_once_with(Bucket=_SRC_BUCKET, Key=_KEY)
        kwargs = client.head_object.call_args[1]
        assert "VersionId" not in kwargs

    def test_head_object_called_against_source_bucket(self):
        """The call targets the source bucket, not any destination bucket (Req 3.1)."""
        client = MagicMock()
        client.head_object.return_value = {"ReplicationStatus": "PENDING"}

        check_source_replication_status(client, _SRC_BUCKET, _KEY, _VERSION_ID)

        kwargs = client.head_object.call_args[1]
        assert kwargs["Bucket"] == _SRC_BUCKET


class TestHeaderAbsent:
    def test_header_absent_when_field_missing(self):
        """A successful call with no ReplicationStatus field resolves HEADER_ABSENT (Req 3.4)."""
        client = MagicMock()
        client.head_object.return_value = {"ContentLength": 123}

        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )

        assert result.kind is SourceStatusCheckKind.HEADER_ABSENT
        assert result.value is None
        assert result.error_reason is None

    def test_header_absent_when_field_empty_string(self):
        """An empty-string ReplicationStatus is treated as absent, not a literal value."""
        client = MagicMock()
        client.head_object.return_value = {"ReplicationStatus": ""}

        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )

        assert result.kind is SourceStatusCheckKind.HEADER_ABSENT


class TestCheckFailed:
    def test_client_error_returns_check_failed(self):
        """A ClientError (e.g. throttling) resolves CHECK_FAILED, not raised (Req 3.6)."""
        client = MagicMock()
        client.head_object.side_effect = _client_error("SlowDown", "Please reduce your request rate")

        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )

        assert result.kind is SourceStatusCheckKind.CHECK_FAILED
        assert result.value is None
        assert result.error_reason is not None
        assert "SlowDown" in result.error_reason

    def test_generic_exception_returns_check_failed(self):
        """A non-ClientError exception (e.g. network stall) also resolves CHECK_FAILED."""
        client = MagicMock()
        client.head_object.side_effect = ConnectionError("network stall")

        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )

        assert result.kind is SourceStatusCheckKind.CHECK_FAILED
        assert result.error_reason is not None
        assert len(result.error_reason) > 0

    def test_check_failed_does_not_raise(self):
        """The adapter never lets an exception escape — always returns a result (Req 3.6)."""
        client = MagicMock()
        client.head_object.side_effect = RuntimeError("boom")

        # Should not raise.
        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )
        assert isinstance(result, SourceStatusResult)


class TestNoDestinationParameter:
    def test_signature_has_no_destination_region_parameter(self):
        """The function never accepts a destination-region parameter.

        Confirms the architectural note in requirements.md: Requirement 3's
        Source_Status_Check never touches the destination account/region, so
        ClientFactory.check_no_destination_client() requires no exception for
        this code path.
        """
        sig = inspect.signature(check_source_replication_status)
        assert "destination_region" not in sig.parameters
        assert "destination_account_id" not in sig.parameters

    def test_signature_has_no_destination_account_parameter(self):
        sig = inspect.signature(check_source_replication_status)
        param_names = set(sig.parameters)
        assert not any("destination" in name for name in param_names)

    def test_signature_only_takes_source_side_arguments(self):
        """Only s3_client, source_bucket, object_key, version_id are accepted."""
        sig = inspect.signature(check_source_replication_status)
        assert set(sig.parameters) == {
            "s3_client",
            "source_bucket",
            "object_key",
            "version_id",
        }


# ---------------------------------------------------------------------------
# OBJECT_GONE — a deleted object version is terminal, not transient (Req 3.7)
# ---------------------------------------------------------------------------


def _client_error_with_status(code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": "error"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "HeadObject",
    )


class TestObjectGone:
    def test_http_404_with_empty_code_is_object_gone(self):
        """HeadObject carries no body, so botocore often surfaces a bare 404.

        Matching on the HTTP status is what makes this case detectable at all
        (Req 3.7).
        """
        client = MagicMock()
        client.head_object.side_effect = _client_error_with_status("404", 404)
        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )
        assert result.kind is SourceStatusCheckKind.OBJECT_GONE
        assert result.kind is not SourceStatusCheckKind.CHECK_FAILED

    @pytest.mark.parametrize("code", ["NoSuchKey", "NoSuchVersion"])
    def test_no_such_key_codes_are_object_gone(self, code: str):
        """Named not-found error codes are terminal too (Req 3.7)."""
        client = MagicMock()
        client.head_object.side_effect = _client_error(code)
        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )
        assert result.kind is SourceStatusCheckKind.OBJECT_GONE

    def test_403_remains_a_transient_check_failure(self):
        """403 must NOT be treated as gone.

        S3 returns 403 both for a genuine permission problem and for a missing
        object when the caller lacks s3:ListBucket. Treating it as terminal
        would silently abandon objects during an IAM misconfiguration, so it
        stays CHECK_FAILED and the IAM grant is what converts the
        missing-object case into a 404 (Req 3.6, 3.7).
        """
        client = MagicMock()
        client.head_object.side_effect = _client_error_with_status(
            "AccessDenied", 403
        )
        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )
        assert result.kind is SourceStatusCheckKind.CHECK_FAILED
        assert result.kind is not SourceStatusCheckKind.OBJECT_GONE

    @pytest.mark.parametrize(
        "code,status",
        [("SlowDown", 503), ("InternalError", 500), ("RequestTimeout", 400)],
    )
    def test_transient_errors_remain_check_failed(self, code: str, status: int):
        """Throttling and service errors stay retryable (Req 3.6)."""
        client = MagicMock()
        client.head_object.side_effect = _client_error_with_status(code, status)
        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )
        assert result.kind is SourceStatusCheckKind.CHECK_FAILED

    def test_object_gone_carries_a_reason(self):
        """The reason is preserved for the audit trail (Req 3.7)."""
        client = MagicMock()
        client.head_object.side_effect = _client_error_with_status("404", 404)
        result = check_source_replication_status(
            client, _SRC_BUCKET, _KEY, _VERSION_ID
        )
        assert result.error_reason
