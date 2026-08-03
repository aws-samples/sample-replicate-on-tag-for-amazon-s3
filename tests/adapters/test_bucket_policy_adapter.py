"""Unit tests for src/adapters/bucket_policy_adapter.py — task 26.4.

Covers Requirements 9.1, 9.3, 9.4, 9.5, 9.6:
- NoSuchBucketPolicy on get_bucket_policy results in a put_bucket_policy
  call with a fresh single-statement document.
- A get_bucket_policy response already containing the exact desired
  statement results in no put_bucket_policy call.
- A get_bucket_policy response containing a different statement under the
  same Sid results in a put_bucket_policy call whose document replaces only
  that Sid and preserves every other statement.
- A non-NoSuchBucketPolicy ClientError on get_bucket_policy propagates
  rather than being swallowed.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from src.adapters.bucket_policy_adapter import ensure_completion_report_bucket_policy

_STATE_BUCKET = "state-bucket"
_CONFIG_ID = "cfg-1"
_ACCOUNT_ID = "111122223333"
_ROLE_ARN = f"arn:aws:iam::{_ACCOUNT_ID}:role/replication-role"

_DESIRED_STATEMENT = {
    "Sid": f"AllowCompletionReportWrite-{_CONFIG_ID}",
    "Effect": "Allow",
    "Principal": {"AWS": _ROLE_ARN},
    "Action": "s3:PutObject",
    "Resource": f"arn:aws:s3:::{_STATE_BUCKET}/completion-reports/{_CONFIG_ID}/*",
}


def _no_such_bucket_policy_error() -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": "NoSuchBucketPolicy", "Message": "no policy"}},
        operation_name="GetBucketPolicy",
    )


def _other_client_error() -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": "AccessDenied", "Message": "nope"}},
        operation_name="GetBucketPolicy",
    )


class TestNoBucketPolicyYet:
    def test_no_such_bucket_policy_results_in_put_with_fresh_document(self):
        client = MagicMock()
        client.get_bucket_policy.side_effect = _no_such_bucket_policy_error()

        result = ensure_completion_report_bucket_policy(
            client, _STATE_BUCKET, _CONFIG_ID, _ROLE_ARN, _ACCOUNT_ID
        )

        assert result is True
        client.put_bucket_policy.assert_called_once()
        kwargs = client.put_bucket_policy.call_args[1]
        assert kwargs["Bucket"] == _STATE_BUCKET
        document = json.loads(kwargs["Policy"])
        assert document == {"Version": "2012-10-17", "Statement": [_DESIRED_STATEMENT]}


class TestStatementAlreadyPresentVerbatim:
    def test_no_write_when_exact_statement_already_present(self):
        client = MagicMock()
        current_document = {"Version": "2012-10-17", "Statement": [_DESIRED_STATEMENT]}
        client.get_bucket_policy.return_value = {"Policy": json.dumps(current_document)}

        result = ensure_completion_report_bucket_policy(
            client, _STATE_BUCKET, _CONFIG_ID, _ROLE_ARN, _ACCOUNT_ID
        )

        assert result is False
        client.put_bucket_policy.assert_not_called()


class TestStatementDifferentUnderSameSid:
    def test_put_replaces_only_matching_sid_and_preserves_others(self):
        client = MagicMock()
        stale_statement = {**_DESIRED_STATEMENT, "Action": "s3:GetObject"}
        other_statement = {
            "Sid": "AllowCompletionReportWrite-cfg-other",
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::111122223333:role/other-role"},
            "Action": "s3:PutObject",
            "Resource": f"arn:aws:s3:::{_STATE_BUCKET}/completion-reports/cfg-other/*",
        }
        unrelated_statement = {
            "Sid": "SomeUnrelatedStatement",
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{_STATE_BUCKET}/public/*",
        }
        current_document = {
            "Version": "2012-10-17",
            "Statement": [stale_statement, other_statement, unrelated_statement],
        }
        client.get_bucket_policy.return_value = {"Policy": json.dumps(current_document)}

        result = ensure_completion_report_bucket_policy(
            client, _STATE_BUCKET, _CONFIG_ID, _ROLE_ARN, _ACCOUNT_ID
        )

        assert result is True
        client.put_bucket_policy.assert_called_once()
        kwargs = client.put_bucket_policy.call_args[1]
        document = json.loads(kwargs["Policy"])
        statements = document["Statement"]
        assert stale_statement not in statements
        assert _DESIRED_STATEMENT in statements
        assert other_statement in statements
        assert unrelated_statement in statements
        assert len(statements) == 3


class TestOtherClientErrorPropagates:
    def test_non_no_such_bucket_policy_error_raises(self):
        client = MagicMock()
        client.get_bucket_policy.side_effect = _other_client_error()

        with pytest.raises(ClientError):
            ensure_completion_report_bucket_policy(
                client, _STATE_BUCKET, _CONFIG_ID, _ROLE_ARN, _ACCOUNT_ID
            )

        client.put_bucket_policy.assert_not_called()

    def test_put_bucket_policy_error_propagates(self):
        client = MagicMock()
        client.get_bucket_policy.side_effect = _no_such_bucket_policy_error()
        client.put_bucket_policy.side_effect = _other_client_error()

        with pytest.raises(ClientError):
            ensure_completion_report_bucket_policy(
                client, _STATE_BUCKET, _CONFIG_ID, _ROLE_ARN, _ACCOUNT_ID
            )


class TestReplicationRoleArnValidation:
    """security-scan-remediation Requirement 11 (Decision 8): a replication
    role ARN that fails validation must skip the bucket policy statement
    entirely, without ever calling GetBucketPolicy/PutBucketPolicy."""

    def test_wrong_account_arn_skips_write_without_any_s3_call(self):
        client = MagicMock()
        wrong_account_arn = "arn:aws:iam::999999999999:role/replication-role"

        result = ensure_completion_report_bucket_policy(
            client, _STATE_BUCKET, _CONFIG_ID, wrong_account_arn, _ACCOUNT_ID
        )

        assert result is False
        client.get_bucket_policy.assert_not_called()
        client.put_bucket_policy.assert_not_called()

    def test_malformed_arn_skips_write_without_any_s3_call(self):
        client = MagicMock()

        result = ensure_completion_report_bucket_policy(
            client, _STATE_BUCKET, _CONFIG_ID, "not-an-arn", _ACCOUNT_ID
        )

        assert result is False
        client.get_bucket_policy.assert_not_called()
        client.put_bucket_policy.assert_not_called()

    def test_rejection_is_logged_with_bucket_and_rejected_value(self, caplog):
        client = MagicMock()
        bad_arn = "arn:aws:iam::999999999999:role/replication-role"

        with caplog.at_level("ERROR"):
            ensure_completion_report_bucket_policy(
                client, _STATE_BUCKET, _CONFIG_ID, bad_arn, _ACCOUNT_ID
            )

        messages = " ".join(r.getMessage() for r in caplog.records)
        assert _CONFIG_ID in messages
        assert bad_arn in messages


# ---------------------------------------------------------------------------
# Audit logging for the grant
#
# AWS Security Agent finding f-fb3a740a-5e8f-4330-b983-b25140d25ce4: the write
# grants an external replication role s3:PutObject on the State_Bucket — a
# privilege-relevant mutation — and emitted no audit entry, unlike the
# comparable iam:PassRole record in batch_operations_adapter.
# ---------------------------------------------------------------------------


class TestGrantIsAudited:
    @staticmethod
    def _audits(emitted: list[dict]) -> list[dict]:
        return [
            e for e in emitted
            if e.get("event") == "audit"
            and e.get("action") == "completion_report_bucket_policy_granted"
        ]

    def _run(self, s3_client, monkeypatch) -> list[dict]:
        emitted: list[dict] = []
        monkeypatch.setattr(
            "src.core.observability.emit", lambda entry: emitted.append(entry)
        )
        ensure_completion_report_bucket_policy(
            s3_client, _STATE_BUCKET, _CONFIG_ID, _ROLE_ARN, _ACCOUNT_ID
        )
        return emitted

    def test_audit_emitted_on_write(self, monkeypatch):
        s3 = MagicMock()
        s3.get_bucket_policy.side_effect = _no_such_bucket_policy_error()

        audits = self._audits(self._run(s3, monkeypatch))

        assert len(audits) == 1
        entry = audits[0]
        assert entry["source_bucket"] == _CONFIG_ID
        assert entry["state_bucket"] == _STATE_BUCKET
        assert entry["replication_role_arn"] == _ROLE_ARN
        assert entry["account_id"] == _ACCOUNT_ID
        assert "timestamp" in entry

    def test_no_audit_when_statement_already_present(self, monkeypatch):
        """A no-op must not log a grant that did not happen."""
        s3 = MagicMock()
        s3.get_bucket_policy.return_value = {
            "Policy": json.dumps(
                {"Version": "2012-10-17", "Statement": [_DESIRED_STATEMENT]}
            )
        }

        emitted = self._run(s3, monkeypatch)

        s3.put_bucket_policy.assert_not_called()
        assert self._audits(emitted) == []

    def test_no_audit_when_role_arn_rejected(self, monkeypatch):
        """A validation failure writes nothing, so it grants nothing."""
        s3 = MagicMock()
        s3.get_bucket_policy.side_effect = _no_such_bucket_policy_error()

        emitted: list[dict] = []
        monkeypatch.setattr(
            "src.core.observability.emit", lambda entry: emitted.append(entry)
        )
        result = ensure_completion_report_bucket_policy(
            s3,
            _STATE_BUCKET,
            _CONFIG_ID,
            f"arn:aws:iam::999999999999:role/foreign-role",
            _ACCOUNT_ID,
        )

        assert result is False
        s3.put_bucket_policy.assert_not_called()
        assert self._audits(emitted) == []

    def test_audit_carries_no_object_key(self, monkeypatch):
        """log_audit's contract forbids object keys in details."""
        s3 = MagicMock()
        s3.get_bucket_policy.side_effect = _no_such_bucket_policy_error()

        entry = self._audits(self._run(s3, monkeypatch))[0]

        assert "object_key" not in entry
