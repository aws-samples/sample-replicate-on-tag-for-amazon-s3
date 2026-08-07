"""Mocked integration tests for src/adapters/replication_config_adapter.py.

Covers task 11.2: GetBucketReplication invoked per bucket; derived rules wired
to the matcher; no-config / unreadable-config / missing-permission cases
produce a skip + report.

Requirements: 3.1, 3.4, 3.5, 3.6, 12.5, 13.5
"""
from __future__ import annotations

from unittest.mock import MagicMock

import botocore.exceptions
import pytest

from src.adapters.replication_config_adapter import (
    SkipReport,
    _COMPONENT,
    get_replication_rules,
)
from src.core.models import DerivedReplicationRule, DestinationRef, MonitoredBucket

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_ROLE_ARN = "arn:aws:iam::123456789012:role/replication-role"
_DEST_ARN = "arn:aws:s3:::dest-bucket"
_BUCKET_NAME = "my-source-bucket"
_REGION = "us-east-1"


def _bucket(name: str = _BUCKET_NAME, region: str = _REGION) -> MonitoredBucket:
    return MonitoredBucket(name=name, region=region)


def _s3_client_returning(response: dict) -> MagicMock:
    client = MagicMock()
    client.get_bucket_replication.return_value = response
    return client


def _client_error(code: str, message: str = "error") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": message}},
        "GetBucketReplication",
    )


def _replication_response(*rules: dict) -> dict:
    """Minimal GetBucketReplication response with the given rules."""
    return {
        "ReplicationConfiguration": {
            "Role": _ROLE_ARN,
            "Rules": list(rules),
        }
    }


def _tag_rule(rule_id: str, key: str = "env", value: str = "prod") -> dict:
    return {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {"Tag": {"Key": key, "Value": value}},
        "Destination": {"Bucket": _DEST_ARN},
    }


def _prefix_rule(rule_id: str) -> dict:
    """A replication rule with no tag filter (prefix-only)."""
    return {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {"Prefix": "data/"},
        "Destination": {"Bucket": _DEST_ARN},
    }


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------


class TestSuccess:
    def test_returns_derived_rules_and_empty_skip_list(self):
        """GetBucketReplication returns config with one tag rule → one DerivedReplicationRule."""
        client = _s3_client_returning(_replication_response(_tag_rule("rule-1")))
        rules, skips = get_replication_rules(client, _bucket())

        assert len(rules) == 1
        assert isinstance(rules[0], DerivedReplicationRule)
        assert skips == []

    def test_get_bucket_replication_called_with_correct_bucket(self):
        """The adapter calls GetBucketReplication with the monitored bucket name (Req. 3.1)."""
        client = _s3_client_returning(_replication_response(_tag_rule("rule-1")))
        get_replication_rules(client, _bucket(name="specific-bucket"))

        client.get_bucket_replication.assert_called_once_with(Bucket="specific-bucket")

    def test_derived_rule_fields_preserved(self):
        client = _s3_client_returning(_replication_response(_tag_rule("my-rule")))
        rules, _ = get_replication_rules(client, _bucket())

        rule = rules[0]
        assert rule.source_bucket == _BUCKET_NAME
        assert rule.rule_id == "my-rule"
        assert rule.tag_filter == {"env": "prod"}
        assert rule.destination == DestinationRef(bucket_arn=_DEST_ARN)

    def test_multiple_tag_rules_all_returned(self):
        client = _s3_client_returning(
            _replication_response(
                _tag_rule("rule-a", key="env", value="prod"),
                _tag_rule("rule-b", key="tier", value="hot"),
            )
        )
        rules, skips = get_replication_rules(client, _bucket())

        assert len(rules) == 2
        assert skips == []

    def test_prefix_rules_excluded_from_derived_set(self):
        """Tag-only rules are derived; prefix-only rules are excluded (Req. 3.3)."""
        client = _s3_client_returning(
            _replication_response(
                _tag_rule("rule-tag"),
                _prefix_rule("rule-prefix"),
            )
        )
        rules, skips = get_replication_rules(client, _bucket())

        assert len(rules) == 1
        assert rules[0].rule_id == "rule-tag"
        assert skips == []


# ---------------------------------------------------------------------------
# Skip: no Replication_Configuration (Req. 3.4, 13.5)
# ---------------------------------------------------------------------------


class TestNoReplicationConfiguration:
    def test_returns_empty_rules_and_one_skip_report(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error(
            "ReplicationConfigurationNotFoundError"
        )
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1

    def test_skip_report_identifies_bucket(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error(
            "ReplicationConfigurationNotFoundError"
        )
        _, skips = get_replication_rules(client, _bucket(name="bucket-x"))

        assert skips[0].source_bucket == "bucket-x"

    def test_skip_report_component_is_adapter(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error(
            "ReplicationConfigurationNotFoundError"
        )
        _, skips = get_replication_rules(client, _bucket())

        assert skips[0].component == _COMPONENT

    def test_skip_report_reason_mentions_no_config(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error(
            "ReplicationConfigurationNotFoundError"
        )
        _, skips = get_replication_rules(client, _bucket())

        assert "ReplicationConfigurationNotFoundError" in skips[0].reason


# ---------------------------------------------------------------------------
# Skip: missing permission (Req. 12.5)
# ---------------------------------------------------------------------------


class TestAccessDenied:
    def test_access_denied_returns_skip(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error("AccessDenied")
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1

    def test_skip_report_identifies_bucket(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error("AccessDenied")
        _, skips = get_replication_rules(client, _bucket(name="locked-bucket"))

        assert skips[0].source_bucket == "locked-bucket"

    def test_skip_report_reason_mentions_permission(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error("AccessDenied")
        _, skips = get_replication_rules(client, _bucket())

        reason = skips[0].reason.lower()
        assert "permission" in reason or "access" in reason

    def test_403_code_also_treated_as_missing_permission(self):
        """Some clients surface AccessDenied as '403'."""
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error("403")
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1


# ---------------------------------------------------------------------------
# Skip: unreadable configuration — unexpected ClientError or other exception
# (Req. 3.6)
# ---------------------------------------------------------------------------


class TestUnreadableConfiguration:
    def test_unexpected_client_error_returns_skip(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error("InternalError")
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1

    def test_generic_exception_returns_skip(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = ConnectionError("network failure")
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1

    def test_skip_report_identifies_bucket_on_generic_error(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = RuntimeError("oops")
        _, skips = get_replication_rules(client, _bucket(name="flaky-bucket"))

        assert skips[0].source_bucket == "flaky-bucket"

    def test_skip_report_reason_mentions_unreadable(self):
        client = MagicMock()
        client.get_bucket_replication.side_effect = _client_error("InternalError")
        _, skips = get_replication_rules(client, _bucket())

        assert "Unreadable" in skips[0].reason or "unreadable" in skips[0].reason.lower()


# ---------------------------------------------------------------------------
# Skip: zero tag-scoped rules in an existing configuration (Req. 3.5)
# ---------------------------------------------------------------------------


class TestZeroTagScopedRules:
    def test_prefix_only_config_produces_skip(self):
        """A config with only prefix-only rules yields zero derived rules → skip."""
        client = _s3_client_returning(
            _replication_response(_prefix_rule("rule-prefix"))
        )
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1

    def test_empty_rules_list_produces_skip(self):
        client = _s3_client_returning(
            {"ReplicationConfiguration": {"Role": _ROLE_ARN, "Rules": []}}
        )
        rules, skips = get_replication_rules(client, _bucket())

        assert rules == []
        assert len(skips) == 1

    def test_skip_report_reason_mentions_zero_rules(self):
        client = _s3_client_returning(
            _replication_response(_prefix_rule("r"))
        )
        _, skips = get_replication_rules(client, _bucket())

        reason = skips[0].reason.lower()
        assert "zero" in reason or "no" in reason

    def test_skip_report_identifies_bucket(self):
        client = _s3_client_returning(
            _replication_response(_prefix_rule("r"))
        )
        _, skips = get_replication_rules(client, _bucket(name="no-tag-rules-bucket"))

        assert skips[0].source_bucket == "no-tag-rules-bucket"


# ---------------------------------------------------------------------------
# SkipReport dataclass shape
# ---------------------------------------------------------------------------


class TestSkipReportShape:
    def test_skip_report_has_required_fields(self):
        report = SkipReport(source_bucket="b", reason="r")
        assert report.source_bucket == "b"
        assert report.reason == "r"
        assert report.component == _COMPONENT

    def test_skip_report_component_can_be_overridden(self):
        report = SkipReport(source_bucket="b", reason="r", component="other")
        assert report.component == "other"
