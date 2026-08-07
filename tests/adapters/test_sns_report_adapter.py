"""Tests for src/adapters/sns_report_adapter.py — task 16.2.

Unit tests for the Completion_Report publish adapter:
  - Successful publish returns a success PublishResult with the message id.
  - A ClientError from sns.publish returns a failure result without raising.
  - A simulated timeout returns a failure result without raising.

Requirements: 4.5
"""
from __future__ import annotations

import concurrent.futures
import json
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from src.adapters.sns_report_adapter import PublishResult, publish_completion_report

_TOPIC_ARN = "arn:aws:sns:us-west-2:123456789012:CompletionReportTopic"
_MESSAGE_ID = "11111111-2222-3333-4444-555555555555"
_REPORT = {
    "job_id": "job-abc",
    "source_bucket": "my-source-bucket",
    "replication_config_id": "cfg-1",
    "item_count": 2,
    "outcome_counts": {"COMPLETE": 2},
    "items": [
        {"object_key": "a.txt", "version_id": None, "outcome": "COMPLETE"},
        {"object_key": "b.txt", "version_id": "v1", "outcome": "COMPLETE"},
    ],
}


def _mock_sns(message_id: str = _MESSAGE_ID) -> MagicMock:
    client = MagicMock()
    client.publish.return_value = {"MessageId": message_id}
    return client


def _client_error(code: str, message: str = "error") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "Publish")


class TestSuccessfulPublish:
    def test_publish_called_once_with_topic_and_message(self):
        client = _mock_sns()
        publish_completion_report(client, _TOPIC_ARN, _REPORT)
        client.publish.assert_called_once()
        kwargs = client.publish.call_args[1]
        assert kwargs["TopicArn"] == _TOPIC_ARN
        assert json.loads(kwargs["Message"]) == _REPORT

    def test_message_body_is_strictly_valid_json(self):
        """The message body must remain valid JSON end-to-end, so any SNS
        subscriber protocol other than email (SQS, Lambda, HTTPS) can still
        json.loads() it without special-casing a non-JSON preamble."""
        client = _mock_sns()
        publish_completion_report(client, _TOPIC_ARN, _REPORT)
        message = client.publish.call_args[1]["Message"]
        json.loads(message)  # must not raise

    def test_message_body_is_pretty_printed_for_readability(self):
        client = _mock_sns()
        publish_completion_report(client, _TOPIC_ARN, _REPORT)
        message = client.publish.call_args[1]["Message"]
        assert "\n" in message  # indent=2 produces multi-line output

    def test_successful_publish_returns_success_result(self):
        client = _mock_sns()
        result = publish_completion_report(client, _TOPIC_ARN, _REPORT)
        assert isinstance(result, PublishResult)
        assert result.success is True
        assert result.message_id == _MESSAGE_ID
        assert result.error_reason is None


class TestClientErrorFailure:
    def test_client_error_returns_failure_without_raising(self):
        client = MagicMock()
        client.publish.side_effect = _client_error("AccessDenied", "Not authorized")
        result = publish_completion_report(client, _TOPIC_ARN, _REPORT)
        assert result.success is False
        assert result.message_id is None
        assert result.error_reason is not None
        assert "AccessDenied" in result.error_reason

    def test_generic_exception_returns_failure_without_raising(self):
        client = MagicMock()
        client.publish.side_effect = ConnectionError("network stall")
        result = publish_completion_report(client, _TOPIC_ARN, _REPORT)
        assert result.success is False
        assert result.error_reason is not None
        assert len(result.error_reason) > 0


class TestTimeoutFailure:
    def test_timeout_returns_failure_without_raising(self):
        client = _mock_sns()
        with patch(
            "src.adapters._aws_call_helpers.concurrent.futures.ThreadPoolExecutor"
        ) as mock_executor_cls:
            mock_pool = MagicMock()
            mock_future = MagicMock()
            mock_future.result.side_effect = concurrent.futures.TimeoutError()
            mock_pool.submit.return_value = mock_future
            mock_executor_cls.return_value.__enter__.return_value = mock_pool

            result = publish_completion_report(client, _TOPIC_ARN, _REPORT)

        assert result.success is False
        assert result.message_id is None
        assert result.error_reason is not None
        assert "did not complete within" in result.error_reason
