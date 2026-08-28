"""Focused tests for the reduced completion-tracking publish interval.

**Validates: Requirements 3.5**
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

from src.core.models import MonitoredBucket
from src.orchestrator import _run_completion_tracking_interval
from tests.support import mock_state_store


_STATE_BUCKET = "state-bucket"
_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:completion-reports"


def _bucket(name: str, region: str = "us-east-1") -> MonitoredBucket:
    return MonitoredBucket(name=name, region=region)


def test_feature_gate_skips_all_clients_and_state_reads() -> None:
    """An unset topic disables the isolated publish phase entirely."""
    factory = MagicMock()
    store = mock_state_store()

    _run_completion_tracking_interval(
        buckets=[_bucket("source-a")],
        factory=factory,
        store=store,
        state_bucket=_STATE_BUCKET,
    )

    factory.create_s3_client.assert_not_called()
    factory.create_sns_client.assert_not_called()
    store.get_all_completion_items.assert_not_called()
    store.get_scan_state.assert_not_called()


def test_s3_client_failure_for_one_bucket_does_not_block_later_bucket() -> None:
    """Per-bucket isolation includes constructing the bucket's S3 client."""
    first_client_failure = RuntimeError("first client unavailable")
    second_client = MagicMock(name="second_s3_client")
    factory = MagicMock()
    factory.create_s3_client.side_effect = [first_client_failure, second_client]
    store = mock_state_store()
    store.get_all_completion_items.return_value = {}
    emitted: list[dict] = []

    with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
        _run_completion_tracking_interval(
            buckets=[_bucket("source-a"), _bucket("source-b", "us-west-2")],
            factory=factory,
            store=store,
            state_bucket=_STATE_BUCKET,
            completion_report_topic_arn=_TOPIC_ARN,
        )

    assert factory.create_s3_client.call_args_list == [
        call(region="us-east-1"),
        call(region="us-west-2"),
    ]
    store.get_all_completion_items.assert_called_once_with(
        second_client, _STATE_BUCKET, "source-b"
    )
    assert any(
        entry.get("component") == "Completion_Tracker"
        and entry.get("bucket") == "source-a"
        and "first client unavailable" in entry.get("cause", "")
        for entry in emitted
    )


def test_publish_phase_reads_state_without_any_migration_call() -> None:
    """Reading for publish is the phase's first state operation.

    An earlier design ran a legacy-state migration here. It could never
    succeed, and its failure branch skipped the affected bucket's whole publish
    phase on every interval. Upgrading from 1.0.1 is now a reinstall, so no
    1.0.1 state reaches a running 1.1.0 stack (design.md Decision 5).
    """
    s3_client = MagicMock(name="s3_client")
    factory = MagicMock()
    factory.create_s3_client.return_value = s3_client
    store = mock_state_store()
    store.get_all_completion_items.return_value = {}

    _run_completion_tracking_interval(
        buckets=[_bucket("source-a")],
        factory=factory,
        store=store,
        state_bucket=_STATE_BUCKET,
        completion_report_topic_arn=_TOPIC_ARN,
    )

    assert store.method_calls[0] == call.get_all_completion_items(
        s3_client, _STATE_BUCKET, "source-a"
    )
    assert not any(
        "migrate" in str(entry) for entry in store.method_calls
    ), store.method_calls


def test_read_failure_for_one_bucket_does_not_block_later_bucket() -> None:
    """A failed state read prevents only that bucket from reaching publish."""
    first_client = MagicMock(name="first_s3_client")
    second_client = MagicMock(name="second_s3_client")
    factory = MagicMock()
    factory.create_s3_client.side_effect = [first_client, second_client]
    store = mock_state_store()
    store.get_all_completion_items.side_effect = [
        RuntimeError("state read conflicted"),
        {},
    ]
    emitted: list[dict] = []

    with patch("src.orchestrator.observability.emit", side_effect=emitted.append):
        _run_completion_tracking_interval(
            buckets=[_bucket("source-a"), _bucket("source-b", "us-west-2")],
            factory=factory,
            store=store,
            state_bucket=_STATE_BUCKET,
            completion_report_topic_arn=_TOPIC_ARN,
        )

    assert store.get_all_completion_items.call_count == 2
    assert store.get_all_completion_items.call_args_list[1] == call(
        second_client, _STATE_BUCKET, "source-b"
    )
    assert any(
        entry.get("component") == "Completion_Tracker"
        and entry.get("bucket") == "source-a"
        and "state read conflicted" in entry.get("cause", "")
        for entry in emitted
    )
