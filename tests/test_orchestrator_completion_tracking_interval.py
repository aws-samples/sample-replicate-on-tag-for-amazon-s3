"""Tests for ``src/orchestrator.py::_run_completion_tracking_interval`` — tasks 18.1, 18.2.

Covers the source-only, per-object (``TrackedObject``) rebuild of the
check-and-reconcile and publish phases: ``store.get_check_eligible_items``,
``completion_tracker.select_check_candidates``, the ``_check_batch``
concurrent-check helper, ``completion_tracker.reconcile_source_status_check``,
``store.apply_completion_resolutions``, ``store.get_all_completion_items``,
``completion_tracker.should_publish``, ``completion_tracker.build_completion_report``,
and ``store.delete_completion_items``.

This REPLACES the superseded destination-polling test file entirely: there
is no ``CompletionItem``/``DestinationOutcome``, no ``CheckKind``, no age
gate, and no destination-presence client anywhere in this file.

Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 4.1, 4.4, 4.5, 4.6, 5.4, 6.2, 7.1,
7.2, 7.3
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.adapters.sns_report_adapter import PublishResult
from src.adapters.source_status_adapter import SourceStatusCheckKind, SourceStatusResult
from src.core.models import (
    CompletionState,
    ConfigContext,
    MonitoredBucket,
    ScanState,
    TrackedObject,
)
from src.orchestrator import _run_completion_tracking_interval

_NOW = datetime.now(tz=timezone.utc)
_STATE_BUCKET = "scratch-state-bucket"
_SOURCE_BUCKET = "source-bucket-a"


def _bucket(name: str = _SOURCE_BUCKET, region: str = "us-east-1") -> MonitoredBucket:
    return MonitoredBucket(name=name, region=region)


def _config_context(
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
    manifest_generated_at: datetime = _NOW - timedelta(days=2),
    bops_confirmed: bool = True,
) -> ConfigContext:
    return ConfigContext(
        replication_config_id=replication_config_id,
        job_id=job_id,
        manifest_generated_at=manifest_generated_at,
        bops_confirmed=bops_confirmed,
    )


def _obj(
    object_key: str = "path/obj.txt",
    version_id: str | None = "v1",
    configs: dict | None = None,
    state: CompletionState = CompletionState.PENDING,
    replication_outcome: str | None = None,
    source_bucket: str = _SOURCE_BUCKET,
) -> TrackedObject:
    return TrackedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
        configs=configs if configs is not None else {"cfg-1": _config_context()},
        state=state,
        replication_outcome=replication_outcome,
    )


def _make_factory() -> MagicMock:
    factory = MagicMock()
    factory.create_s3_client.return_value = MagicMock(name="s3_client")
    factory.create_sns_client.return_value = MagicMock(name="sns_client")
    return factory


def _make_store(
    eligible_items_by_bucket: dict[str, dict[str, TrackedObject]] | None = None,
    all_items_by_bucket: dict[str, dict[str, TrackedObject]] | None = None,
    scan_state_by_bucket: dict[str, dict[str, ScanState]] | None = None,
) -> MagicMock:
    store = MagicMock()

    def _get_eligible(_s3_client, _state_bucket, source_bucket):
        return (eligible_items_by_bucket or {}).get(source_bucket, {})

    def _get_all(_s3_client, _state_bucket, source_bucket):
        return (all_items_by_bucket or {}).get(source_bucket, {})

    def _get_scan_state(_s3_client, _state_bucket, source_bucket):
        return (scan_state_by_bucket or {}).get(source_bucket, {})

    store.get_check_eligible_items.side_effect = _get_eligible
    store.get_all_completion_items.side_effect = _get_all
    store.get_scan_state.side_effect = _get_scan_state
    store.get_checkpoint.return_value = (MagicMock(), "checkpoint-etag")
    store.apply_completion_resolutions.side_effect = (
        lambda s3_client, state_bucket, source_bucket, mutate_fn, **kw: mutate_fn({})
    )
    return store


def _run(buckets, factory, store, **kwargs):
    _run_completion_tracking_interval(
        buckets=buckets,
        factory=factory,
        store=store,
        state_bucket=_STATE_BUCKET,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Check-and-reconcile phase (task 18.1)
# ---------------------------------------------------------------------------


def test_completed_header_resolves_complete():
    obj = _obj()
    eligible = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    captured = {}

    def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
        captured["result"] = mutate_fn({})
        return "etag-1"

    store.apply_completion_resolutions.side_effect = _apply

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED"),
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={"key-1": obj},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([_bucket()], factory, store)

    resolved = captured["result"]["completion_items"]["key-1"]
    assert resolved.state == CompletionState.RESOLVED
    assert resolved.replication_outcome == "COMPLETE"
    assert resolved.resolution_method == "source_status_header"


def test_pending_header_resolves_pending_verbatim():
    obj = _obj()
    eligible = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    captured = {}

    def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
        captured["result"] = mutate_fn({})
        return "etag-1"

    store.apply_completion_resolutions.side_effect = _apply

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="PENDING"),
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={"key-1": obj},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([_bucket()], factory, store)

    resolved = captured["result"]["completion_items"]["key-1"]
    assert resolved.state == CompletionState.RESOLVED
    assert resolved.replication_outcome == "PENDING"


def test_header_absent_resolves_unknown_and_logs_error():
    obj = _obj()
    eligible = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    emitted: list = []

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT),
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={"key-1": obj},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ), patch(
        "src.orchestrator.observability.emit", side_effect=emitted.append
    ):
        _run([_bucket()], factory, store)

    errors = [e for e in emitted if e.get("event") == "error" and e.get("component") == "Completion_Tracker"]
    assert any("header absent" in e.get("cause", "").lower() for e in errors)
    # job_id/replication_config_id present, object key absent from log entries.
    causes = " ".join(e.get("cause", "") for e in errors)
    assert "job-1" in causes
    assert "cfg-1" in causes
    assert obj.object_key not in causes


def test_check_failed_leaves_object_pending_untouched():
    obj = _obj()
    eligible = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    captured = {}

    def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
        captured["result"] = mutate_fn({})
        return "etag-1"

    store.apply_completion_resolutions.side_effect = _apply

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.CHECK_FAILED, error_reason="throttled"),
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={"key-1": obj},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([_bucket()], factory, store)

    # The item is persisted unchanged — still PENDING — for retry next
    # interval (Requirement 3.6).
    resolved = captured["result"]["completion_items"]["key-1"]
    assert resolved.state == CompletionState.PENDING


def test_unconfirmed_config_never_checked():
    """A TrackedObject with an unconfirmed ConfigContext is excluded by
    get_check_eligible_items (mocked here to simulate that filtering), so
    no HeadObject call is ever issued for it."""
    obj = _obj(configs={"cfg-1": _config_context(bops_confirmed=False)})
    # Simulate get_check_eligible_items already filtering this out.
    store = _make_store(eligible_items_by_bucket={_SOURCE_BUCKET: {}})
    factory = _make_factory()

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status"
    ) as check_mock:
        _run([_bucket()], factory, store)

    check_mock.assert_not_called()


def test_single_candidate_failure_does_not_block_others():
    obj_a = _obj(object_key="a.txt", configs={"cfg-1": _config_context(job_id="job-a")})
    obj_b = _obj(object_key="b.txt", configs={"cfg-1": _config_context(job_id="job-b")})
    eligible = {_SOURCE_BUCKET: {"key-a": obj_a, "key-b": obj_b}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    captured = {}

    def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
        captured["result"] = mutate_fn({})
        return "etag-1"

    store.apply_completion_resolutions.side_effect = _apply

    def _check(_s3_client, _source_bucket, object_key, _version_id):
        if object_key == "a.txt":
            raise RuntimeError("simulated check failure")
        return SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED")

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        side_effect=_check,
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={"key-a": obj_a, "key-b": obj_b},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([_bucket()], factory, store)

    result = captured["result"]["completion_items"]
    assert result["key-b"].replication_outcome == "COMPLETE"
    assert result["key-a"].state == CompletionState.PENDING


def test_bucket_persistence_failure_does_not_block_other_buckets():
    bucket_a = _bucket(name="bucket-a")
    bucket_b = _bucket(name="bucket-b")

    obj_a = _obj(object_key="a.txt", source_bucket="bucket-a")
    obj_b = _obj(object_key="b.txt", source_bucket="bucket-b")
    eligible = {"bucket-a": {"key-a": obj_a}, "bucket-b": {"key-b": obj_b}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    apply_calls = []

    def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
        apply_calls.append(source_bucket)
        if source_bucket == "bucket-a":
            raise RuntimeError("simulated persistence failure")
        return mutate_fn({})

    store.apply_completion_resolutions.side_effect = _apply

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED"),
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        side_effect=lambda payload: {"key-a": obj_a} if payload.get("_b") is None else {"key-b": obj_b},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([bucket_a, bucket_b], factory, store)

    assert set(apply_calls) == {"bucket-a", "bucket-b"}


def test_cross_bucket_ordering_and_cap():
    bucket_a = _bucket(name="bucket-a")
    bucket_b = _bucket(name="bucket-b")

    older_manifest = _NOW - timedelta(days=5)
    newer_manifest = _NOW - timedelta(days=1)

    obj_a = _obj(
        object_key="a.txt",
        source_bucket="bucket-a",
        configs={"cfg-1": _config_context(manifest_generated_at=older_manifest)},
    )
    obj_b = _obj(
        object_key="b.txt",
        source_bucket="bucket-b",
        configs={"cfg-1": _config_context(manifest_generated_at=newer_manifest)},
    )
    eligible = {"bucket-a": {"key-a": obj_a}, "bucket-b": {"key-b": obj_b}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    check_mock = MagicMock(
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED")
    )

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status", check_mock
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([bucket_a, bucket_b], factory, store, check_batch_size=1)

    assert check_mock.call_count == 1
    checked_key = check_mock.call_args[0][2]
    assert checked_key == "a.txt"


def test_no_destination_client_ever_constructed():
    obj = _obj()
    eligible = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(eligible_items_by_bucket=eligible)
    factory = _make_factory()

    with patch(
        "src.orchestrator.source_status_adapter.check_source_replication_status",
        return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED"),
    ), patch(
        "src.orchestrator.completion_serializer.deserialize_completion_items",
        return_value={},
    ), patch(
        "src.orchestrator.completion_serializer.serialize_completion_items",
        side_effect=lambda items: items,
    ):
        _run([_bucket()], factory, store)

    # Only source-side clients (s3, sns) are ever constructed by the factory.
    for call in factory.method_calls:
        assert call[0] in ("create_s3_client", "create_sns_client")


# ---------------------------------------------------------------------------
# Publish phase (task 18.2)
# ---------------------------------------------------------------------------


def _resolved_obj(
    object_key: str = "resolved.txt",
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
    manifest_generated_at: datetime = _NOW - timedelta(days=2),
    replication_outcome: str = "COMPLETE",
) -> TrackedObject:
    return TrackedObject(
        source_bucket=_SOURCE_BUCKET,
        object_key=object_key,
        version_id="v1",
        configs={
            replication_config_id: _config_context(
                replication_config_id=replication_config_id,
                job_id=job_id,
                manifest_generated_at=manifest_generated_at,
            )
        },
        state=CompletionState.RESOLVED,
        resolved_at=_NOW - timedelta(hours=1),
        resolution_method="source_status_header",
        replication_outcome=replication_outcome,
    )


def _quiescent_scan_state(manifest_generated_at: datetime) -> ScanState:
    return ScanState(last_scan_at=manifest_generated_at + timedelta(minutes=10), last_scan_match_count=0)


def test_publish_success_deletes_items_and_emits_audit():
    obj = _resolved_obj()
    all_items = {_SOURCE_BUCKET: {"key-1": obj}}
    scan_state = {_SOURCE_BUCKET: {"cfg-1": _quiescent_scan_state(obj.configs["cfg-1"].manifest_generated_at)}}
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket=scan_state)
    factory = _make_factory()

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report",
        return_value=PublishResult(success=True, message_id="msg-1"),
    ) as publish_mock:
        _run(
            [_bucket()],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    assert publish_mock.call_count == 1
    report = publish_mock.call_args[0][2]
    assert report["source_bucket"] == _SOURCE_BUCKET
    assert report["item_count"] == 1

    store.delete_completion_items.assert_called_once()
    call_args = store.delete_completion_items.call_args[0]
    assert call_args[2] == _SOURCE_BUCKET
    assert call_args[3] == ["key-1"]


def test_publish_excludes_pending_item():
    obj = _obj(object_key="pending.txt")
    all_items = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket={})
    factory = _make_factory()

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report"
    ) as publish_mock:
        _run(
            [_bucket()],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    publish_mock.assert_not_called()
    store.delete_completion_items.assert_not_called()


def test_publish_mixed_batch_excludes_pending_includes_resolved():
    resolved = _resolved_obj(object_key="resolved.txt")
    pending = _obj(object_key="pending.txt")
    all_items = {_SOURCE_BUCKET: {"key-pending": pending, "key-resolved": resolved}}
    scan_state = {_SOURCE_BUCKET: {"cfg-1": _quiescent_scan_state(resolved.configs["cfg-1"].manifest_generated_at)}}
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket=scan_state)
    factory = _make_factory()

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report",
        return_value=PublishResult(success=True, message_id="msg-1"),
    ) as publish_mock:
        _run(
            [_bucket()],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    report = publish_mock.call_args[0][2]
    assert report["item_count"] == 1
    assert report["items"][0]["object_key"] == "resolved.txt"

    deleted_keys = store.delete_completion_items.call_args[0][3]
    assert deleted_keys == ["key-resolved"]


def test_publish_multi_config_item_aggregate_outcome():
    obj = TrackedObject(
        source_bucket=_SOURCE_BUCKET,
        object_key="multi.txt",
        version_id="v1",
        configs={
            "cfg-a": _config_context(replication_config_id="cfg-a", job_id="job-a"),
            "cfg-b": _config_context(replication_config_id="cfg-b", job_id="job-b"),
        },
        state=CompletionState.RESOLVED,
        resolved_at=_NOW - timedelta(hours=1),
        resolution_method="source_status_header",
        replication_outcome="COMPLETE",
    )
    all_items = {_SOURCE_BUCKET: {"key-1": obj}}
    scan_state = {
        _SOURCE_BUCKET: {
            "cfg-a": _quiescent_scan_state(obj.configs["cfg-a"].manifest_generated_at),
            "cfg-b": _quiescent_scan_state(obj.configs["cfg-b"].manifest_generated_at),
        }
    }
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket=scan_state)
    factory = _make_factory()

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report",
        return_value=PublishResult(success=True, message_id="msg-1"),
    ) as publish_mock:
        _run(
            [_bucket()],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    report = publish_mock.call_args[0][2]
    assert report["item_count"] == 1
    assert set(report["items"][0]["destinations"]) == {"cfg-a", "cfg-b"}
    assert report["items"][0]["outcome"] == "COMPLETE"
    assert report["outcome_counts"]["COMPLETE"] == 1


def test_publish_failure_never_deletes_and_logs_error():
    obj = _resolved_obj()
    all_items = {_SOURCE_BUCKET: {"key-1": obj}}
    scan_state = {_SOURCE_BUCKET: {"cfg-1": _quiescent_scan_state(obj.configs["cfg-1"].manifest_generated_at)}}
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket=scan_state)
    factory = _make_factory()

    emitted: list = []

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report",
        return_value=PublishResult(success=False, error_reason="AccessDenied"),
    ), patch(
        "src.orchestrator.observability.emit", side_effect=emitted.append
    ):
        _run(
            [_bucket()],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    store.delete_completion_items.assert_not_called()
    causes = [e.get("cause", "") for e in emitted if e.get("event") == "error"]
    assert any("Failed to publish Completion_Report" in c for c in causes)


def test_publish_skipped_entirely_when_topic_arn_unset():
    obj = _resolved_obj()
    all_items = {_SOURCE_BUCKET: {"key-1": obj}}
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket={})
    factory = _make_factory()

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report"
    ) as publish_mock:
        _run([_bucket()], factory, store)  # no completion_report_topic_arn

    publish_mock.assert_not_called()
    store.get_all_completion_items.assert_not_called()
    store.get_scan_state.assert_not_called()
    factory.create_sns_client.assert_not_called()
    store.delete_completion_items.assert_not_called()


def test_publish_multi_bucket_isolation():
    bucket_a = _bucket(name="bucket-a")
    bucket_b = _bucket(name="bucket-b")

    obj_a = TrackedObject(
        source_bucket="bucket-a",
        object_key="a.txt",
        version_id="v1",
        configs={"cfg-1": _config_context()},
        state=CompletionState.RESOLVED,
        resolved_at=_NOW,
        resolution_method="source_status_header",
        replication_outcome="COMPLETE",
    )
    obj_b = TrackedObject(
        source_bucket="bucket-b",
        object_key="b.txt",
        version_id="v1",
        configs={"cfg-1": _config_context()},
        state=CompletionState.RESOLVED,
        resolved_at=_NOW,
        resolution_method="source_status_header",
        replication_outcome="COMPLETE",
    )
    all_items = {"bucket-a": {"key-a": obj_a}, "bucket-b": {"key-b": obj_b}}
    scan_state = {
        "bucket-a": {"cfg-1": _quiescent_scan_state(obj_a.configs["cfg-1"].manifest_generated_at)},
        "bucket-b": {"cfg-1": _quiescent_scan_state(obj_b.configs["cfg-1"].manifest_generated_at)},
    }
    store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket=scan_state)
    factory = _make_factory()

    def _publish(_sns_client, _topic_arn, report, subject=""):
        # subject is accepted because the orchestrator supplies the SNS email
        # subject line alongside the report body.
        assert subject
        if report["source_bucket"] == "bucket-a":
            return PublishResult(success=False, error_reason="throttled")
        return PublishResult(success=True, message_id="msg-b")

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report", side_effect=_publish
    ):
        _run(
            [bucket_a, bucket_b],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    deleted_buckets = [call.args[2] for call in store.delete_completion_items.call_args_list]
    assert deleted_buckets == ["bucket-b"]


def test_publish_item_absent_from_check_eligible_but_present_in_full_set():
    """A fully-RESOLVED item is excluded from get_check_eligible_items (it's
    no longer PENDING), but must still be correctly evaluated and published
    via get_all_completion_items."""
    obj = _resolved_obj(object_key="fully-resolved.txt")
    eligible_items_by_bucket: dict[str, dict] = {_SOURCE_BUCKET: {}}
    all_items = {_SOURCE_BUCKET: {"key-1": obj}}
    scan_state = {_SOURCE_BUCKET: {"cfg-1": _quiescent_scan_state(obj.configs["cfg-1"].manifest_generated_at)}}
    store = _make_store(
        eligible_items_by_bucket=eligible_items_by_bucket,
        all_items_by_bucket=all_items,
        scan_state_by_bucket=scan_state,
    )
    factory = _make_factory()

    with patch(
        "src.orchestrator.sns_report_adapter.publish_completion_report",
        return_value=PublishResult(success=True, message_id="msg-1"),
    ) as publish_mock:
        _run(
            [_bucket()],
            factory,
            store,
            completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
        )

    assert publish_mock.call_count == 1
    report = publish_mock.call_args[0][2]
    assert report["item_count"] == 1
    assert report["items"][0]["object_key"] == "fully-resolved.txt"
    store.delete_completion_items.assert_called_once()


# ---------------------------------------------------------------------------
# Property tests (tasks 18.4-18.7)
# ---------------------------------------------------------------------------

from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Property 11: A publish failure is retried, and a confirmed publish is
# never repeated for any covered Tracked_Object
# Feature: source-status-completion-tracking, Property 11: A publish failure is retried, and a confirmed publish is never repeated for any covered Tracked_Object
# Validates: Requirements 4.5, 4.6
# ---------------------------------------------------------------------------


class TestProperty11PublishRetryAndNoRepeat:
    """# Feature: source-status-completion-tracking, Property 11: A publish failure is retried, and a confirmed publish is never repeated for any covered Tracked_Object

    Validates: Requirements 4.5, 4.6
    """

    @given(
        item_count=st.integers(min_value=1, max_value=6),
        publish_succeeds=st.booleans(),
    )
    @settings(max_examples=100)
    def test_publish_retry_and_no_repeat_guarantee(
        self, item_count: int, publish_succeeds: bool
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 11: A publish failure is retried, and a confirmed publish is never repeated for any covered Tracked_Object"""
        items = {
            f"key-{i}": _resolved_obj(object_key=f"obj-{i}.txt", job_id=f"job-{i}")
            for i in range(item_count)
        }
        scan_state = {
            "cfg-1": _quiescent_scan_state(
                next(iter(items.values())).configs["cfg-1"].manifest_generated_at
            )
        }
        all_items = {_SOURCE_BUCKET: items}
        store = _make_store(all_items_by_bucket=all_items, scan_state_by_bucket={_SOURCE_BUCKET: scan_state})
        factory = _make_factory()

        publish_result = (
            PublishResult(success=True, message_id="msg-1")
            if publish_succeeds
            else PublishResult(success=False, error_reason="throttled")
        )

        with patch(
            "src.orchestrator.sns_report_adapter.publish_completion_report",
            return_value=publish_result,
        ):
            _run(
                [_bucket()],
                factory,
                store,
                completion_report_topic_arn="arn:aws:sns:us-east-1:123456789012:CompletionReportTopic",
            )

        if publish_succeeds:
            # Every covered item was deleted -> no subsequent evaluation
            # could ever publish it again.
            deleted_keys = set(store.delete_completion_items.call_args[0][3])
            assert deleted_keys == set(items.keys())
        else:
            # Nothing was deleted -> every item is still present, unchanged,
            # and would satisfy should_publish again on the next evaluation.
            store.delete_completion_items.assert_not_called()
            from src.core.completion_tracker import should_publish

            for item in items.values():
                assert should_publish(item, scan_state) is True


# ---------------------------------------------------------------------------
# Property 13: A single failing check or bucket never blocks the rest of
# its batch
# Feature: source-status-completion-tracking, Property 13: A single failing check or bucket never blocks the rest of its batch
# Validates: Requirements 6.2
# ---------------------------------------------------------------------------


class TestProperty13SingleFailureNeverBlocksBatch:
    """# Feature: source-status-completion-tracking, Property 13: A single failing check or bucket never blocks the rest of its batch

    Validates: Requirements 6.2
    """

    @given(
        item_count=st.integers(min_value=2, max_value=6),
        failing_indices=st.lists(st.integers(min_value=0, max_value=5), min_size=1, max_size=5, unique=True),
    )
    @settings(max_examples=100)
    def test_failing_subset_of_check_candidates_does_not_block_rest(
        self, item_count: int, failing_indices: list[int]
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 13: A single failing check or bucket never blocks the rest of its batch"""
        failing_set = {i for i in failing_indices if i < item_count}
        items = {
            f"key-{i}": _obj(object_key=f"obj-{i}.txt", configs={"cfg-1": _config_context(job_id=f"job-{i}")})
            for i in range(item_count)
        }
        store = _make_store(eligible_items_by_bucket={_SOURCE_BUCKET: items})
        factory = _make_factory()

        captured = {}

        def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
            captured["result"] = mutate_fn({})
            return "etag-1"

        store.apply_completion_resolutions.side_effect = _apply

        def _check(_s3_client, _source_bucket, object_key, _version_id):
            idx = int(object_key.split("-")[1].split(".")[0])
            if idx in failing_set:
                raise RuntimeError("simulated check failure")
            return SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED")

        with patch(
            "src.orchestrator.source_status_adapter.check_source_replication_status",
            side_effect=_check,
        ), patch(
            "src.orchestrator.completion_serializer.deserialize_completion_items",
            return_value=items,
        ), patch(
            "src.orchestrator.completion_serializer.serialize_completion_items",
            side_effect=lambda x: x,
        ):
            _run([_bucket()], factory, store)

        succeeding_indices = [i for i in range(item_count) if i not in failing_set]
        if not succeeding_indices:
            # Every candidate failed -> no resolutions to persist at all;
            # apply_completion_resolutions is correctly never invoked.
            store.apply_completion_resolutions.assert_not_called()
            return

        result = captured["result"]["completion_items"]
        for i in range(item_count):
            if i in failing_set:
                assert result[f"key-{i}"].state == CompletionState.PENDING
            else:
                assert result[f"key-{i}"].replication_outcome == "COMPLETE"

    @given(
        bucket_count=st.integers(min_value=2, max_value=5),
        failing_indices=st.lists(st.integers(min_value=0, max_value=4), min_size=1, max_size=4, unique=True),
    )
    @settings(max_examples=100)
    def test_failing_subset_of_buckets_does_not_block_rest(
        self, bucket_count: int, failing_indices: list[int]
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 13: A single failing check or bucket never blocks the rest of its batch"""
        failing_set = {i for i in failing_indices if i < bucket_count}
        bucket_names = [f"bucket-{i:02d}" for i in range(bucket_count)]
        buckets = [_bucket(name=n) for n in bucket_names]

        eligible = {n: {f"key-{n}": _obj(object_key=f"{n}.txt", source_bucket=n)} for n in bucket_names}
        store = _make_store(eligible_items_by_bucket=eligible)
        factory = _make_factory()

        apply_calls = []

        def _apply(s3_client, state_bucket, source_bucket, mutate_fn, **kw):
            apply_calls.append(source_bucket)
            if source_bucket in {bucket_names[i] for i in failing_set}:
                raise RuntimeError("simulated persistence failure")
            return mutate_fn({})

        store.apply_completion_resolutions.side_effect = _apply

        with patch(
            "src.orchestrator.source_status_adapter.check_source_replication_status",
            return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED"),
        ), patch(
            "src.orchestrator.completion_serializer.deserialize_completion_items",
            side_effect=lambda payload: {},
        ), patch(
            "src.orchestrator.completion_serializer.serialize_completion_items",
            side_effect=lambda x: x,
        ):
            _run(buckets, factory, store)

        # Every bucket's resolution attempt was made, regardless of the
        # failing subset.
        assert set(apply_calls) == set(bucket_names)


# ---------------------------------------------------------------------------
# Property 14: No log entry is emitted on resolution, and every error entry
# is job-scoped only
# Feature: source-status-completion-tracking, Property 14: No log entry is emitted on resolution, and every error entry is job-scoped only
# Validates: Requirements 7.1, 7.2
# ---------------------------------------------------------------------------


class TestProperty14LoggingScopedToJob:
    """# Feature: source-status-completion-tracking, Property 14: No log entry is emitted on resolution, and every error entry is job-scoped only

    Validates: Requirements 7.1, 7.2
    """

    @given(header_value=st.sampled_from(["COMPLETED", "PENDING", "FAILED"]))
    @settings(max_examples=100)
    def test_clean_resolution_emits_no_log_entry(self, header_value: str) -> None:
        """# Feature: source-status-completion-tracking, Property 14: No log entry is emitted on resolution, and every error entry is job-scoped only"""
        obj = _obj(configs={"cfg-1": _config_context(job_id="job-clean")})
        eligible = {_SOURCE_BUCKET: {"key-1": obj}}
        store = _make_store(eligible_items_by_bucket=eligible)
        factory = _make_factory()

        emitted: list = []

        with patch(
            "src.orchestrator.source_status_adapter.check_source_replication_status",
            return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value=header_value),
        ), patch(
            "src.orchestrator.completion_serializer.deserialize_completion_items",
            return_value={"key-1": obj},
        ), patch(
            "src.orchestrator.completion_serializer.serialize_completion_items",
            side_effect=lambda x: x,
        ), patch(
            "src.orchestrator.observability.emit", side_effect=emitted.append
        ):
            _run([_bucket()], factory, store)

        errors = [e for e in emitted if e.get("event") == "error"]
        assert errors == []

    @given(
        job_id=st.text(
            alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E, blacklist_characters="\\'\""),
            min_size=1,
            max_size=20,
        ).map(lambda s: f"JOBID-{s}"),
        config_id=st.text(
            alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E, blacklist_characters="\\'\""),
            min_size=1,
            max_size=20,
        ).map(lambda s: f"CFGID-{s}"),
    )
    @settings(max_examples=100)
    def test_header_absent_error_entry_is_job_scoped(self, job_id: str, config_id: str) -> None:
        """# Feature: source-status-completion-tracking, Property 14: No log entry is emitted on resolution, and every error entry is job-scoped only"""
        obj = _obj(configs={config_id: _config_context(replication_config_id=config_id, job_id=job_id)})
        eligible = {_SOURCE_BUCKET: {"key-1": obj}}
        store = _make_store(eligible_items_by_bucket=eligible)
        factory = _make_factory()

        emitted: list = []

        with patch(
            "src.orchestrator.source_status_adapter.check_source_replication_status",
            return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT),
        ), patch(
            "src.orchestrator.completion_serializer.deserialize_completion_items",
            return_value={"key-1": obj},
        ), patch(
            "src.orchestrator.completion_serializer.serialize_completion_items",
            side_effect=lambda x: x,
        ), patch(
            "src.orchestrator.observability.emit", side_effect=emitted.append
        ):
            _run([_bucket()], factory, store)

        errors = [e for e in emitted if e.get("event") == "error"]
        assert len(errors) == 1
        assert job_id in errors[0]["cause"]
        assert config_id in errors[0]["cause"]


# ---------------------------------------------------------------------------
# Property 15: No log entry emitted by the Completion_Tracker ever contains
# the raw object key
# Feature: source-status-completion-tracking, Property 15: No log entry emitted by the Completion_Tracker ever contains the raw object key
# Validates: Requirements 7.3
# ---------------------------------------------------------------------------


class TestProperty15NoRawObjectKeyInLogs:
    """# Feature: source-status-completion-tracking, Property 15: No log entry emitted by the Completion_Tracker ever contains the raw object key

    Validates: Requirements 7.3
    """

    # Object keys are generated with a distinctive prefix and enough entropy
    # that they cannot coincidentally collide with unrelated numeric/textual
    # content already present in a log entry (e.g. the ISO timestamp).
    _OBJECT_KEY_KEYWORD_STRATEGY = st.text(
        alphabet=st.characters(min_codepoint=0x21, max_codepoint=0x7E, blacklist_characters="\\'\""),
        min_size=8,
        max_size=40,
    ).map(lambda s: f"OBJKEY-MARKER-{s}")

    @given(object_key=_OBJECT_KEY_KEYWORD_STRATEGY)
    @settings(max_examples=100)
    def test_header_absent_error_never_contains_object_key(self, object_key: str) -> None:
        """# Feature: source-status-completion-tracking, Property 15: No log entry emitted by the Completion_Tracker ever contains the raw object key"""
        obj = _obj(object_key=object_key, configs={"cfg-1": _config_context(job_id="job-x")})
        eligible = {_SOURCE_BUCKET: {f"{object_key}\x00v1": obj}}
        store = _make_store(eligible_items_by_bucket=eligible)
        factory = _make_factory()

        emitted: list = []

        with patch(
            "src.orchestrator.source_status_adapter.check_source_replication_status",
            return_value=SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT),
        ), patch(
            "src.orchestrator.completion_serializer.deserialize_completion_items",
            return_value={f"{object_key}\x00v1": obj},
        ), patch(
            "src.orchestrator.completion_serializer.serialize_completion_items",
            side_effect=lambda x: x,
        ), patch(
            "src.orchestrator.observability.emit", side_effect=emitted.append
        ):
            _run([_bucket()], factory, store)

        for entry in emitted:
            serialized = str(entry)
            assert object_key not in serialized

    @given(object_key=_OBJECT_KEY_KEYWORD_STRATEGY)
    @settings(max_examples=100)
    def test_check_failed_error_never_contains_object_key(self, object_key: str) -> None:
        """# Feature: source-status-completion-tracking, Property 15: No log entry emitted by the Completion_Tracker ever contains the raw object key"""
        obj = _obj(object_key=object_key, configs={"cfg-1": _config_context(job_id="job-x")})
        eligible = {_SOURCE_BUCKET: {f"{object_key}\x00v1": obj}}
        store = _make_store(eligible_items_by_bucket=eligible)
        factory = _make_factory()

        emitted: list = []

        with patch(
            "src.orchestrator.source_status_adapter.check_source_replication_status",
            return_value=SourceStatusResult(kind=SourceStatusCheckKind.CHECK_FAILED, error_reason="throttled"),
        ), patch(
            "src.orchestrator.completion_serializer.deserialize_completion_items",
            return_value={f"{object_key}\x00v1": obj},
        ), patch(
            "src.orchestrator.completion_serializer.serialize_completion_items",
            side_effect=lambda x: x,
        ), patch(
            "src.orchestrator.observability.emit", side_effect=emitted.append
        ):
            _run([_bucket()], factory, store)

        for entry in emitted:
            serialized = str(entry)
            assert object_key not in serialized


# ---------------------------------------------------------------------------
# Task 18.8: unit test for completion-tracking call ordering
# Requirements: 6.3
# ---------------------------------------------------------------------------


class TestCallOrdering:
    def test_runs_after_checkpoint_lease_and_metrics_publish(self):
        """_run_completion_tracking_interval runs only after the per-bucket
        checkpoint/lease work and the metrics-publish call (Requirement
        6.3)."""
        from src.adapters.batch_operations_adapter import SubmissionResult
        from src.adapters.inventory_manifest_writer import WrittenManifest
        from src.core.models import (
            DerivedReplicationRule,
            DestinationRef,
            S3Location,
            SubmissionStatus,
            TaggingOperation,
        )
        from src.orchestrator import run_interval

        _role_arn = "arn:aws:iam::123456789012:role/rep-role"
        _dest_arn = "arn:aws:s3:::dest-bucket"

        def _config(bucket_names: list[str]) -> dict:
            return {"buckets": [{"name": n, "region": "us-east-1"} for n in bucket_names]}

        def _rule(source_bucket: str, rule_id: str = "rule-1") -> DerivedReplicationRule:
            return DerivedReplicationRule(
                source_bucket=source_bucket,
                replication_config_id=rule_id,
                rule_id=rule_id,
                tag_filter={"env": "prod"},
                destination=DestinationRef(bucket_arn=_dest_arn),
                replication_role_arn=_role_arn,
            )

        def _op(source_bucket: str, seq: str = "seq-001") -> TaggingOperation:
            return TaggingOperation(
                source_bucket=source_bucket,
                object_key="path/obj.txt",
                resulting_tag_set={"env": "prod"},
                sequence_number=seq,
                operation="PutObjectTagging",
                event_time=_NOW,
            )

        def _written_manifest(config_id: str = "rule-1") -> WrittenManifest:
            return WrittenManifest(
                s3_location=S3Location(bucket=_STATE_BUCKET, key=f"manifests/{config_id}/ts.csv"),
                etag="abc123",
                object_count=1,
            )

        def _submitted(config_id: str = "rule-1") -> SubmissionResult:
            return SubmissionResult(
                status=SubmissionStatus.SUBMITTED,
                config_id=config_id,
                object_count=1,
                job_id="job-abc-123",
            )

        _base_runtime = {
            "state_bucket": _STATE_BUCKET,
            "athena_workgroup": "primary",
            "athena_output_location": f"s3://{_STATE_BUCKET}/athena/",
            "account_id": "123456789012",
            "region": "us-east-1",
        }
        _ct_topic_arn = "arn:aws:sns:us-east-1:123456789012:completion-topic"

        call_order: list = []

        mock_factory_cls = MagicMock()
        mock_factory = MagicMock()
        mock_factory_cls.return_value = mock_factory

        mock_store_cls = MagicMock()
        mock_store = MagicMock()
        mock_store_cls.return_value = mock_store

        def _checkpoint(source_bucket, watermark=""):
            from src.core.models import CheckpointState
            return CheckpointState(source_bucket=source_bucket, last_processed_watermark=watermark, lease=None)

        mock_store.get_checkpoint.side_effect = (
            lambda s3_client, state_bucket, source_bucket: (_checkpoint(source_bucket), '"etag-0"')
        )
        mock_store.get_submission_records.return_value = {}

        def _release_side_effect(*args, **kwargs):
            call_order.append("release_lease")
            return '"etag-2"'

        mock_store.acquire_lease.return_value = '"etag-1"'
        mock_store.release_lease.side_effect = _release_side_effect
        mock_store.record_submission.return_value = '"etag-3"'

        def _publish_side_effect(*args, **kwargs):
            call_order.append("metrics_publish")

        mock_publish = MagicMock(side_effect=_publish_side_effect)

        def _completion_interval_side_effect(*args, **kwargs):
            call_order.append("completion_tracking_interval")

        mock_completion_interval = MagicMock(side_effect=_completion_interval_side_effect)

        rt = dict(_base_runtime)
        rt["completion_report_topic_arn"] = _ct_topic_arn

        with (
            patch("src.orchestrator.ClientFactory", mock_factory_cls),
            patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
            patch(
                "src.orchestrator.replication_config_adapter.get_replication_rules",
                return_value=([_rule("my-bucket")], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.read_journal",
                return_value=([_op("my-bucket")], []),
            ),
            patch(
                "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
                return_value=None,
            ),
            patch(
                "src.orchestrator.write_in_memory_inventory_manifest",
                return_value=_written_manifest(),
            ),
            patch(
                "src.orchestrator.batch_operations_adapter.submit_batch_job",
                return_value=_submitted(),
            ),
            patch("src.orchestrator.preflight_count", return_value=0),
            patch("src.orchestrator.read_permanent_deletes", return_value=set()),
            patch("src.orchestrator.MetricsPublisher.publish", mock_publish),
            patch("src.orchestrator._run_completion_tracking_interval", mock_completion_interval),
        ):
            run_interval(_config(["my-bucket"]), rt)

        mock_completion_interval.assert_called_once()
        assert call_order == ["release_lease", "metrics_publish", "completion_tracking_interval"]
