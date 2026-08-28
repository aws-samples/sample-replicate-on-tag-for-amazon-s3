"""Tests for detection and escalation of an absent S3 Metadata journal.

A source bucket whose journal is not enabled fails its Athena read on every
interval. That failure is isolated per bucket, so the run still succeeds, no
Lambda ``Errors`` metric is raised, and ``ReplicationLambdaErrorAlarm`` never
fires. Before the behavior covered here, the bucket replicated nothing and said
nothing beyond one log line.

Covers:
  - Classification of the Athena failure as journal-unavailable, and the
    deliberate non-classification of everything else.
  - Rate limiting, so an unmet prerequisite notifies daily rather than every
    interval, and self-clears without a write on the healthy path.
  - Escalation wiring: the audit event fires unconditionally, the callback
    fires only when classified, and the bucket is never disabled.
  - The alert publisher's log-always / SNS-conditional split.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, UTC
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from src.adapters.athena_journal_adapter import (
    JournalReadError,
    _is_journal_unavailable_reason,
    read_journal,
)
from src.adapters.state_store import StateStore
from src.core.checkpoint_serializer import serialize
from src.core.models import CheckpointState
from src.lambda_handler import _publish_journal_unavailable_alert
from src.orchestrator import JOURNAL_UNAVAILABLE_REALERT_INTERVAL, run_interval
from tests.support import mock_state_store

_ACCOUNT = "123456789012"
_STATE_BUCKET = "state-bucket"
_SRC_BUCKET = "source-bucket"
_BATCHOPS_ROLE_ARN = f"arn:aws:iam::{_ACCOUNT}:role/s3rot-batch-operations-role"
_CONFIG = {"buckets": [{"name": _SRC_BUCKET, "region": "us-west-2"}]}
_WORKGROUP = "primary"
_OUTPUT = "s3://state-bucket/athena/"
_EXEC_ID = "qeid-1"
_ETAG = '"etag-0"'
_NEW_ETAG = '"etag-1"'
_NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Classification (src/adapters/athena_journal_adapter.py)
# ---------------------------------------------------------------------------


class TestJournalUnavailableClassification:
    """Which Athena failure reasons mean "the journal does not exist"."""

    def test_athena_missing_schema_wording_is_classified(self):
        """The wording Athena actually emits for a missing namespace."""
        reason = (
            "SCHEMA_NOT_FOUND: line 1:15: Schema 'b_source-bucket' "
            "does not exist"
        )
        assert _is_journal_unavailable_reason(reason) is True

    def test_athena_missing_table_wording_is_classified(self):
        reason = "TABLE_NOT_FOUND: line 1:15: Table 'journal' does not exist"
        assert _is_journal_unavailable_reason(reason) is True

    def test_glue_entity_not_found_is_classified(self):
        """A missing s3tablescatalog surfaces through Glue, not Athena's parser."""
        reason = "EntityNotFoundException: Catalog s3tablescatalog not found"
        assert _is_journal_unavailable_reason(reason) is True

    def test_classification_is_case_insensitive(self):
        assert _is_journal_unavailable_reason("table_not_found: nope") is True
        assert _is_journal_unavailable_reason("TABLE_NOT_FOUND: nope") is True

    def test_transient_failure_is_not_classified(self):
        """A throttle or an internal error must not be reported as a missing
        prerequisite: it resolves on its own, and naming the wrong cause sends
        the operator to enable something that already exists."""
        for reason in (
            "ThrottlingException: Rate exceeded",
            "INTERNAL_ERROR_QUERY_ENGINE",
            "Query exhausted resources at this scale factor",
            "boom",
            "",
        ):
            assert _is_journal_unavailable_reason(reason) is False, reason

    def test_access_denied_is_not_classified(self):
        """Deliberately excluded. A Lake Formation account missing a grant also
        cannot read the journal, but its remedy is a grant, so it must not be
        reported as a journal that needs enabling."""
        for reason in (
            "AccessDeniedException: not authorized to perform glue:GetTable",
            "Insufficient Lake Formation permission(s) on journal",
        ):
            assert _is_journal_unavailable_reason(reason) is False, reason


def _athena_client(
    state: str = "SUCCEEDED",
    failure_reason: str = "",
    start_raises: ClientError | None = None,
) -> MagicMock:
    client = MagicMock()
    if start_raises is not None:
        client.start_query_execution.side_effect = start_raises
    else:
        client.start_query_execution.return_value = {
            "QueryExecutionId": _EXEC_ID
        }
    status: dict = {"State": state}
    if failure_reason:
        status["StateChangeReason"] = failure_reason
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": status}
    }
    return client


class TestReadJournalSetsTheFlag:
    """read_journal surfaces the classification on the fatal error it returns."""

    def test_failed_state_with_missing_schema_sets_flag(self):
        client = _athena_client(
            state="FAILED",
            failure_reason="SCHEMA_NOT_FOUND: Schema 'b_x' does not exist",
        )
        ops, errors = read_journal(client, _SRC_BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True
        assert errors[0].is_journal_unavailable is True

    def test_failed_state_with_transient_reason_leaves_flag_unset(self):
        client = _athena_client(
            state="FAILED", failure_reason="ThrottlingException: Rate exceeded"
        )
        _, errors = read_journal(client, _SRC_BUCKET, _WORKGROUP, _OUTPUT)
        assert errors[0].is_fatal is True
        assert errors[0].is_journal_unavailable is False

    def test_start_query_error_naming_a_missing_schema_sets_flag(self):
        """A missing namespace can be rejected at submission rather than
        reaching a FAILED terminal state, so that branch classifies too."""
        exc = ClientError(
            {
                "Error": {
                    "Code": "InvalidRequestException",
                    "Message": "Schema 'b_source-bucket' does not exist",
                }
            },
            "StartQueryExecution",
        )
        client = _athena_client(start_raises=exc)
        _, errors = read_journal(client, _SRC_BUCKET, _WORKGROUP, _OUTPUT)
        assert errors[0].is_fatal is True
        assert errors[0].is_journal_unavailable is True

    def test_flag_defaults_false(self):
        """Per-record errors and any error built without the keyword are
        unclassified, so nothing accidentally escalates."""
        err = JournalReadError(bucket=_SRC_BUCKET, cause="x", is_fatal=False)
        assert err.is_journal_unavailable is False


# ---------------------------------------------------------------------------
# Rate limiting (src/adapters/state_store.py)
# ---------------------------------------------------------------------------


def _s3_with_payload(payload: dict | None, etag: str = _ETAG) -> MagicMock:
    """Return a mock S3 client whose get_object yields *payload*."""
    client = MagicMock()
    if payload is None:
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
        )
    else:
        body = MagicMock()
        body.read.return_value = json.dumps(payload).encode("utf-8")
        client.get_object.return_value = {"Body": body, "ETag": etag}
    client.put_object.return_value = {"ETag": _NEW_ETAG}
    return client


def _base_payload(alerts: dict | None = None) -> dict:
    payload = json.loads(serialize(CheckpointState(
        source_bucket=_SRC_BUCKET,
        last_processed_watermark="2026-01-01T00:00:00.000000Z",
    )))
    if alerts is not None:
        payload["journal_unavailable_alerts"] = alerts
    return payload


def _due(client: MagicMock, now: datetime) -> bool:
    """Check using the interval the orchestrator really passes, so these tests
    keep testing the shipped behavior if that constant is retuned."""
    return StateStore().journal_unavailable_alert_due(
        client, _STATE_BUCKET, _SRC_BUCKET, _SRC_BUCKET,
        now=now,
        min_interval=JOURNAL_UNAVAILABLE_REALERT_INTERVAL,
    )


def _record(client: MagicMock, now: datetime) -> str:
    return StateStore().record_journal_unavailable_alert(
        client, _STATE_BUCKET, _SRC_BUCKET, _SRC_BUCKET,
        now=now, current_etag=_ETAG,
    )


# Offsets expressed relative to the interval rather than as fixed hours, for
# the same reason.
_WITHIN_INTERVAL = JOURNAL_UNAVAILABLE_REALERT_INTERVAL / 2
_PAST_INTERVAL = JOURNAL_UNAVAILABLE_REALERT_INTERVAL + timedelta(minutes=1)


class TestJournalUnavailableAlertDue:
    def test_no_recorded_alert_is_due_without_writing(self):
        client = _s3_with_payload(_base_payload())
        assert _due(client, _NOW) is True
        client.put_object.assert_not_called()

    def test_due_with_no_state_object(self):
        """A bucket failing its very first read has no state object yet."""
        client = _s3_with_payload(None)
        assert _due(client, _NOW) is True

    def test_inside_the_interval_is_not_due(self):
        """This is what stops one email per interval, indefinitely."""
        client = _s3_with_payload(
            _base_payload({_SRC_BUCKET: _NOW.isoformat()})
        )
        assert _due(client, _NOW + _WITHIN_INTERVAL) is False
        client.put_object.assert_not_called()

    def test_due_again_after_the_interval_elapses(self):
        """An unmet prerequisite keeps reminding rather than going quiet."""
        client = _s3_with_payload(
            _base_payload({_SRC_BUCKET: _NOW.isoformat()})
        )
        assert _due(client, _NOW + _PAST_INTERVAL) is True

    def test_a_different_bucket_is_due_independently(self):
        client = _s3_with_payload(
            _base_payload({"other-bucket": _NOW.isoformat()})
        )
        assert _due(client, _NOW) is True

    def test_unparseable_timestamp_is_treated_as_absent(self):
        """Fails toward an extra alert, not toward silence forever."""
        client = _s3_with_payload(_base_payload({_SRC_BUCKET: "not-a-date"}))
        assert _due(client, _NOW) is True

    def test_naive_timestamp_does_not_raise(self):
        """A hand-edited value without a timezone must not crash the alert
        path, since losing the alert is the failure this code prevents."""
        client = _s3_with_payload(
            _base_payload({_SRC_BUCKET: "2026-03-01T12:00:00"})
        )
        assert _due(client, _NOW + _WITHIN_INTERVAL) is False


class TestRecordJournalUnavailableAlert:
    def test_records_the_time_and_returns_the_new_etag(self):
        client = _s3_with_payload(_base_payload())
        assert _record(client, _NOW) == _NEW_ETAG

        written = json.loads(
            client.put_object.call_args.kwargs["Body"].decode("utf-8")
        )
        assert written["journal_unavailable_alerts"] == {
            _SRC_BUCKET: _NOW.isoformat()
        }

    def test_records_against_a_missing_state_object(self):
        client = _s3_with_payload(None)
        assert _record(client, _NOW) == _NEW_ETAG

    def test_recording_suppresses_the_next_check(self):
        """The two halves compose: what record writes is what due reads."""
        client = _s3_with_payload(_base_payload())
        _record(client, _NOW)
        written = json.loads(
            client.put_object.call_args.kwargs["Body"].decode("utf-8")
        )
        assert _due(_s3_with_payload(written), _NOW + _WITHIN_INTERVAL) is False


# ---------------------------------------------------------------------------
# Escalation wiring (src/orchestrator.py)
# ---------------------------------------------------------------------------


def _run_with_journal_errors(
    journal_errors: list[JournalReadError],
    alert_due: bool = True,
    due_error: Exception | None = None,
    record_error: Exception | None = None,
    on_journal_unavailable=None,
    on_bucket_disabled=None,
):
    """Drive run_interval to the journal read and stop there.

    The bucket is abandoned on a fatal journal error, so nothing past the read
    needs mocking.

    Returns (emitted_log_entries, mock_store).
    """
    from src.core.models import DerivedReplicationRule, DestinationRef

    rule = DerivedReplicationRule(
        source_bucket=_SRC_BUCKET,
        replication_config_id="rule-1",
        rule_id="r1",
        tag_filter={"repl": "true"},
        destination=DestinationRef(bucket_arn="arn:aws:s3:::dest"),
    )

    mock_store = mock_state_store()
    mock_store.get_checkpoint.return_value = (
        CheckpointState(
            source_bucket=_SRC_BUCKET,
            last_processed_watermark="2026-01-01T00:00:00.000000Z",
        ),
        _ETAG,
    )
    mock_store.get_submission_records.return_value = {}
    mock_store.journal_unavailable_alert_due.return_value = alert_due
    mock_store.record_journal_unavailable_alert.return_value = _NEW_ETAG
    if due_error is not None:
        mock_store.journal_unavailable_alert_due.side_effect = due_error
    if record_error is not None:
        mock_store.record_journal_unavailable_alert.side_effect = record_error

    runtime_config = {
        "state_bucket": _STATE_BUCKET,
        "athena_workgroup": _WORKGROUP,
        "athena_output_location": _OUTPUT,
        "account_id": _ACCOUNT,
        "batch_operations_role_arn": _BATCHOPS_ROLE_ARN,
        "region": "us-west-2",
    }
    if on_journal_unavailable is not None:
        runtime_config["on_journal_unavailable"] = on_journal_unavailable
    if on_bucket_disabled is not None:
        runtime_config["on_bucket_disabled"] = on_bucket_disabled

    emitted: list = []
    with patch("src.orchestrator.ClientFactory", return_value=MagicMock()), \
         patch(
             "src.orchestrator.replication_config_adapter.get_replication_rules",
             return_value=([rule], []),
         ), \
         patch(
             "src.orchestrator.state_store_module.StateStore",
             return_value=mock_store,
         ), \
         patch(
             "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
             return_value=None,
         ), \
         patch(
             "src.orchestrator.athena_journal_adapter.read_journal",
             return_value=([], journal_errors),
         ), \
         patch("src.orchestrator.MetricsPublisher"), \
         patch(
             "src.orchestrator.observability.emit",
             side_effect=lambda entry: emitted.append(entry),
         ):
        run_interval(_CONFIG, runtime_config)

    return emitted, mock_store


def _unavailable_error() -> JournalReadError:
    return JournalReadError(
        bucket=_SRC_BUCKET,
        cause="SCHEMA_NOT_FOUND: Schema 'b_source-bucket' does not exist",
        is_fatal=True,
        is_journal_unavailable=True,
    )


def _generic_fatal_error() -> JournalReadError:
    return JournalReadError(
        bucket=_SRC_BUCKET,
        cause="ThrottlingException: Rate exceeded",
        is_fatal=True,
    )


def _audit_actions(emitted: list) -> list[str]:
    return [
        e.get("action") for e in emitted
        if isinstance(e, dict) and e.get("action")
    ]


class TestJournalUnavailableEscalation:
    def test_callback_fires_with_bucket_and_cause(self):
        alerts: list[tuple[str, str]] = []
        _run_with_journal_errors(
            [_unavailable_error()],
            on_journal_unavailable=lambda b, c: alerts.append((b, c)),
        )
        assert len(alerts) == 1
        bucket, cause = alerts[0]
        assert bucket == _SRC_BUCKET
        assert "does not exist" in cause

    def test_audit_event_is_emitted(self):
        emitted, _ = _run_with_journal_errors(
            [_unavailable_error()], on_journal_unavailable=lambda b, c: None,
        )
        assert "journal_unavailable" in _audit_actions(emitted)

    def test_audit_event_is_emitted_without_any_alert_destination(self):
        """The queryable record does not depend on AlarmEmail being set, which
        is what keeps the condition from being wholly silent."""
        emitted, mock_store = _run_with_journal_errors([_unavailable_error()])
        assert "journal_unavailable" in _audit_actions(emitted)
        mock_store.journal_unavailable_alert_due.assert_not_called()
        mock_store.record_journal_unavailable_alert.assert_not_called()

    def test_alert_suppressed_when_it_is_not_due(self):
        alerts: list = []
        emitted, mock_store = _run_with_journal_errors(
            [_unavailable_error()],
            alert_due=False,
            on_journal_unavailable=lambda b, c: alerts.append(b),
        )
        assert alerts == []
        mock_store.record_journal_unavailable_alert.assert_not_called()
        # Still logged every interval, only the notification is rate-limited.
        assert "journal_unavailable" in _audit_actions(emitted)

    def test_bucket_is_never_disabled(self):
        """The remedy is in the operator's account and the bucket resumes on
        its own, so disabling would only add a config edit to recovery."""
        disabled: list = []
        _run_with_journal_errors(
            [_unavailable_error()],
            on_journal_unavailable=lambda b, c: None,
            on_bucket_disabled=lambda b, r: disabled.append(b),
        )
        assert disabled == []

    def test_generic_fatal_error_does_not_escalate(self):
        alerts: list = []
        emitted, mock_store = _run_with_journal_errors(
            [_generic_fatal_error()],
            on_journal_unavailable=lambda b, c: alerts.append(b),
        )
        assert alerts == []
        assert "journal_unavailable" not in _audit_actions(emitted)
        mock_store.journal_unavailable_alert_due.assert_not_called()

    def test_alert_still_sent_when_the_due_check_fails(self):
        """Losing the notification is worse than sending a duplicate."""
        alerts: list = []
        _run_with_journal_errors(
            [_unavailable_error()],
            due_error=RuntimeError("state read failed"),
            on_journal_unavailable=lambda b, c: alerts.append(b),
        )
        assert alerts == [_SRC_BUCKET]

    def test_suppression_is_recorded_after_a_delivered_alert(self):
        _, mock_store = _run_with_journal_errors(
            [_unavailable_error()], on_journal_unavailable=lambda b, c: None,
        )
        mock_store.record_journal_unavailable_alert.assert_called_once()
        assert (
            mock_store.record_journal_unavailable_alert.call_args.args[3]
            == _SRC_BUCKET
        )

    def test_a_failed_publish_leaves_suppression_unset(self):
        """The point of the ordering. A publish that raises must not spend the
        interval, or a stalled bucket goes unannounced for a whole interval
        while the condition recurs every run."""
        def boom(bucket, cause):
            raise RuntimeError("SNS down")

        _, mock_store = _run_with_journal_errors(
            [_unavailable_error()], on_journal_unavailable=boom,
        )
        mock_store.record_journal_unavailable_alert.assert_not_called()

    def test_a_failed_suppression_write_does_not_break_the_run(self):
        """It costs one duplicate alert on the next interval, which is the
        direction this path already fails in."""
        alerts: list = []
        _run_with_journal_errors(
            [_unavailable_error()],
            record_error=RuntimeError("state write failed"),
            on_journal_unavailable=lambda b, c: alerts.append(b),
        )
        assert alerts == [_SRC_BUCKET]

    def test_callback_exception_does_not_break_the_run(self):
        def boom(bucket, cause):
            raise RuntimeError("SNS down")

        # Completes without raising; the run itself must survive a failed alert.
        _run_with_journal_errors(
            [_unavailable_error()], on_journal_unavailable=boom,
        )

    def test_healthy_read_never_touches_the_alert_record(self):
        """Guards the ETag chain. Any write on the healthy path would thread
        itself into the per-bucket conditional-write sequence, which
        TestIdleRunScanRecording::test_idle_run_passes_the_live_etag exists to
        protect.
        """
        _, mock_store = _run_with_journal_errors([])
        mock_store.journal_unavailable_alert_due.assert_not_called()
        mock_store.record_journal_unavailable_alert.assert_not_called()


# ---------------------------------------------------------------------------
# Alert publisher (src/lambda_handler.py)
# ---------------------------------------------------------------------------


class TestPublishJournalUnavailableAlert:
    def _publish(self, topic_arn: str | None):
        sns, logs = MagicMock(), MagicMock()
        _publish_journal_unavailable_alert(
            sns_client=sns,
            logs_client=logs,
            topic_arn=topic_arn,
            log_group_name="batch-job-failures",
            bucket_name=_SRC_BUCKET,
            cause="SCHEMA_NOT_FOUND: Schema 'b_source-bucket' does not exist",
            now=_NOW,
        )
        return sns, logs

    def test_logs_even_without_a_topic(self):
        sns, logs = self._publish(None)
        logs.put_log_events.assert_called_once()
        sns.publish.assert_not_called()

    def test_log_entry_is_structured_and_names_the_event(self):
        _, logs = self._publish(None)
        message = json.loads(
            logs.put_log_events.call_args.kwargs["logEvents"][0]["message"]
        )
        assert message["event"] == "journal_unavailable"
        assert message["source_bucket"] == _SRC_BUCKET
        assert "does not exist" in message["cause"]
        assert message["recovery"]

    def test_log_stream_is_named_for_this_alert_kind(self):
        _, logs = self._publish(None)
        stream = logs.put_log_events.call_args.kwargs["logStreamName"]
        assert stream.startswith("journal-unavailable-")

    def test_publishes_to_sns_when_a_topic_is_configured(self):
        sns, _ = self._publish("arn:aws:sns:us-west-2:123456789012:alerts")
        sns.publish.assert_called_once()
        kwargs = sns.publish.call_args.kwargs
        assert _SRC_BUCKET in kwargs["Subject"]
        assert len(kwargs["Subject"]) < 100

    def test_email_body_names_the_remedy_and_says_nothing_is_disabled(self):
        sns, _ = self._publish("arn:aws:sns:us-west-2:123456789012:alerts")
        body = sns.publish.call_args.kwargs["Message"]
        assert "Metadata configuration" in body
        assert "s3tablescatalog" in body
        assert "has not been disabled" in body
