"""Mocked integration tests for src/adapters/state_store.py — task 13.2.

Covers Requirements 7.4, 9.4:
- S3 conditional-write (If-Match / If-None-Match) compare-and-set semantics.
- A write with a stale ETag fails the precondition (ConditionalWriteError).
- Checkpoint advances only via an ETag-guarded conditional write.
- Submission record persisted into the same state object (7.4).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from hypothesis import given, settings
from hypothesis import strategies as st

from src.adapters.bops_report_reader import BopsCompletionReport
from src.adapters.state_store import (
    COMPLETION_SIDE_MAP_CEILING as CEILING,
)
from src.adapters.state_store import ConditionalWriteError, StateStore
from src.core.checkpoint_serializer import deserialize, serialize
from src.core.completion_serializer import item_key as _item_key_fn
from src.core.completion_serializer import serialize_completion_items
from src.core.observability import redact_object_key
from src.core.models import (
    CheckpointState,
    CompletionState,
    ConfigContext,
    Lease,
    LeaseStatus,
    ManifestEntry,
    ProcessedRef,
    ScanState,
    SubmissionRecord,
    SubmissionStatus,
    TrackedObject,
)

_NOW = datetime(2024, 1, 1, tzinfo=timezone.utc)
_LOOKBACK = timedelta(minutes=10)
_STATE_BUCKET = "state-bucket"
_SRC_BUCKET = "my-source-bucket"
_STATE_KEY = f"state/{_SRC_BUCKET}.json"
_ETAG = '"abc123"'
_NEW_ETAG = '"def456"'

_WM_050 = "2024-01-01T00:00:50.000000Z"
_WM_099 = "2024-01-01T00:01:39.000000Z"
_WM_042 = "2024-01-01T00:00:42.000000Z"
_WM_077 = "2024-01-01T00:01:17.000000Z"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_state(watermark: str = _WM_050, lease: Lease | None = None) -> CheckpointState:
    return CheckpointState(
        source_bucket=_SRC_BUCKET,
        last_processed_watermark=watermark,
        lease=lease,
    )


def _make_lease(candidate_max: str = _WM_099) -> Lease:
    return Lease(
        lease_id="lease-1",
        candidate_max_watermark=candidate_max,
        acquired_at=_NOW,
        status=LeaseStatus.IN_FLIGHT,
    )


def _mock_s3_get(state: CheckpointState, etag: str = _ETAG) -> MagicMock:
    """Return a mock whose get_object returns the given state."""
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = serialize(state).encode("utf-8")
    client.get_object.return_value = {"Body": body, "ETag": etag}
    client.put_object.return_value = {"ETag": _NEW_ETAG}
    return client


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "err"}}, "PutObject")


# ---------------------------------------------------------------------------
# get_checkpoint — read path
# ---------------------------------------------------------------------------


class TestGetCheckpoint:
    def test_returns_state_and_etag_on_success(self):
        state = _make_state(watermark=_WM_042)
        client = _mock_s3_get(state, etag=_ETAG)
        store = StateStore()
        restored, etag = store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        assert restored.last_processed_watermark == _WM_042
        assert etag == _ETAG

    def test_calls_get_object_with_correct_key(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        client.get_object.assert_called_once_with(
            Bucket=_STATE_BUCKET, Key=_STATE_KEY
        )

    def test_returns_default_state_with_none_etag_when_no_key(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        store = StateStore()
        state, etag = store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        assert state.last_processed_watermark == ""
        assert state.source_bucket == _SRC_BUCKET
        assert etag is None

    def test_reraises_other_client_errors(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("AccessDenied")
        store = StateStore()
        with pytest.raises(ClientError):
            store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)


# ---------------------------------------------------------------------------
# put_checkpoint — conditional write semantics (Req 9.4)
# ---------------------------------------------------------------------------


def _mock_s3_no_prior_object() -> MagicMock:
    """Client whose get_object raises NoSuchKey (no state object exists yet).

    put_checkpoint reads the current object before writing (to preserve
    sibling keys); tests that don't care about that merge use this helper
    so the read path is a no-op first-write.
    """
    client = MagicMock()
    client.get_object.side_effect = _client_error("NoSuchKey")
    return client


class TestPutCheckpoint:
    def test_uses_if_match_when_etag_provided(self):
        """Existing object → PutObject with IfMatch (Req 9.4)."""
        state = _make_state()
        client = _mock_s3_no_prior_object()
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        new_etag = store.put_checkpoint(client, _STATE_BUCKET, state, expected_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        assert "IfMatch" in kwargs
        assert kwargs["IfMatch"] == _ETAG
        assert "IfNoneMatch" not in kwargs
        assert new_etag == _NEW_ETAG

    def test_uses_if_none_match_when_no_etag(self):
        """First write → PutObject with IfNoneMatch: * (Req 9.4)."""
        state = _make_state()
        client = _mock_s3_no_prior_object()
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        store.put_checkpoint(client, _STATE_BUCKET, state, expected_etag=None)
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfNoneMatch") == "*"
        assert "IfMatch" not in kwargs

    def test_stale_etag_raises_conditional_write_error(self):
        """PreconditionFailed → ConditionalWriteError (Req 9.4)."""
        state = _make_state()
        client = _mock_s3_no_prior_object()
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.put_checkpoint(client, _STATE_BUCKET, state, expected_etag=_ETAG)

    def test_conditional_request_conflict_raises_conditional_write_error(self):
        """ConditionalRequestConflict also maps to ConditionalWriteError."""
        state = _make_state()
        client = _mock_s3_no_prior_object()
        client.put_object.side_effect = _client_error("ConditionalRequestConflict")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.put_checkpoint(client, _STATE_BUCKET, state, expected_etag=_ETAG)

    def test_other_client_errors_propagate(self):
        """Non-precondition errors are re-raised (not converted)."""
        state = _make_state()
        client = _mock_s3_no_prior_object()
        client.put_object.side_effect = _client_error("AccessDenied")
        store = StateStore()
        with pytest.raises(ClientError):
            store.put_checkpoint(client, _STATE_BUCKET, state, expected_etag=_ETAG)

    def test_checkpoint_state_serialized_in_body(self):
        """PutObject body contains the serialized checkpoint."""
        state = _make_state(watermark=_WM_077)
        client = _mock_s3_no_prior_object()
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        store.put_checkpoint(client, _STATE_BUCKET, state, expected_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_077
        assert body["source_bucket"] == _SRC_BUCKET


# ---------------------------------------------------------------------------
# acquire_lease / release_lease — atomic ETag-guarded updates (Req 9.4)
# ---------------------------------------------------------------------------


class TestAcquireLease:
    def test_acquire_lease_returns_new_etag(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        new_etag = store.acquire_lease(
            client, _STATE_BUCKET, _SRC_BUCKET, _make_lease(), current_etag=_ETAG
        )
        assert new_etag == _NEW_ETAG

    def test_acquire_lease_embeds_lease_in_written_state(self):
        """The PUT body contains the lease (Req 9.4)."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        lease = _make_lease(candidate_max=_WM_099)
        store.acquire_lease(
            client, _STATE_BUCKET, _SRC_BUCKET, lease, current_etag=_ETAG
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["lease"] is not None
        assert body["lease"]["candidate_max_watermark"] == _WM_099

    def test_acquire_lease_stale_etag_raises(self):
        state = _make_state()
        client = _mock_s3_get(state)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.acquire_lease(
                client, _STATE_BUCKET, _SRC_BUCKET, _make_lease(), current_etag="stale"
            )


class TestReleaseLease:
    def test_release_with_refs_advances_checkpoint(self):
        """Successful submission → watermark advanced to HWM (Req 9.1)."""
        state = _make_state(watermark=_WM_050, lease=_make_lease())
        client = _mock_s3_get(state)
        store = StateStore()
        store.release_lease(
            client, _STATE_BUCKET, _SRC_BUCKET,
            submitted_refs=[ProcessedRef(logical_operation_id="op", watermark=_WM_099)],
            lookback=_LOOKBACK,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_099
        assert body["lease"] is None

    def test_release_with_no_refs_leaves_checkpoint_unchanged(self):
        """Failed submission → watermark unchanged, lease cleared (Req 9.3)."""
        state = _make_state(watermark=_WM_050, lease=_make_lease())
        client = _mock_s3_get(state)
        store = StateStore()
        store.release_lease(
            client, _STATE_BUCKET, _SRC_BUCKET,
            submitted_refs=None,
            lookback=_LOOKBACK,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_050
        assert body["lease"] is None

    def test_release_stale_etag_raises(self):
        state = _make_state(lease=_make_lease())
        client = _mock_s3_get(state)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.release_lease(
                client, _STATE_BUCKET, _SRC_BUCKET,
                submitted_refs=[ProcessedRef(logical_operation_id="op", watermark=_WM_099)],
                lookback=_LOOKBACK,
                current_etag="stale",
            )


# ---------------------------------------------------------------------------
# put_checkpoint / acquire_lease / release_lease — sibling-key preservation
# regression (code-review-remediation spec, Req 1)
# ---------------------------------------------------------------------------


def _mock_s3_from_raw_payload(payload: dict, etag: str = _ETAG) -> MagicMock:
    """Return a mock S3 client whose store starts with the given raw JSON
    payload and tracks subsequent conditional PutObject calls so the
    resulting object can be re-read.
    """
    client = MagicMock()
    state_holder = {"body": json.dumps(payload).encode("utf-8"), "etag": etag}

    def _get_object(Bucket, Key):
        body = MagicMock()
        body.read.return_value = state_holder["body"]
        return {"Body": body, "ETag": state_holder["etag"]}

    def _put_object(Bucket, Key, Body, **kwargs):
        state_holder["body"] = Body
        state_holder["etag"] = _NEW_ETAG
        return {"ETag": _NEW_ETAG}

    client.get_object.side_effect = _get_object
    client.put_object.side_effect = _put_object
    client._state_holder = state_holder
    return client


_SIBLING_KEYS_PAYLOAD_EXTRAS = {
    "submission_records": {
        _SRC_BUCKET: {
            "replication_config_id": _SRC_BUCKET,
            "source_bucket": _SRC_BUCKET,
            "job_id": "job-1",
            "manifest_key": "manifests/x",
            "submitted_at": _NOW.isoformat(),
            "status": "SUBMITTED",
            "watermark_low": _WM_042,
            "watermark_high": _WM_050,
            "consecutive_failures": 0,
        }
    },
    "completion_items": {
        "obj-a\u0000ver-1": {
            "source_bucket": _SRC_BUCKET,
            "object_key": "obj-a",
            "version_id": "ver-1",
            "state": "PENDING",
            "resolved_at": None,
            "resolution_method": None,
            "replication_outcome": None,
            "configs": {
                _SRC_BUCKET: {
                    "replication_config_id": _SRC_BUCKET,
                    "job_id": "job-1",
                    "manifest_generated_at": _NOW.isoformat(),
                    "bops_confirmed": True,
                }
            },
        }
    },
    "completion_processed_job_ids": ["job-1"],
    "completion_scan_state": {
        _SRC_BUCKET: {"last_scan_at": _NOW.isoformat(), "last_scan_match_count": 5},
    },
    "completion_report_alerted_configs": [_SRC_BUCKET],
}


def _seeded_payload(watermark: str = _WM_050, lease: Lease | None = None) -> dict:
    payload = json.loads(serialize(_make_state(watermark=watermark, lease=lease)))
    payload.update(_SIBLING_KEYS_PAYLOAD_EXTRAS)
    return payload


class TestPutCheckpointPreservesSiblingKeys:
    """Regression test for the checkpoint-clobber bug (code-review-remediation
    spec Req 1): put_checkpoint/acquire_lease/release_lease must not wipe
    submission_records/completion_items/completion_processed_job_ids/
    completion_scan_state/completion_report_alerted_configs when persisting
    the checkpoint or lease.
    """

    def _assert_siblings_survive(self, written_payload: dict) -> None:
        for key, expected in _SIBLING_KEYS_PAYLOAD_EXTRAS.items():
            assert key in written_payload, f"{key} missing after write"
            assert written_payload[key] == expected, f"{key} mutated unexpectedly"

    def test_put_checkpoint_preserves_sibling_keys(self):
        seeded = _seeded_payload()
        client = _mock_s3_from_raw_payload(seeded)
        store = StateStore()
        store.put_checkpoint(
            client, _STATE_BUCKET, _make_state(watermark=_WM_077), expected_etag=_ETAG
        )
        written = json.loads(client._state_holder["body"])
        assert written["last_processed_watermark"] == _WM_077
        self._assert_siblings_survive(written)

    def test_acquire_lease_preserves_sibling_keys(self):
        seeded = _seeded_payload()
        client = _mock_s3_from_raw_payload(seeded)
        store = StateStore()
        store.acquire_lease(
            client, _STATE_BUCKET, _SRC_BUCKET, _make_lease(), current_etag=_ETAG
        )
        written = json.loads(client._state_holder["body"])
        assert written["lease"] is not None
        self._assert_siblings_survive(written)

    def test_release_lease_preserves_sibling_keys(self):
        seeded = _seeded_payload(lease=_make_lease())
        client = _mock_s3_from_raw_payload(seeded)
        store = StateStore()
        store.release_lease(
            client, _STATE_BUCKET, _SRC_BUCKET,
            submitted_refs=[ProcessedRef(logical_operation_id="op", watermark=_WM_099)],
            lookback=_LOOKBACK,
            current_etag=_ETAG,
        )
        written = json.loads(client._state_holder["body"])
        assert written["last_processed_watermark"] == _WM_099
        assert written["lease"] is None
        self._assert_siblings_survive(written)

    def test_full_acquire_then_release_cycle_preserves_sibling_keys(self):
        """End-to-end: acquire_lease followed by release_lease in the same
        run (the real orchestrator sequence) must leave every sibling key
        intact after both writes.
        """
        seeded = _seeded_payload()
        client = _mock_s3_from_raw_payload(seeded)
        store = StateStore()

        etag_after_acquire = store.acquire_lease(
            client, _STATE_BUCKET, _SRC_BUCKET, _make_lease(), current_etag=_ETAG
        )
        after_acquire = json.loads(client._state_holder["body"])
        self._assert_siblings_survive(after_acquire)

        store.release_lease(
            client, _STATE_BUCKET, _SRC_BUCKET,
            submitted_refs=[ProcessedRef(logical_operation_id="op", watermark=_WM_099)],
            lookback=_LOOKBACK,
            current_etag=etag_after_acquire,
        )
        after_release = json.loads(client._state_holder["body"])
        self._assert_siblings_survive(after_release)
        assert after_release["last_processed_watermark"] == _WM_099
        assert after_release["lease"] is None

    def test_put_checkpoint_creates_object_when_missing(self):
        """First-write path (no prior object) still succeeds and needs no
        sibling keys, since none existed yet."""
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        new_etag = store.put_checkpoint(
            client, _STATE_BUCKET, _make_state(), expected_etag=None
        )
        assert new_etag == _NEW_ETAG
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["source_bucket"] == _SRC_BUCKET


# ---------------------------------------------------------------------------
# record_submission — persisted into same state object (Req 7.4)
# ---------------------------------------------------------------------------


class TestRecordSubmission:
    def _make_submission(self) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket=_SRC_BUCKET,
            job_id="job-abc-123",
            manifest_key="manifests/cfg-1/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
        )

    def test_submission_record_written_to_state_object(self):
        """Submission record embedded in the same per-bucket JSON (Req 7.4)."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_submission(client, _STATE_BUCKET, self._make_submission(), _ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        # Keyed by job_id under "submission_records", so a job submitted while an
        # earlier one is still running cannot displace it.
        assert "submission_records" in body
        assert "job-abc-123" in body["submission_records"]
        assert body["submission_records"]["job-abc-123"]["job_id"] == "job-abc-123"

    def test_submission_preserves_existing_checkpoint(self):
        """Record submission does not clobber the checkpoint (Req 7.4)."""
        state = _make_state(watermark=_WM_077)
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_submission(client, _STATE_BUCKET, self._make_submission(), _ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_077

    def test_submission_conditional_write_uses_current_etag(self):
        """The PUT uses the provided ETag guard (Req 9.4)."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_submission(client, _STATE_BUCKET, self._make_submission(), _ETAG)
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfMatch") == _ETAG

    def test_submission_stale_etag_raises(self):
        state = _make_state()
        client = _mock_s3_get(state)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.record_submission(client, _STATE_BUCKET, self._make_submission(), "stale")


# ---------------------------------------------------------------------------
# disable_bucket / get_disable_state — the per-bucket disable flag
#
# The flag lives in the state object rather than in solution-config.json, so
# that the config custom resource — which rewrites that object wholesale from
# template parameters on every stack create/update — cannot silently re-enable
# a bucket the circuit breaker disabled.
# ---------------------------------------------------------------------------


def _disabled_payload(reason: str = "circuit breaker tripped") -> dict:
    payload = json.loads(serialize(_make_state()))
    payload["disabled"] = True
    payload["disabled_reason"] = reason
    payload["disabled_at"] = _NOW.isoformat()
    return payload


class TestDisableBucket:
    def _submission_body(self, key: str = _SRC_BUCKET) -> dict:
        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = {
            key: {
                "replication_config_id": key,
                "source_bucket": _SRC_BUCKET,
                "job_id": "job-dead-123",
                "manifest_key": "manifests/x/y.json",
                "submitted_at": _NOW.isoformat(),
                "status": SubmissionStatus.SUBMITTED.value,
                "watermark_low": _WM_042,
                "watermark_high": _WM_077,
                "consecutive_failures": 4,
            }
        }
        return payload

    def _mock_s3_get_raw(self, payload: dict, etag: str = _ETAG) -> MagicMock:
        client = MagicMock()
        body = MagicMock()
        body.read.return_value = json.dumps(payload).encode("utf-8")
        client.get_object.return_value = {"Body": body, "ETag": etag}
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        return client

    def _disable(self, client, current_etag=_ETAG, reason="circuit breaker tripped"):
        return StateStore().disable_bucket(
            client, _STATE_BUCKET, _SRC_BUCKET,
            reason=reason, now=_NOW, current_etag=current_etag,
        )

    def _written(self, client) -> dict:
        return json.loads(client.put_object.call_args[1]["Body"])

    def test_writes_the_three_disable_keys(self):
        client = self._mock_s3_get_raw(self._submission_body())
        self._disable(client, reason="ceiling exceeded")
        body = self._written(client)
        assert body["disabled"] is True
        assert body["disabled_reason"] == "ceiling exceeded"
        assert body["disabled_at"] == _NOW.isoformat()

    def test_clears_submission_records_in_the_same_write(self):
        """One write does both, so the bucket can never be left disabled with a
        stale SubmissionRecord still pointing at the dead job that tripped the
        breaker — which would re-trip it on the first run after re-enabling."""
        client = self._mock_s3_get_raw(self._submission_body())
        self._disable(client)
        client.put_object.assert_called_once()
        assert self._written(client)["submission_records"] == {}

    def test_clears_legacy_singular_submission_record_field_too(self):
        payload = self._submission_body()
        payload["submission_record"] = payload["submission_records"][_SRC_BUCKET]
        client = self._mock_s3_get_raw(payload)
        self._disable(client)
        assert "submission_record" not in self._written(client)

    def test_clears_legacy_config_id_keyed_records_too(self):
        """A pre-migration object with legacy config_id-keyed entries (not the
        bucket-name sentinel) is fully cleared, not selectively drained."""
        payload = self._submission_body(key="legacy-cfg-1")
        payload["submission_records"]["legacy-cfg-2"] = dict(
            payload["submission_records"]["legacy-cfg-1"]
        )
        client = self._mock_s3_get_raw(payload)
        self._disable(client)
        assert self._written(client)["submission_records"] == {}

    def test_preserves_checkpoint_and_other_sibling_keys(self):
        payload = self._submission_body()
        payload["completion_processed_job_ids"] = ["job-1", "job-2"]
        client = self._mock_s3_get_raw(payload)
        self._disable(client)
        body = self._written(client)
        assert body["source_bucket"] == _SRC_BUCKET
        assert body["last_processed_watermark"] == _WM_050
        assert body["completion_processed_job_ids"] == ["job-1", "job-2"]

    def test_does_not_clear_the_submission_failure_streak(self):
        """The streak counts requests botocore rejects outright, which is a code
        defect rather than a transient condition, so re-enabling without a fix
        deployed should re-trip promptly instead of starting from zero."""
        payload = self._submission_body()
        payload["submission_failure_streaks"] = {_SRC_BUCKET: 4}
        client = self._mock_s3_get_raw(payload)
        self._disable(client)
        assert self._written(client)["submission_failure_streaks"] == {_SRC_BUCKET: 4}

    def test_uses_the_caller_supplied_etag_not_its_own_read(self):
        """The write is guarded by the ETag the orchestrator's StateWriter holds,
        not by the one this method's own read returned. Both call sites run
        inside that ETag chain — the submission-streak one from inside the lease
        scope — so a self-managed ETag would invalidate the writer's and make
        the release_lease in _lease_scope's finally strand the lease."""
        client = self._mock_s3_get_raw(self._submission_body(), etag='"fresh-read"')
        self._disable(client, current_etag='"held-by-writer"')
        assert client.put_object.call_args[1]["IfMatch"] == '"held-by-writer"'

    def test_returns_the_new_etag_so_the_chain_continues(self):
        client = self._mock_s3_get_raw(self._submission_body())
        assert self._disable(client) == _NEW_ETAG

    def test_stale_etag_at_write_time_raises_conditional_write_error(self):
        """The caller must be able to tell that the bucket was NOT disabled, so
        it does not announce a disable that never landed."""
        client = self._mock_s3_get_raw(self._submission_body())
        client.put_object.side_effect = _client_error("PreconditionFailed")
        with pytest.raises(ConditionalWriteError):
            self._disable(client)

    def test_missing_state_object_is_a_create(self):
        """Edge case: the bucket is disabled before its first-ever checkpoint
        write. A create-only write still records the flag."""
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        assert self._disable(client, current_etag=None) == _NEW_ETAG
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfNoneMatch") == "*"
        body = json.loads(kwargs["Body"])
        assert body["disabled"] is True
        assert body["submission_records"] == {}

    def test_survives_a_subsequent_put_checkpoint(self):
        """put_checkpoint overlays only the CheckpointState keys, so the flag
        written here is preserved by the end-of-interval release_lease. Were it
        a CheckpointState field instead, advance_checkpoint would rebuild it
        from an explicit field list and reset it every interval."""
        client = self._mock_s3_get_raw(_disabled_payload())
        StateStore().put_checkpoint(client, _STATE_BUCKET, _make_state(), _ETAG)
        body = json.loads(client.put_object.call_args[1]["Body"])
        assert body["disabled"] is True
        assert body["disabled_reason"] == "circuit breaker tripped"


class TestGetDisableState:
    def _client(self, payload: dict | None) -> MagicMock:
        client = MagicMock()
        if payload is None:
            client.get_object.side_effect = _client_error("NoSuchKey")
        else:
            body = MagicMock()
            body.read.return_value = json.dumps(payload).encode("utf-8")
            client.get_object.return_value = {"Body": body, "ETag": _ETAG}
        return client

    def _read(self, payload: dict | None):
        return StateStore().get_disable_state(
            self._client(payload), _STATE_BUCKET, _SRC_BUCKET
        )

    def test_reads_the_flag_and_its_reason(self):
        state = self._read(_disabled_payload(reason="ceiling exceeded"))
        assert state.disabled is True
        assert state.reason == "ceiling exceeded"
        assert state.at == _NOW.isoformat()

    def test_absent_state_object_is_enabled(self):
        assert self._read(None).disabled is False

    def test_absent_key_is_enabled(self):
        assert self._read(json.loads(serialize(_make_state()))).disabled is False

    def test_explicit_false_is_enabled(self):
        """The documented recovery step is setting "disabled": false, so that
        has to read back as enabled just as removing the key does."""
        payload = _disabled_payload()
        payload["disabled"] = False
        assert self._read(payload).disabled is False

    def test_readable_when_the_checkpoint_itself_does_not_parse(self):
        """The read deliberately bypasses deserialize(). A disabled bucket must
        not read back as enabled because its watermark was corrupted or
        hand-edited — that would resume billable job submission for exactly the
        bucket the breaker stopped."""
        payload = _disabled_payload()
        payload["last_processed_watermark"] = "9999-12-31T23:59:59.000000Z"
        with pytest.raises(ValueError):
            deserialize(json.dumps(payload))
        assert self._read(payload).disabled is True


# ---------------------------------------------------------------------------
# get_submission_records — read path (Task 4.6 / Requirements 4.1, 4.2)
# ---------------------------------------------------------------------------


class TestGetSubmissionRecords:
    def _make_submission(
        self,
        config_id: str = "cfg-1",
        job_id: str = "job-abc-123",
        watermark_low: str = _WM_042,
        watermark_high: str = _WM_077,
    ) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id=config_id,
            source_bucket=_SRC_BUCKET,
            job_id=job_id,
            manifest_key=f"manifests/{config_id}/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=watermark_low,
            watermark_high=watermark_high,
        )

    def _state_json_with_submission_records(self, records: dict) -> str:
        """Return a JSON state object body with submission_records dict."""
        from src.core.checkpoint_serializer import serialize, serialize_submission_record
        state = _make_state()
        payload = __import__("json").loads(serialize(state))
        payload["submission_records"] = {
            cid: serialize_submission_record(rec) for cid, rec in records.items()
        }
        return __import__("json").dumps(payload)

    def _state_json_with_legacy_submission(self, rec: SubmissionRecord) -> str:
        """Return a JSON state object with the legacy singular submission_record."""
        from src.core.checkpoint_serializer import serialize
        import json
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["submission_record"] = {
            "replication_config_id": rec.replication_config_id,
            "source_bucket": rec.source_bucket,
            "job_id": rec.job_id,
            "manifest_key": rec.manifest_key,
            "submitted_at": rec.submitted_at.isoformat(),
            "status": rec.status.value,
            # No watermark_low/high in the legacy form
        }
        return json.dumps(payload)

    def _mock_s3_get_raw(self, body_json: str, etag: str = _ETAG) -> MagicMock:
        client = MagicMock()
        body = MagicMock()
        body.read.return_value = body_json.encode("utf-8")
        client.get_object.return_value = {"Body": body, "ETag": etag}
        return client

    def test_returns_dict_keyed_by_job_id(self):
        """get_submission_records returns records keyed by their own job_id."""
        rec = self._make_submission(config_id="cfg-1")
        body = self._state_json_with_submission_records({"cfg-1": rec})
        client = self._mock_s3_get_raw(body)
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert "job-abc-123" in result
        assert result["job-abc-123"].job_id == "job-abc-123"

    def test_round_trips_watermark_fields(self):
        """watermark_low and watermark_high survive the serialize→get cycle."""
        rec = self._make_submission(
            watermark_low=_WM_042,
            watermark_high=_WM_077,
        )
        body = self._state_json_with_submission_records({"cfg-1": rec})
        client = self._mock_s3_get_raw(body)
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result["job-abc-123"].watermark_low == _WM_042
        assert result["job-abc-123"].watermark_high == _WM_077

    def test_returns_empty_dict_when_no_submission_records(self):
        """Absent submission_records → empty dict (no KeyError)."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {}

    def test_returns_empty_dict_when_state_object_absent(self):
        """NoSuchKey → empty dict (first run)."""
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {}

    def test_legacy_singular_submission_record_key_is_ignored(self):
        """Legacy singular submission_record field (without submission_records dict)
        is now ignored — returns empty dict after compatibility removal."""
        rec = self._make_submission(config_id="cfg-legacy")
        body = self._state_json_with_legacy_submission(rec)
        client = self._mock_s3_get_raw(body)
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {}

    def test_legacy_singular_record_returns_empty_dict(self):
        """Legacy records in the singular submission_record field are ignored
        post-compatibility-removal — empty dict regardless of content."""
        rec = self._make_submission(config_id="cfg-old")
        body = self._state_json_with_legacy_submission(rec)
        client = self._mock_s3_get_raw(body)
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {}

    def test_multiple_records_all_returned_keyed_by_job_id(self):
        """Every stored record is returned, re-keyed by its own job_id."""
        rec_a = self._make_submission(config_id="cfg-a", job_id="job-a")
        rec_b = self._make_submission(config_id="cfg-b", job_id="job-b")
        body = self._state_json_with_submission_records({"cfg-a": rec_a, "cfg-b": rec_b})
        client = self._mock_s3_get_raw(body)
        store = StateStore()
        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)
        assert set(result.keys()) == {"job-a", "job-b"}
        assert result["job-a"].job_id == "job-a"
        assert result["job-b"].job_id == "job-b"


# ---------------------------------------------------------------------------
# record_submission — writes submission_records dict (Task 4.6 / Req 7.4)
# ---------------------------------------------------------------------------


class TestRecordSubmissionUpdated:
    def _make_submission(self, config_id: str = "cfg-1") -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id=config_id,
            source_bucket=_SRC_BUCKET,
            job_id=f"job-{config_id}",
            manifest_key=f"manifests/{config_id}/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=_WM_042,
            watermark_high=_WM_077,
        )

    def test_writes_submission_records_dict(self):
        """record_submission writes submission_records keyed by job_id."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_submission(client, _STATE_BUCKET, self._make_submission(), _ETAG)
        kwargs = client.put_object.call_args[1]
        body = __import__("json").loads(kwargs["Body"])
        assert "submission_records" in body
        assert "job-cfg-1" in body["submission_records"]

    def test_watermark_fields_persisted(self):
        """watermark_low and watermark_high are stored on the job's record."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_submission(client, _STATE_BUCKET, self._make_submission(), _ETAG)
        kwargs = client.put_object.call_args[1]
        body = __import__("json").loads(kwargs["Body"])
        rec = body["submission_records"]["job-cfg-1"]
        assert rec["watermark_low"] == _WM_042
        assert rec["watermark_high"] == _WM_077

    def test_legacy_singular_field_removed_on_write(self):
        """Writing a new submission removes any legacy submission_record key."""
        import json
        state = _make_state()
        # Seed the mock with a state that has the old singular field.
        payload = json.loads(
            __import__("src.core.checkpoint_serializer", fromlist=["serialize"]).serialize(state)
        )
        payload["submission_record"] = {"replication_config_id": "old", "job_id": "old-job"}
        body_str = json.dumps(payload)
        body_mock = MagicMock()
        body_mock.read.return_value = body_str.encode("utf-8")
        client = MagicMock()
        client.get_object.return_value = {"Body": body_mock, "ETag": _ETAG}
        client.put_object.return_value = {"ETag": _NEW_ETAG}

        store = StateStore()
        store.record_submission(client, _STATE_BUCKET, self._make_submission(), _ETAG)
        kwargs = client.put_object.call_args[1]
        written = json.loads(kwargs["Body"])
        assert "submission_record" not in written

    def test_existing_entry_is_re_keyed_and_kept_not_dropped(self):
        """An entry already present is preserved, re-keyed by its own job_id.

        Dropping it was the defect: the discarded job's completion report was
        never read, the report-missing check could not see it, and a later failure
        left no watermark_low to roll back to (Requirement 1.1)."""
        import json
        state = _make_state()
        from src.core.checkpoint_serializer import serialize, serialize_submission_record
        payload = json.loads(serialize(state))
        existing_rec = self._make_submission("cfg-existing")
        payload["submission_records"] = {
            "cfg-existing": serialize_submission_record(existing_rec)
        }
        body_str = json.dumps(payload)
        body_mock = MagicMock()
        body_mock.read.return_value = body_str.encode("utf-8")
        client = MagicMock()
        client.get_object.return_value = {"Body": body_mock, "ETag": _ETAG}
        client.put_object.return_value = {"ETag": _NEW_ETAG}

        store = StateStore()
        store.record_submission(
            client, _STATE_BUCKET, self._make_submission("cfg-new"), _ETAG
        )
        kwargs = client.put_object.call_args[1]
        written = json.loads(kwargs["Body"])
        assert set(written["submission_records"].keys()) == {
            "job-cfg-existing", "job-cfg-new",
        }
        assert written["submission_records"]["job-cfg-new"]["job_id"] == "job-cfg-new"
        assert (
            written["submission_records"]["job-cfg-existing"]["job_id"]
            == "job-cfg-existing"
        )


# ---------------------------------------------------------------------------
# record_submission / get_submission_records — single-record round trip,
# legacy multi-config migration-on-read, and top-level key preservation
# (Task 3.2 / Requirements 2.2, 2.4)
# ---------------------------------------------------------------------------


class TestSingleRecordRoundTrip:
    """A SubmissionRecord written via record_submission is read back via
    get_submission_records under its job_id key, with every field
    intact (Req 2.2)."""

    def _make_submission(self) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket=_SRC_BUCKET,
            job_id="job-round-trip",
            manifest_key=f"manifests/{_SRC_BUCKET}/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=_WM_042,
            watermark_high=_WM_077,
            consecutive_failures=2,
        )

    def test_round_trip_preserves_all_fields(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        submission = self._make_submission()

        store.record_submission(client, _STATE_BUCKET, submission, _ETAG)

        # Feed the written body back as the "current" state for the read,
        # mirroring the pattern used elsewhere in this file (e.g.
        # TestRecordScanResultAndGetScanState.test_round_trips_scan_state).
        written_payload = json.loads(client.put_object.call_args[1]["Body"])
        read_client = self._mock_s3_get_raw(written_payload)

        result = store.get_submission_records(read_client, _STATE_BUCKET, _SRC_BUCKET)

        assert set(result.keys()) == {"job-round-trip"}
        rec = result["job-round-trip"]
        assert rec.job_id == submission.job_id
        assert rec.manifest_key == submission.manifest_key
        assert rec.status == submission.status
        assert rec.watermark_low == submission.watermark_low
        assert rec.watermark_high == submission.watermark_high
        assert rec.consecutive_failures == submission.consecutive_failures
        assert rec.source_bucket == submission.source_bucket
        assert rec.replication_config_id == submission.replication_config_id
        assert rec.submitted_at == submission.submitted_at

    @staticmethod
    def _mock_s3_get_raw(payload: dict) -> MagicMock:
        client = MagicMock()
        body = MagicMock()
        body.read.return_value = json.dumps(payload).encode("utf-8")
        client.get_object.return_value = {"Body": body, "ETag": _NEW_ETAG}
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        return client


class TestLegacyMultiConfigStateRead:
    """A legacy state object whose submission_records dict is keyed by
    multiple old-style replication_config_id values reads without error and
    returns every record, re-keyed by its own job_id (Req 1.2)."""

    def _legacy_record(self, config_id: str, job_id: str) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id=config_id,
            source_bucket=_SRC_BUCKET,
            job_id=job_id,
            manifest_key=f"manifests/{config_id}/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=_WM_042,
            watermark_high=_WM_077,
        )

    def _legacy_multi_config_payload(self) -> dict:
        from src.core.checkpoint_serializer import serialize_submission_record

        state = _make_state()
        payload = json.loads(serialize(state))
        payload["submission_records"] = {
            "cfg-1": serialize_submission_record(self._legacy_record("cfg-1", "job-cfg-1")),
            "cfg-2": serialize_submission_record(self._legacy_record("cfg-2", "job-cfg-2")),
        }
        return payload

    def test_reads_without_error_and_returns_all_legacy_records(self):
        payload = self._legacy_multi_config_payload()
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        result = store.get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)

        assert set(result.keys()) == {"job-cfg-1", "job-cfg-2"}
        assert result["job-cfg-1"].replication_config_id == "cfg-1"
        assert result["job-cfg-2"].replication_config_id == "cfg-2"


class TestLegacyMultiConfigStateAfterOneWrite:
    """One record_submission re-keys a legacy state object by job_id.

    The migration is free: every SubmissionRecord carries its own job_id, so a
    payload keyed by bucket name or by replication_config_id loads correctly on
    read and the first write persists the new keying as a side effect. No
    migration write, no schema version, nothing for an operator to do (Req 1.2).

    Crucially the legacy records are re-keyed rather than dropped. Dropping them
    is what discarded a running job's id along with its report, its
    report-missing check, and its rollback target.
    """

    def _legacy_record(self, config_id: str, job_id: str) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id=config_id,
            source_bucket=_SRC_BUCKET,
            job_id=job_id,
            manifest_key=f"manifests/{config_id}/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=_WM_042,
            watermark_high=_WM_077,
        )

    def _legacy_multi_config_payload(self) -> dict:
        from src.core.checkpoint_serializer import serialize_submission_record

        state = _make_state()
        payload = json.loads(serialize(state))
        payload["submission_records"] = {
            "cfg-1": serialize_submission_record(self._legacy_record("cfg-1", "job-cfg-1")),
            "cfg-2": serialize_submission_record(self._legacy_record("cfg-2", "job-cfg-2")),
        }
        return payload

    def test_one_write_re_keys_every_record_by_job_id(self):
        payload = self._legacy_multi_config_payload()
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        new_submission = SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket=_SRC_BUCKET,
            job_id="job-new-union",
            manifest_key=f"manifests/{_SRC_BUCKET}/20240102T000000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
        )
        store.record_submission(client, _STATE_BUCKET, new_submission, _ETAG)

        written = json.loads(client.put_object.call_args[1]["Body"])
        records = written["submission_records"]

        assert set(records.keys()) == {"job-cfg-1", "job-cfg-2", "job-new-union"}
        assert records["job-new-union"]["job_id"] == "job-new-union"

        # And reads back in the new keying without error.
        read_client = _mock_s3_get_raw_payload(written)
        result = store.get_submission_records(read_client, _STATE_BUCKET, _SRC_BUCKET)
        assert set(result.keys()) == {"job-cfg-1", "job-cfg-2", "job-new-union"}

    def test_a_bucket_name_keyed_record_is_re_keyed_by_its_job_id(self):
        """The shape 1.0.1 and the interim 1.1.0 build actually wrote."""
        from src.core.checkpoint_serializer import serialize_submission_record

        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = {
            _SRC_BUCKET: serialize_submission_record(
                self._legacy_record(_SRC_BUCKET, "job-from-1-0-1")
            )
        }
        client = _mock_s3_get_raw_payload(payload)

        result = StateStore().get_submission_records(
            client, _STATE_BUCKET, _SRC_BUCKET
        )

        assert set(result.keys()) == {"job-from-1-0-1"}
        client.put_object.assert_not_called()  # no migration write

    def test_a_record_with_no_job_id_is_dropped_on_read(self):
        """It identifies no job, so nothing can be described, merged, or rolled
        back from it, and keeping it would put a "" key in the dict (Req 1.2)."""
        from src.core.checkpoint_serializer import serialize_submission_record

        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = {
            _SRC_BUCKET: serialize_submission_record(
                self._legacy_record(_SRC_BUCKET, "")
            ),
            "job-real": serialize_submission_record(
                self._legacy_record(_SRC_BUCKET, "job-real")
            ),
        }
        client = _mock_s3_get_raw_payload(payload)

        result = StateStore().get_submission_records(
            client, _STATE_BUCKET, _SRC_BUCKET
        )

        assert set(result.keys()) == {"job-real"}


class TestRecordSubmissionPreservesOtherTopLevelKeys:
    """record_submission on a state object that also carries an unrelated
    completion_items key, a lease, and a processed_window leaves those keys
    untouched after the write (Req 2.2)."""

    def test_lease_completion_items_and_processed_window_untouched(self):
        lease = _make_lease()
        state = CheckpointState(
            source_bucket=_SRC_BUCKET,
            last_processed_watermark=_WM_050,
            lease=lease,
            processed_window=[
                ProcessedRef(logical_operation_id="op-1", watermark=_WM_042)
            ],
        )
        payload = json.loads(serialize(state))
        item = _make_item(object_key="key-untouched")
        payload["completion_items"] = serialize_completion_items(
            {_item_key_fn("key-untouched", "v1"): item}
        )
        payload["completion_processed_job_ids"] = ["job-already-processed"]
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        submission = SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket=_SRC_BUCKET,
            job_id="job-new",
            manifest_key=f"manifests/{_SRC_BUCKET}/20240101T120000Z.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
        )
        store.record_submission(client, _STATE_BUCKET, submission, _ETAG)

        written = json.loads(client.put_object.call_args[1]["Body"])

        assert written["last_processed_watermark"] == _WM_050
        assert written["lease"]["lease_id"] == lease.lease_id
        assert written["lease"]["status"] == lease.status.value
        assert written["processed_window"] == [
            {"logical_operation_id": "op-1", "watermark": _WM_042}
        ]
        assert written["completion_items"] == payload["completion_items"]
        assert written["completion_processed_job_ids"] == ["job-already-processed"]
        # And the new submission record was still written correctly.
        assert written["submission_records"]["job-new"]["job_id"] == "job-new"


# ---------------------------------------------------------------------------
# Serializer watermark round-trip (Task 4.6 / Req 4.5)
# ---------------------------------------------------------------------------


class TestSerializerSubmissionRecordWatermarks:
    def test_watermark_fields_serialize_round_trip(self):
        """serialize_submission_record / deserialize_submission_record round-trips."""
        from src.core.checkpoint_serializer import (
            serialize_submission_record,
            deserialize_submission_records,
        )
        rec = SubmissionRecord(
            replication_config_id="cfg-rt",
            source_bucket=_SRC_BUCKET,
            job_id="job-rt",
            manifest_key="manifests/cfg-rt/key.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=_WM_042,
            watermark_high=_WM_077,
        )
        payload = {"submission_records": {"cfg-rt": serialize_submission_record(rec)}}
        result = deserialize_submission_records(payload)
        assert result["cfg-rt"].watermark_low == _WM_042
        assert result["cfg-rt"].watermark_high == _WM_077

    def test_absent_watermarks_default_to_empty_string(self):
        """Backward compat: missing watermark_low/high fields default to ''."""
        from src.core.checkpoint_serializer import deserialize_submission_records
        import json
        payload = {
            "submission_records": {
                "cfg-old": {
                    "replication_config_id": "cfg-old",
                    "source_bucket": _SRC_BUCKET,
                    "job_id": "job-old",
                    "manifest_key": "manifests/cfg-old/key.csv",
                    "submitted_at": _NOW.isoformat(),
                    "status": "SUBMITTED",
                    # no watermark_low, no watermark_high
                }
            }
        }
        result = deserialize_submission_records(payload)
        assert result["cfg-old"].watermark_low == ""
        assert result["cfg-old"].watermark_high == ""

    def test_absent_submission_records_returns_empty_dict(self):
        """Calling deserialize_submission_records on a payload without the key → {}."""
        from src.core.checkpoint_serializer import deserialize_submission_records
        result = deserialize_submission_records({"source_bucket": "x"})
        assert result == {}


# ---------------------------------------------------------------------------
# completion_job_exists / merge_completion_report /
# delete_completion_items — Task 10.1
# (Requirements 2.1, 2.4, 2.6, 3.1)
# ---------------------------------------------------------------------------

_JOB_T0 = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
_JOB_T1 = datetime(2024, 6, 16, 8, 0, 0, tzinfo=timezone.utc)


def _make_config_context(
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
    manifest_generated_at: datetime = _JOB_T0,
    bops_confirmed: bool = True,
) -> ConfigContext:
    return ConfigContext(
        replication_config_id=replication_config_id,
        job_id=job_id,
        manifest_generated_at=manifest_generated_at,
        bops_confirmed=bops_confirmed,
    )


def _make_item(
    object_key: str = "key-a",
    version_id: str | None = "v1",
    source_bucket: str = _SRC_BUCKET,
    configs: dict[str, ConfigContext] | None = None,
    state: CompletionState = CompletionState.PENDING,
) -> TrackedObject:
    return TrackedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
        configs=configs if configs is not None else {"cfg-1": _make_config_context()},
        state=state,
    )


def _payload_with_completion_items(
    items: dict[str, TrackedObject],
    watermark: str = _WM_050,
    processed_job_ids: set[str] | None = None,
) -> dict:
    """Build a raw state-object payload dict with checkpoint fields plus a
    completion_items key (and optionally completion_processed_job_ids), via
    the real serializers (mirrors the pattern in TestGetSubmissionRecords)."""
    state = _make_state(watermark=watermark)
    payload = json.loads(serialize(state))
    payload["completion_items"] = serialize_completion_items(items)
    if processed_job_ids is not None:
        payload["completion_processed_job_ids"] = list(processed_job_ids)
    return payload


def _mock_s3_get_raw_payload(payload: dict, etag: str = _ETAG) -> MagicMock:
    """Return a mock whose get_object returns the given raw JSON payload."""
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = json.dumps(payload).encode("utf-8")
    client.get_object.return_value = {"Body": body, "ETag": etag}
    client.put_object.return_value = {"ETag": _NEW_ETAG}
    return client


class TestCompletionJobExists:
    def test_returns_true_when_job_id_processed(self):
        payload = _payload_with_completion_items({}, processed_job_ids={"job-abc"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        assert store.completion_job_exists(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc"
        )

    def test_returns_false_when_job_id_not_processed(self):
        payload = _payload_with_completion_items({}, processed_job_ids={"job-other"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        assert not store.completion_job_exists(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-missing"
        )

    def test_returns_false_when_no_processed_job_ids_key(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        assert not store.completion_job_exists(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc"
        )

    def test_returns_false_when_state_object_absent(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        store = StateStore()
        assert not store.completion_job_exists(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc"
        )

    def test_reraises_other_client_errors(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("AccessDenied")
        store = StateStore()
        with pytest.raises(ClientError):
            store.completion_job_exists(client, _STATE_BUCKET, _SRC_BUCKET, "job-abc")

    def test_does_not_mutate(self):
        """Read-only: no put_object call."""
        payload = _payload_with_completion_items({}, processed_job_ids={"job-abc"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.completion_job_exists(client, _STATE_BUCKET, _SRC_BUCKET, "job-abc")
        client.put_object.assert_not_called()


class TestMergeCompletionReport:
    def _report(self, *keys: str) -> BopsCompletionReport:
        return BopsCompletionReport(
            created_at=_JOB_T0,
            entries=tuple(
                ManifestEntry(
                    source_bucket=_SRC_BUCKET,
                    object_key=key,
                    version_id="v1",
                    task_status="succeeded",
                )
                for key in keys
            ),
        )

    def test_creates_new_item_with_resolved_state_and_confirmed_config(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id="cfg-1",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=None,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        item_key = _item_key_fn("key-a", "v1")
        assert item_key in body["completion_items"]
        item = body["completion_items"][item_key]
        assert item["state"] == "RESOLVED"
        assert item["replication_outcome"] == "COMPLETE"
        cfg = item["configs"]["cfg-1"]
        assert cfg["job_id"] == "job-new"
        assert cfg["bops_confirmed"] is True
        assert kwargs.get("IfNoneMatch") == "*"

    def test_records_job_id_in_processed_job_ids(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id="cfg-1",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=None,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert "job-new" in body["completion_processed_job_ids"]

    def test_preserves_existing_checkpoint_lease_and_submission_records(self):
        state = _make_state(watermark=_WM_077, lease=_make_lease())
        payload = json.loads(serialize(state))
        payload["submission_records"] = {"cfg-x": {"job_id": "old-job"}}
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id="cfg-1",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_077
        assert body["lease"] is not None
        assert body["submission_records"] == {"cfg-x": {"job_id": "old-job"}}

    def test_merges_new_config_into_already_existing_item(self):
        """Requirement 2.6: a sibling rule's job already created this item
        with a config for a different replication_config_id — the new
        job's config must be added alongside it, not overwrite it."""
        existing_item = _make_item(
            object_key="key-a",
            version_id="v1",
            configs={"cfg-other": _make_config_context(replication_config_id="cfg-other", job_id="job-old")},
        )
        payload = _payload_with_completion_items({_item_key_fn("key-a", "v1"): existing_item})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id="cfg-new",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        configs = body["completion_items"][_item_key_fn("key-a", "v1")]["configs"]
        assert "cfg-other" in configs
        assert "cfg-new" in configs
        assert configs["cfg-other"]["job_id"] == "job-old"
        assert configs["cfg-new"]["job_id"] == "job-new"

    def test_preserves_other_completion_items(self):
        existing_item = _make_item(object_key="key-existing", version_id="v1")
        payload = _payload_with_completion_items({_item_key_fn("key-existing", "v1"): existing_item})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-new"),
            replication_config_id="cfg-1",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert _item_key_fn("key-existing", "v1") in body["completion_items"]
        assert _item_key_fn("key-new", "v1") in body["completion_items"]

    def test_uses_if_match_when_etag_provided(self):
        payload = _payload_with_completion_items({})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id="cfg-1",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfMatch") == _ETAG

    def test_stale_etag_raises_conditional_write_error(self):
        payload = _payload_with_completion_items({})
        client = _mock_s3_get_raw_payload(payload)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.merge_completion_report(
                client, _STATE_BUCKET, _SRC_BUCKET,
                report=self._report("key-a"),
                replication_config_id="cfg-1",
                job_id="job-new",
                job_created_at=_JOB_T0,
                current_etag="stale",
            )

    def test_returns_new_etag(self):
        payload = _payload_with_completion_items({})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        new_etag = store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id="cfg-1",
            job_id="job-new",
            job_created_at=_JOB_T0,
            current_etag=_ETAG,
        )
        assert new_etag == _NEW_ETAG

    def test_two_jobs_keyed_by_same_bucket_sentinel_collapse_to_one_config(self):
        """D4 formalization (task 5.1): design.md D4 says the single
        per-bucket job produces ONE ConfigContext per object, keyed by the
        per-bucket sentinel. If the orchestrator calls
        merge_completion_report twice for the SAME object with
        replication_config_id == the bucket sentinel each time (e.g. two
        legacy jobs terminating in the same migration-window run), the
        SECOND call's ConfigContext must overwrite the first under that one
        key — not add a second entry — so configs ends up with exactly one
        key regardless of how many jobs merged into it. This is the
        intended "latest job's confirmation counts" behavior, not data
        loss."""
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()

        # First legacy job's report merge, keyed by the bucket sentinel.
        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id=_SRC_BUCKET,
            job_id="job-legacy-a",
            job_created_at=_JOB_T0,
            current_etag=None,
        )
        first_body = json.loads(client.put_object.call_args[1]["Body"])

        # Simulate the second call reading back what the first call wrote.
        client2 = _mock_s3_get_raw_payload(first_body)
        store.merge_completion_report(
            client2, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-a"),
            replication_config_id=_SRC_BUCKET,
            job_id="job-legacy-b",
            job_created_at=_JOB_T1,
            current_etag=_ETAG,
        )
        second_body = json.loads(client2.put_object.call_args[1]["Body"])

        item_key = _item_key_fn("key-a", "v1")
        configs = second_body["completion_items"][item_key]["configs"]
        assert list(configs.keys()) == [_SRC_BUCKET]
        assert configs[_SRC_BUCKET]["job_id"] == "job-legacy-b"

    def test_disjoint_objects_from_different_jobs_both_survive_under_sentinel(self):
        """A later job's merge only touches the objects listed in ITS OWN
        report. An object unique to an earlier job's report — and not
        re-listed in the later job's report — keeps its earlier, already
        bops_confirmed ConfigContext untouched. No object is silently
        dropped by a later merge for a different set of objects."""
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()

        store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-only-in-first"),
            replication_config_id=_SRC_BUCKET,
            job_id="job-first",
            job_created_at=_JOB_T0,
            current_etag=None,
        )
        first_body = json.loads(client.put_object.call_args[1]["Body"])

        client2 = _mock_s3_get_raw_payload(first_body)
        store.merge_completion_report(
            client2, _STATE_BUCKET, _SRC_BUCKET,
            report=self._report("key-only-in-second"),
            replication_config_id=_SRC_BUCKET,
            job_id="job-second",
            job_created_at=_JOB_T1,
            current_etag=_ETAG,
        )
        second_body = json.loads(client2.put_object.call_args[1]["Body"])

        items = second_body["completion_items"]
        first_key = _item_key_fn("key-only-in-first", "v1")
        second_key = _item_key_fn("key-only-in-second", "v1")
        assert first_key in items
        assert second_key in items
        assert items[first_key]["configs"][_SRC_BUCKET]["job_id"] == "job-first"
        assert items[second_key]["configs"][_SRC_BUCKET]["job_id"] == "job-second"


class TestDeleteCompletionItems:
    def test_removes_given_item_keys(self):
        item = _make_item(object_key="key-to-delete")
        payload = _payload_with_completion_items({_item_key_fn("key-to-delete", "v1"): item})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET,
            [_item_key_fn("key-to-delete", "v1")], current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert _item_key_fn("key-to-delete", "v1") not in body["completion_items"]

    def test_removes_exactly_given_keys_leaving_others_untouched(self):
        item_a = _make_item(object_key="key-a")
        item_b = _make_item(object_key="key-b")
        item_c = _make_item(object_key="key-c")
        payload = _payload_with_completion_items(
            {
                _item_key_fn("key-a", "v1"): item_a,
                _item_key_fn("key-b", "v1"): item_b,
                _item_key_fn("key-c", "v1"): item_c,
            }
        )
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET,
            [_item_key_fn("key-a", "v1"), _item_key_fn("key-c", "v1")],
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert _item_key_fn("key-a", "v1") not in body["completion_items"]
        assert _item_key_fn("key-c", "v1") not in body["completion_items"]
        assert _item_key_fn("key-b", "v1") in body["completion_items"]

    def test_preserves_checkpoint_lease_and_submission_records(self):
        state = _make_state(watermark=_WM_042, lease=_make_lease())
        item = _make_item(object_key="key-to-delete")
        payload = json.loads(serialize(state))
        payload["completion_items"] = serialize_completion_items({_item_key_fn("key-to-delete", "v1"): item})
        payload["submission_records"] = {"cfg-x": {"job_id": "old-job"}}
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET,
            [_item_key_fn("key-to-delete", "v1")], current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_042
        assert body["lease"] is not None
        assert body["submission_records"] == {"cfg-x": {"job_id": "old-job"}}

    def test_deleting_absent_item_key_is_a_noop_write(self):
        item = _make_item(object_key="key-a")
        payload = _payload_with_completion_items({_item_key_fn("key-a", "v1"): item})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET, [_item_key_fn("key-missing", "v1")], current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert _item_key_fn("key-a", "v1") in body["completion_items"]

    def test_stale_etag_raises_conditional_write_error(self):
        item = _make_item(object_key="key-a")
        payload = _payload_with_completion_items({_item_key_fn("key-a", "v1"): item})
        client = _mock_s3_get_raw_payload(payload)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.delete_completion_items(
                client, _STATE_BUCKET, _SRC_BUCKET, [_item_key_fn("key-a", "v1")], current_etag="stale",
            )

    def test_returns_new_etag(self):
        item = _make_item(object_key="key-a")
        payload = _payload_with_completion_items({_item_key_fn("key-a", "v1"): item})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        new_etag = store.delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET, [_item_key_fn("key-a", "v1")], current_etag=_ETAG,
        )
        assert new_etag == _NEW_ETAG


# ---------------------------------------------------------------------------
# Property 3: A Config_Context is created at most once per job_id
# Feature: source-status-completion-tracking, Property 3: A Config_Context
# is created at most once per job_id
# Validates: Requirements 2.4
# ---------------------------------------------------------------------------


class _FakeConditionalS3:
    """A minimal fake S3 client that models one state object with real
    conditional-write (If-Match/If-None-Match) semantics, so that a second
    merge_completion_report call for the same job_id operates against
    the state as it would actually evolve across two real calls."""

    def __init__(self) -> None:
        self._body: str | None = None
        self._etag: str | None = None
        self._version = 0

    def get_object(self, Bucket, Key):  # noqa: N803 - mirrors boto3 signature
        if self._body is None:
            raise _client_error("NoSuchKey")
        body = MagicMock()
        body.read.return_value = self._body.encode("utf-8")
        return {"Body": body, "ETag": self._etag}

    def put_object(self, **kwargs):
        if_match = kwargs.get("IfMatch")
        if_none_match = kwargs.get("IfNoneMatch")
        if if_none_match == "*" and self._body is not None:
            raise _client_error("PreconditionFailed")
        if if_match is not None and if_match != self._etag:
            raise _client_error("PreconditionFailed")
        self._body = kwargs["Body"].decode("utf-8")
        self._version += 1
        self._etag = f'"etag-{self._version}"'
        return {"ETag": self._etag}


_job_ids_st = st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True)
_object_keys_st = st.text(min_size=1, max_size=15)


@st.composite
def _completion_job_pair(draw):
    """Generate a job_id plus two distinct object-key lists (an original
    manifest and an arbitrary different one for the second create
    attempt)."""
    job_id = draw(_job_ids_st)
    original_keys = draw(
        st.lists(_object_keys_st, min_size=1, max_size=5, unique=True)
    )
    other_keys = draw(
        st.lists(_object_keys_st, min_size=1, max_size=5, unique=True)
    )
    return job_id, original_keys, other_keys


class TestProperty3CreatedAtMostOncePerJobId:
    """Calling the creation path a second time when job_id already appears
    in completion_processed_job_ids (guarded by completion_job_exists,
    mirroring the orchestrator's gate) produces no additional or altered
    ConfigContext entries anywhere in completion_items.

    # Feature: source-status-completion-tracking, Property 3: A Config_Context
    is created at most once per job_id
    Validates: Requirements 2.4
    """

    @given(pair=_completion_job_pair())
    @settings(max_examples=100)
    def test_guarded_second_merge_leaves_items_unchanged(self, pair) -> None:
        job_id, original_keys, other_keys = pair
        client = _FakeConditionalS3()
        store = StateStore()

        original_report = BopsCompletionReport(
            created_at=_JOB_T0,
            entries=tuple(
                ManifestEntry(
                    source_bucket=_SRC_BUCKET,
                    object_key=key,
                    version_id="v1",
                    task_status="succeeded",
                )
                for key in original_keys
            ),
        )
        etag = store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=original_report,
            replication_config_id="cfg-1",
            job_id=job_id,
            job_created_at=_JOB_T0,
            current_etag=None,
        )

        # Mirror the orchestrator's guard: only merge if not already present.
        if not store.completion_job_exists(client, _STATE_BUCKET, _SRC_BUCKET, job_id):
            other_report = BopsCompletionReport(
                created_at=_JOB_T0,
                entries=tuple(
                    ManifestEntry(
                        source_bucket=_SRC_BUCKET,
                        object_key=key,
                        version_id="v1",
                        task_status="succeeded",
                    )
                    for key in other_keys
                ),
            )
            store.merge_completion_report(
                client, _STATE_BUCKET, _SRC_BUCKET,
                report=other_report,
                replication_config_id="cfg-1",
                job_id=job_id,
                job_created_at=_JOB_T0,
                current_etag=etag,
            )

        # The guard must have prevented the second merge: exactly the
        # original set of item keys carry a config created by job_id.
        items = store.get_all_completion_items(client, _STATE_BUCKET, _SRC_BUCKET)
        keys_touched_by_job = {
            item_key
            for item_key, item in items.items()
            if any(ctx.job_id == job_id for ctx in item.configs.values())
        }
        expected_keys = {_item_key_fn(k, "v1") for k in original_keys}
        assert keys_touched_by_job == expected_keys


# ---------------------------------------------------------------------------
# record_scan_result / get_scan_state — Task 12.2 (Requirements 5.1, 5.2, 5.3)
# ---------------------------------------------------------------------------

_SCAN_AT_0 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
_SCAN_AT_1 = datetime(2024, 6, 16, 9, 0, 0, tzinfo=timezone.utc)


class TestRecordScanResultAndGetScanState:
    def test_round_trips_scan_state(self):
        """record_scan_result followed by get_scan_state returns the same values."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            "cfg-1",
            scan_at=_SCAN_AT_0,
            match_count=5,
            current_etag=_ETAG,
        )
        # Feed the written body back as the "current" state for the read.
        kwargs = client.put_object.call_args[1]
        written_payload = json.loads(kwargs["Body"])
        client2 = _mock_s3_get_raw_payload(written_payload)
        result = store.get_scan_state(client2, _STATE_BUCKET, _SRC_BUCKET)
        assert "cfg-1" in result
        assert result["cfg-1"].last_scan_at == _SCAN_AT_0
        assert result["cfg-1"].last_scan_match_count == 5

    def test_round_trips_scan_state_keyed_by_bucket_sentinel(self):
        """design.md D5: record_scan_result/get_scan_state round-trip
        correctly when the caller passes the per-bucket sentinel (the
        source bucket's own name) as the ``replication_config_id`` key,
        matching the orchestrator's per-bucket call site."""
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            _SRC_BUCKET,  # per-bucket sentinel, per design.md D5
            scan_at=_SCAN_AT_0,
            match_count=42,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        written_payload = json.loads(kwargs["Body"])
        client2 = _mock_s3_get_raw_payload(written_payload)
        result = store.get_scan_state(client2, _STATE_BUCKET, _SRC_BUCKET)
        assert _SRC_BUCKET in result
        assert result[_SRC_BUCKET].last_scan_at == _SCAN_AT_0
        assert result[_SRC_BUCKET].last_scan_match_count == 42

    def test_uses_if_match_when_etag_provided(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            "cfg-1",
            scan_at=_SCAN_AT_0,
            match_count=0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfMatch") == _ETAG

    def test_missing_state_object_defaults_to_fresh_checkpoint(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        new_etag = store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            "cfg-1",
            scan_at=_SCAN_AT_0,
            match_count=0,
        )
        assert new_etag == _NEW_ETAG
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfNoneMatch") == "*"

    def test_recording_one_config_never_alters_another_configs_entry(self):
        """record_scan_result for cfg-1 must not touch cfg-2's stored entry."""
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["completion_scan_state"] = {
            "cfg-2": {
                "last_scan_at": _SCAN_AT_0.isoformat(),
                "last_scan_match_count": 7,
            }
        }
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            "cfg-1",
            scan_at=_SCAN_AT_1,
            match_count=0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["completion_scan_state"]["cfg-2"]["last_scan_match_count"] == 7
        assert body["completion_scan_state"]["cfg-1"]["last_scan_match_count"] == 0

    def test_overwrites_existing_entry_for_same_config(self):
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["completion_scan_state"] = {
            "cfg-1": {
                "last_scan_at": _SCAN_AT_0.isoformat(),
                "last_scan_match_count": 3,
            }
        }
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            "cfg-1",
            scan_at=_SCAN_AT_1,
            match_count=0,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["completion_scan_state"]["cfg-1"]["last_scan_match_count"] == 0
        assert body["completion_scan_state"]["cfg-1"]["last_scan_at"] == _SCAN_AT_1.isoformat()

    def test_preserves_checkpoint_lease_submission_records_and_completion_items(self):
        state = _make_state(watermark=_WM_077, lease=_make_lease())
        item = _make_item(object_key="key-a")
        payload = json.loads(serialize(state))
        payload["submission_records"] = {"cfg-x": {"job_id": "old-job"}}
        payload["completion_items"] = serialize_completion_items({_item_key_fn("key-a", "v1"): item})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.record_scan_result(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            "cfg-1",
            scan_at=_SCAN_AT_0,
            match_count=1,
            current_etag=_ETAG,
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_077
        assert body["lease"] is not None
        assert body["submission_records"] == {"cfg-x": {"job_id": "old-job"}}
        assert _item_key_fn("key-a", "v1") in body["completion_items"]

    def test_stale_etag_raises_conditional_write_error(self):
        state = _make_state()
        client = _mock_s3_get(state)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.record_scan_result(
                client,
                _STATE_BUCKET,
                _SRC_BUCKET,
                "cfg-1",
                scan_at=_SCAN_AT_0,
                match_count=0,
                current_etag="stale",
            )

    def test_get_scan_state_returns_empty_dict_when_no_key(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        result = store.get_scan_state(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {}

    def test_get_scan_state_returns_empty_dict_when_state_object_absent(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        store = StateStore()
        result = store.get_scan_state(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {}

    def test_get_scan_state_reraises_other_client_errors(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("AccessDenied")
        store = StateStore()
        with pytest.raises(ClientError):
            store.get_scan_state(client, _STATE_BUCKET, _SRC_BUCKET)

    def test_get_scan_state_does_not_mutate(self):
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["completion_scan_state"] = {
            "cfg-1": {
                "last_scan_at": _SCAN_AT_0.isoformat(),
                "last_scan_match_count": 0,
            }
        }
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.get_scan_state(client, _STATE_BUCKET, _SRC_BUCKET)
        client.put_object.assert_not_called()

    def test_get_scan_state_returns_multiple_configs(self):
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["completion_scan_state"] = {
            "cfg-1": {
                "last_scan_at": _SCAN_AT_0.isoformat(),
                "last_scan_match_count": 0,
            },
            "cfg-2": {
                "last_scan_at": _SCAN_AT_1.isoformat(),
                "last_scan_match_count": 4,
            },
        }
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        result = store.get_scan_state(client, _STATE_BUCKET, _SRC_BUCKET)
        assert set(result.keys()) == {"cfg-1", "cfg-2"}
        assert result["cfg-1"].last_scan_match_count == 0
        assert result["cfg-2"].last_scan_match_count == 4


# ---------------------------------------------------------------------------
# get_alerted_configs / add_alerted_config / clear_alerted_config — Task 23.5
# (Requirements 8.5, 8.6)
# ---------------------------------------------------------------------------


def _payload_with_alerted_configs(
    alerted: set[str] | None = None,
    watermark: str = _WM_050,
) -> dict:
    state = _make_state(watermark=watermark)
    payload = json.loads(serialize(state))
    if alerted is not None:
        payload["completion_report_alerted_configs"] = list(alerted)
    return payload


class TestGetAlertedConfigs:
    def test_returns_configs_when_present(self):
        payload = _payload_with_alerted_configs({"cfg-1", "cfg-2"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        result = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == {"cfg-1", "cfg-2"}

    def test_returns_empty_set_when_key_absent(self):
        state = _make_state()
        client = _mock_s3_get(state)
        store = StateStore()
        result = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == set()

    def test_returns_empty_set_when_state_object_absent(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        store = StateStore()
        result = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result == set()

    def test_reraises_other_client_errors(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("AccessDenied")
        store = StateStore()
        with pytest.raises(ClientError):
            store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)

    def test_does_not_mutate(self):
        payload = _payload_with_alerted_configs({"cfg-1"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        client.put_object.assert_not_called()


class TestAddAlertedConfig:
    def test_adds_config_to_empty_set(self):
        payload = _payload_with_alerted_configs(set())
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert set(body["completion_report_alerted_configs"]) == {"cfg-1"}

    def test_preserves_other_alerted_configs(self):
        payload = _payload_with_alerted_configs({"cfg-existing"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-new", current_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert set(body["completion_report_alerted_configs"]) == {"cfg-existing", "cfg-new"}

    def test_adding_already_alerted_config_is_idempotent(self):
        payload = _payload_with_alerted_configs({"cfg-1"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert set(body["completion_report_alerted_configs"]) == {"cfg-1"}

    def test_preserves_checkpoint_lease_and_completion_items(self):
        state = _make_state(watermark=_WM_042, lease=_make_lease())
        item = _make_item(object_key="key-a")
        payload = json.loads(serialize(state))
        payload["completion_items"] = serialize_completion_items({_item_key_fn("key-a", "v1"): item})
        payload["submission_records"] = {"cfg-x": {"job_id": "old-job"}}
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_042
        assert body["lease"] is not None
        assert body["submission_records"] == {"cfg-x": {"job_id": "old-job"}}
        assert _item_key_fn("key-a", "v1") in body["completion_items"]

    def test_missing_state_object_defaults_to_fresh_checkpoint(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        new_etag = store.add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1")
        assert new_etag == _NEW_ETAG
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfNoneMatch") == "*"

    def test_stale_etag_raises_conditional_write_error(self):
        payload = _payload_with_alerted_configs(set())
        client = _mock_s3_get_raw_payload(payload)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.add_alerted_config(
                client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag="stale"
            )


class TestClearAlertedConfig:
    def test_removes_given_config(self):
        payload = _payload_with_alerted_configs({"cfg-1", "cfg-2"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.clear_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert set(body["completion_report_alerted_configs"]) == {"cfg-2"}

    def test_clearing_absent_config_is_a_noop_write(self):
        payload = _payload_with_alerted_configs({"cfg-1"})
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.clear_alerted_config(
            client, _STATE_BUCKET, _SRC_BUCKET, "cfg-missing", current_etag=_ETAG
        )
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert set(body["completion_report_alerted_configs"]) == {"cfg-1"}

    def test_preserves_checkpoint_lease_and_submission_records(self):
        state = _make_state(watermark=_WM_077, lease=_make_lease())
        payload = json.loads(serialize(state))
        payload["completion_report_alerted_configs"] = ["cfg-1"]
        payload["submission_records"] = {"cfg-x": {"job_id": "old-job"}}
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        store.clear_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=_ETAG)
        kwargs = client.put_object.call_args[1]
        body = json.loads(kwargs["Body"])
        assert body["last_processed_watermark"] == _WM_077
        assert body["lease"] is not None
        assert body["submission_records"] == {"cfg-x": {"job_id": "old-job"}}

    def test_missing_state_object_defaults_to_fresh_checkpoint(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}
        store = StateStore()
        new_etag = store.clear_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1")
        assert new_etag == _NEW_ETAG
        kwargs = client.put_object.call_args[1]
        assert kwargs.get("IfNoneMatch") == "*"

    def test_stale_etag_raises_conditional_write_error(self):
        payload = _payload_with_alerted_configs({"cfg-1"})
        client = _mock_s3_get_raw_payload(payload)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        with pytest.raises(ConditionalWriteError):
            store.clear_alerted_config(
                client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag="stale"
            )

    def test_add_then_clear_round_trip(self):
        """add_alerted_config followed by clear_alerted_config leaves the
        config absent from the set (integration of both methods)."""
        payload = _payload_with_alerted_configs(set())
        client = _FakeConditionalS3()
        store = StateStore()
        client._body = json.dumps(payload)
        client._etag = _ETAG
        etag1 = store.add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=_ETAG)
        result_after_add = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result_after_add == {"cfg-1"}
        store.clear_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag=etag1)
        result_after_clear = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        assert result_after_clear == set()


# ---------------------------------------------------------------------------
# Property 19: Per-config alert suppression persists across repeated
# overdue checks and resets exactly once a report is observed
# Feature: source-status-completion-tracking, Property 19: Per-config alert
# suppression persists across repeated overdue checks and resets exactly
# once a report is observed
# Validates: Requirements 8.5, 8.6
# ---------------------------------------------------------------------------


_alert_event_st = st.sampled_from(["check", "report_observed"])


class TestProperty19AlertSuppressionPersistsAndResets:
    """# Feature: source-status-completion-tracking, Property 19: Per-config alert suppression persists across repeated overdue checks and resets exactly once a report is observed

    Validates: Requirements 8.5, 8.6

    Simulates a sequence of ``check_report_handler``-style events for one
    ``replication_config_id`` against a real (fake) conditional-write S3
    client: a "check" event models an overdue-and-still-absent condition
    (which would only actually alert if the config is not currently
    suppressed — mirrored here directly using ``get_alerted_configs`` +
    ``add_alerted_config``, exactly as ``check_report_handler`` does), and
    a "report_observed" event models the creation hook's
    ``clear_alerted_config`` call. Asserts at most one alert per
    unsuppressed streak, and exactly one further alert after a reset.
    """

    @given(events=st.lists(_alert_event_st, min_size=1, max_size=20))
    @settings(max_examples=100)
    def test_alert_count_matches_unsuppressed_streaks(self, events: list[str]) -> None:
        client = _FakeConditionalS3()
        store = StateStore()
        config_id = "cfg-alert-1"

        # Seed a state object so get_object never 404s across the sequence.
        payload = _payload_with_alerted_configs(set())
        client._body = json.dumps(payload)
        client._etag = _ETAG

        alert_count = 0
        currently_suppressed = False

        for event in events:
            if event == "report_observed":
                store.clear_alerted_config(
                    client, _STATE_BUCKET, _SRC_BUCKET, config_id, current_etag=client._etag
                )
                currently_suppressed = False
            else:  # "check" — models an overdue, still-absent condition
                alerted = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
                if config_id not in alerted:
                    # Would alert exactly once here, then suppress.
                    alert_count += 1
                    store.add_alerted_config(
                        client, _STATE_BUCKET, _SRC_BUCKET, config_id,
                        current_etag=client._etag,
                    )
                    currently_suppressed = True
                else:
                    currently_suppressed = True

        # At most one alert per maximal unsuppressed streak — recompute the
        # expected count by replaying the same state machine independently.
        expected_alerts = 0
        suppressed = False
        for event in events:
            if event == "report_observed":
                suppressed = False
            elif not suppressed:
                expected_alerts += 1
                suppressed = True

        assert alert_count == expected_alerts

        final_alerted = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
        assert (config_id in final_alerted) == currently_suppressed

    def test_single_alert_while_unobserved_then_one_more_after_reset(self) -> None:
        """Concrete example: check, check, check (only first alerts), then
        report observed (reset), then check (alerts again exactly once)."""
        client = _FakeConditionalS3()
        store = StateStore()
        config_id = "cfg-1"
        payload = _payload_with_alerted_configs(set())
        client._body = json.dumps(payload)
        client._etag = _ETAG

        def maybe_alert() -> bool:
            alerted = store.get_alerted_configs(client, _STATE_BUCKET, _SRC_BUCKET)
            if config_id in alerted:
                return False
            store.add_alerted_config(
                client, _STATE_BUCKET, _SRC_BUCKET, config_id, current_etag=client._etag
            )
            return True

        assert maybe_alert() is True
        assert maybe_alert() is False
        assert maybe_alert() is False

        store.clear_alerted_config(
            client, _STATE_BUCKET, _SRC_BUCKET, config_id, current_etag=client._etag
        )

        assert maybe_alert() is True
        assert maybe_alert() is False


# ---------------------------------------------------------------------------
# Alert-suppression conditional write (security-scan-remediation Req 8.2, 8.4)
# ---------------------------------------------------------------------------


class TestAlertSuppressionConditionalWrite:
    """``add_alerted_config``/``clear_alerted_config`` called without an
    explicit ``current_etag`` must use the ETag from their own ``get_object``
    as the precondition (8.2), while a genuine concurrent modification still
    fails the precondition (8.4)."""

    def test_add_uses_etag_from_own_read_when_caller_supplies_none(self):
        client = _mock_s3_get_raw_payload(_payload_with_alerted_configs(set()))
        new_etag = StateStore().add_alerted_config(
            client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1"
        )

        assert new_etag == _NEW_ETAG
        kwargs = client.put_object.call_args.kwargs
        assert kwargs["IfMatch"] == _ETAG
        assert "IfNoneMatch" not in kwargs

    def test_clear_uses_etag_from_own_read_when_caller_supplies_none(self):
        client = _mock_s3_get_raw_payload(_payload_with_alerted_configs({"cfg-1"}))
        StateStore().clear_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1")

        kwargs = client.put_object.call_args.kwargs
        assert kwargs["IfMatch"] == _ETAG
        assert "IfNoneMatch" not in kwargs

    def test_explicit_caller_etag_takes_precedence(self):
        """A caller threading its own ETag chain (e.g. the orchestrator's
        ``clear_alerted_config`` call) is unaffected by the self-read
        fallback (Req 8.3)."""
        client = _mock_s3_get_raw_payload(_payload_with_alerted_configs(set()))
        StateStore().add_alerted_config(
            client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1", current_etag='"chained"'
        )

        assert client.put_object.call_args.kwargs["IfMatch"] == '"chained"'

    def test_missing_state_object_still_creates_with_if_none_match(self):
        client = MagicMock()
        client.get_object.side_effect = _client_error("NoSuchKey")
        client.put_object.return_value = {"ETag": _NEW_ETAG}

        StateStore().add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1")

        kwargs = client.put_object.call_args.kwargs
        assert kwargs["IfNoneMatch"] == "*"
        assert "IfMatch" not in kwargs

    def test_concurrent_modification_still_raises_conditional_write_error(self):
        """Req 8.4: the self-read ETag is a real precondition, not a bypass —
        a write by ReplicationLambda between this method's read and its put
        still fails."""
        client = _mock_s3_get_raw_payload(_payload_with_alerted_configs(set()))
        client.put_object.side_effect = _client_error("PreconditionFailed")

        with pytest.raises(ConditionalWriteError):
            StateStore().add_alerted_config(client, _STATE_BUCKET, _SRC_BUCKET, "cfg-1")


# ---------------------------------------------------------------------------
# mark_report_diagnosed — sets report_diagnosed on submission record
# ---------------------------------------------------------------------------


class TestMarkReportDiagnosed:
    def _payload_with_submission(self, diagnosed: bool = False) -> dict:
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["submission_records"] = {
            _SRC_BUCKET: {
                "replication_config_id": _SRC_BUCKET,
                "source_bucket": _SRC_BUCKET,
                "job_id": "job-abc-123",
                "manifest_key": "manifests/x/y.csv",
                "submitted_at": _NOW.isoformat(),
                "status": SubmissionStatus.SUBMITTED.value,
                "watermark_low": _WM_042,
                "watermark_high": _WM_077,
                "consecutive_failures": 0,
                "report_diagnosed": diagnosed,
            }
        }
        return payload

    def test_sets_report_diagnosed_true(self):
        """mark_report_diagnosed sets the flag on the submission record."""
        payload = self._payload_with_submission(diagnosed=False)
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        store.mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123", current_etag=_ETAG
        )

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        rec = body["submission_records"][_SRC_BUCKET]
        assert rec["report_diagnosed"] is True

    def test_matches_on_job_id_not_on_the_stored_key(self):
        """The payload here is still keyed by bucket name, as 1.0.1 wrote it.

        Keying the lookup would silently stop writing the flag until the first
        record_submission re-keyed the object, and a flag never set means the
        report diagnostic repeats on every run."""
        payload = self._payload_with_submission(diagnosed=False)
        client = _mock_s3_get_raw_payload(payload)

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123", current_etag=_ETAG
        )

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        assert body["submission_records"][_SRC_BUCKET]["report_diagnosed"] is True

    def test_leaves_a_sibling_job_untouched(self):
        payload = self._payload_with_submission(diagnosed=False)
        payload["submission_records"]["job-sibling"] = {
            **payload["submission_records"][_SRC_BUCKET],
            "job_id": "job-sibling",
        }
        client = _mock_s3_get_raw_payload(payload)

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123", current_etag=_ETAG
        )

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        assert body["submission_records"][_SRC_BUCKET]["report_diagnosed"] is True
        assert body["submission_records"]["job-sibling"]["report_diagnosed"] is False

    def test_preserves_other_submission_fields(self):
        """The write does not clobber other fields on the submission record."""
        payload = self._payload_with_submission(diagnosed=False)
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        store.mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, _SRC_BUCKET, current_etag=_ETAG
        )

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        rec = body["submission_records"][_SRC_BUCKET]
        assert rec["job_id"] == "job-abc-123"
        assert rec["consecutive_failures"] == 0
        assert rec["watermark_low"] == _WM_042

    def test_no_op_when_job_id_not_in_records(self):
        """If the job_id is absent, the write still happens (no crash)."""
        payload = self._payload_with_submission(diagnosed=False)
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        store.mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-nonexistent",
            current_etag=_ETAG,
        )

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        rec = body["submission_records"][_SRC_BUCKET]
        assert rec["report_diagnosed"] is False

    def test_conditional_write_uses_provided_etag(self):
        """The PUT uses the provided ETag guard."""
        payload = self._payload_with_submission()
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()

        store.mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123", current_etag=_ETAG
        )

        assert client.put_object.call_args.kwargs["IfMatch"] == _ETAG

    def test_stale_etag_raises_conditional_write_error(self):
        """A concurrent modification raises ConditionalWriteError."""
        payload = self._payload_with_submission()
        client = _mock_s3_get_raw_payload(payload)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()

        with pytest.raises(ConditionalWriteError):
            store.mark_report_diagnosed(
                client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123",
                current_etag="stale",
            )


# ---------------------------------------------------------------------------
# StateWriter.mark_report_diagnosed — best-effort wrapper
# ---------------------------------------------------------------------------


class TestStateWriterMarkReportDiagnosed:
    """Tests for the best-effort wrapper in StateWriter (src/orchestrator.py)."""

    def _payload_with_submission(self) -> dict:
        state = _make_state()
        payload = json.loads(serialize(state))
        payload["submission_records"] = {
            _SRC_BUCKET: {
                "replication_config_id": _SRC_BUCKET,
                "source_bucket": _SRC_BUCKET,
                "job_id": "job-xyz",
                "manifest_key": "manifests/x/y.csv",
                "submitted_at": _NOW.isoformat(),
                "status": SubmissionStatus.SUBMITTED.value,
                "watermark_low": "",
                "watermark_high": "",
                "consecutive_failures": 0,
                "report_diagnosed": False,
            }
        }
        return payload

    def test_success_updates_etag(self):
        """On success, StateWriter advances its held ETag."""
        from src.orchestrator import StateWriter

        payload = self._payload_with_submission()
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        writer = StateWriter(store, client, _STATE_BUCKET, _SRC_BUCKET, _ETAG)

        writer.mark_report_diagnosed("job-xyz")

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        assert body["submission_records"][_SRC_BUCKET]["report_diagnosed"] is True

    def test_failure_is_swallowed(self):
        """A store failure does not propagate — best-effort semantics."""
        from src.orchestrator import StateWriter

        payload = self._payload_with_submission()
        client = _mock_s3_get_raw_payload(payload)
        client.put_object.side_effect = _client_error("PreconditionFailed")
        store = StateStore()
        writer = StateWriter(store, client, _STATE_BUCKET, _SRC_BUCKET, _ETAG)

        writer.mark_report_diagnosed("job-xyz")


# ---------------------------------------------------------------------------
# get_checkpoint — a discarded lease is reported, never silent
# ---------------------------------------------------------------------------


class TestLeaseDiscardedAudit:
    """Dropping an untrustworthy lease unblocks the bucket, but it is state
    being discarded, so it must be visible to an operator."""

    def _poisoned_payload(self, candidate_max="9999-12-31T23:59:59.000000Z"):
        return {
            "source_bucket": _SRC_BUCKET,
            "last_processed_watermark": _WM_050,
            "lease": {
                "lease_id": "lease-1",
                "candidate_max_watermark": candidate_max,
                "acquired_at": _NOW.isoformat(),
                "status": "IN_FLIGHT",
            },
        }

    def test_discarded_lease_emits_audit(self):
        client = _mock_s3_get_raw_payload(self._poisoned_payload())
        store = StateStore()
        with patch("src.adapters.state_store.observability.emit") as emit:
            state, _ = store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        assert state.lease is None
        actions = [
            call[0][0].get("action")
            for call in emit.call_args_list
            if call[0] and isinstance(call[0][0], dict)
        ]
        assert "lease_discarded" in actions

    def test_audit_names_the_bucket(self):
        client = _mock_s3_get_raw_payload(self._poisoned_payload())
        store = StateStore()
        with patch("src.adapters.state_store.observability.emit") as emit:
            store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        entries = [
            call[0][0]
            for call in emit.call_args_list
            if call[0]
            and isinstance(call[0][0], dict)
            and call[0][0].get("action") == "lease_discarded"
        ]
        assert entries, "expected a lease_discarded entry"
        assert entries[0]["source_bucket"] == _SRC_BUCKET

    def test_no_audit_when_lease_is_legitimately_absent(self):
        """An absent lease is the normal state and must not be reported as a
        discard, or every idle run would emit one."""
        payload = {
            "source_bucket": _SRC_BUCKET,
            "last_processed_watermark": _WM_050,
            "lease": None,
        }
        client = _mock_s3_get_raw_payload(payload)
        store = StateStore()
        with patch("src.adapters.state_store.observability.emit") as emit:
            state, _ = store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        assert state.lease is None
        actions = [
            call[0][0].get("action")
            for call in emit.call_args_list
            if call[0] and isinstance(call[0][0], dict)
        ]
        assert "lease_discarded" not in actions

    def test_no_audit_when_lease_is_valid(self):
        client = _mock_s3_get_raw_payload(self._poisoned_payload(_WM_099))
        store = StateStore()
        with patch("src.adapters.state_store.observability.emit") as emit:
            state, _ = store.get_checkpoint(client, _STATE_BUCKET, _SRC_BUCKET)
        assert state.lease is not None
        actions = [
            call[0][0].get("action")
            for call in emit.call_args_list
            if call[0] and isinstance(call[0][0], dict)
        ]
        assert "lease_discarded" not in actions


class TestMergeCompletionReport:
    """Focused tests for report-derived atomic resolution (Req 1.4, 2.6)."""

    def test_resolves_rows_preserves_enrichment_and_records_job_in_one_write(self):
        payload = _payload_with_completion_items({})
        item_a = _item_key_fn("key-a", "v1")
        item_b = _item_key_fn("key-b", "v1")
        payload["completion_timestamps"] = {
            item_a: {"tagged_at": _JOB_T0.isoformat(), "last_modified": _JOB_T1.isoformat()}
        }
        payload["completion_routing"] = {
            item_a: {"matched_rules": ["rule-a"], "destinations": ["dest-a"]}
        }
        client = _mock_s3_get_raw_payload(payload)
        report_created_at = datetime(2024, 6, 17, 9, 15, tzinfo=timezone.utc)

        StateStore().merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=BopsCompletionReport(
                created_at=report_created_at,
                entries=(
                    ManifestEntry(_SRC_BUCKET, "key-a", "v1", task_status=" succeeded "),
                    ManifestEntry(_SRC_BUCKET, "key-b", "v1", task_status="FAILED"),
                ),
            ),
            replication_config_id="cfg-1",
            job_id="job-report",
            job_created_at=_JOB_T1,
            current_etag=_ETAG,
        )

        client.put_object.assert_called_once()
        kwargs = client.put_object.call_args.kwargs
        assert kwargs["IfMatch"] == _ETAG
        body = json.loads(kwargs["Body"])
        assert "job-report" in body["completion_processed_job_ids"]
        assert body["completion_items"][item_a] == {
            "source_bucket": _SRC_BUCKET,
            "object_key": "key-a",
            "version_id": "v1",
            "state": "RESOLVED",
            "resolved_at": report_created_at.isoformat(),
            "resolution_method": "bops_completion_report",
            "replication_outcome": "COMPLETE",
            "configs": {
                "cfg-1": {
                    "replication_config_id": "cfg-1",
                    "job_id": "job-report",
                    "manifest_generated_at": _JOB_T1.isoformat(),
                    "bops_confirmed": True,
                }
            },
            "tagged_at": _JOB_T0.isoformat(),
            "last_modified": _JOB_T1.isoformat(),
            "matched_rules": ["rule-a"],
            "destinations": ["dest-a"],
        }
        assert body["completion_items"][item_b]["replication_outcome"] == "FAILED"

    @pytest.mark.parametrize(
        ("stored_job_id", "incoming_job_id"),
        [("job-new", "job-old"), ("job-z", "job-a")],
    )
    def test_older_report_preserves_newer_resolution_and_records_processed_id(
        self, stored_job_id, incoming_job_id
    ):
        """An older creation time or equal-time lower job ID cannot regress a row.

        **Validates: Requirements 2.7**
        """
        item_key = _item_key_fn("key-a", "v1")
        newer_resolved_at = datetime(2024, 6, 17, 10, tzinfo=timezone.utc)
        existing_item = TrackedObject(
            source_bucket=_SRC_BUCKET,
            object_key="key-a",
            version_id="v1",
            configs={
                "cfg-1": ConfigContext(
                    replication_config_id="cfg-1",
                    job_id=stored_job_id,
                    manifest_generated_at=_JOB_T1,
                    bops_confirmed=True,
                )
            },
            state=CompletionState.RESOLVED,
            resolved_at=newer_resolved_at,
            resolution_method="bops_completion_report",
            replication_outcome="FAILED",
        )
        client = _mock_s3_get_raw_payload(
            _payload_with_completion_items({item_key: existing_item})
        )

        StateStore().merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=BopsCompletionReport(
                created_at=_JOB_T0,
                entries=(
                    ManifestEntry(
                        _SRC_BUCKET, "key-a", "v1", task_status="succeeded"
                    ),
                ),
            ),
            replication_config_id="cfg-1",
            job_id=incoming_job_id,
            job_created_at=_JOB_T0 if stored_job_id == "job-new" else _JOB_T1,
            current_etag=_ETAG,
        )

        body = json.loads(client.put_object.call_args.kwargs["Body"])
        item = body["completion_items"][item_key]
        assert item["configs"]["cfg-1"]["job_id"] == stored_job_id
        assert item["replication_outcome"] == "FAILED"
        assert item["resolved_at"] == newer_resolved_at.isoformat()
        assert incoming_job_id in body["completion_processed_job_ids"]

    def test_mapping_failure_leaves_processed_job_id_and_items_unwritten(self):
        payload = _payload_with_completion_items({}, processed_job_ids={"job-old"})
        client = _mock_s3_get_raw_payload(payload)
        with patch(
            "src.adapters.state_store.completion_tracker.outcome_from_report_row",
            side_effect=ValueError("invalid report status"),
        ):
            with pytest.raises(ValueError, match="invalid report status"):
                StateStore().merge_completion_report(
                    client, _STATE_BUCKET, _SRC_BUCKET,
                    report=BopsCompletionReport(
                        created_at=_JOB_T1,
                        entries=(ManifestEntry(_SRC_BUCKET, "key-a", "v1"),),
                    ),
                    replication_config_id="cfg-1",
                    job_id="job-new",
                    job_created_at=_JOB_T0,
                    current_etag=_ETAG,
                )
        client.put_object.assert_not_called()

    def test_serialization_failure_leaves_processed_job_id_and_items_unwritten(self):
        payload = _payload_with_completion_items({}, processed_job_ids={"job-old"})
        client = _mock_s3_get_raw_payload(payload)
        with patch(
            "src.adapters.state_store.completion_serializer.serialize_completion_items",
            side_effect=ValueError("cannot serialize completion items"),
        ):
            with pytest.raises(ValueError, match="cannot serialize completion items"):
                StateStore().merge_completion_report(
                    client, _STATE_BUCKET, _SRC_BUCKET,
                    report=BopsCompletionReport(
                        created_at=_JOB_T1,
                        entries=(
                            ManifestEntry(
                                _SRC_BUCKET, "key-a", "v1", task_status="succeeded"
                            ),
                        ),
                    ),
                    replication_config_id="cfg-1",
                    job_id="job-new",
                    job_created_at=_JOB_T0,
                    current_etag=_ETAG,
                )
        client.put_object.assert_not_called()


# ---------------------------------------------------------------------------
# Report-derived resolution state transitions — task 2.5 (Req 8.2)
# ---------------------------------------------------------------------------


class TestReportDerivedCompletionStateTransitions:
    """State-store coverage for report outcome mapping and job ordering.

    **Validates: Requirements 1.4, 2.1, 2.2, 2.3, 2.6, 2.7, 8.2**
    """

    _REPORT_T0 = datetime(2024, 6, 17, 9, 0, tzinfo=timezone.utc)
    _REPORT_T1 = datetime(2024, 6, 17, 10, 0, tzinfo=timezone.utc)

    @staticmethod
    def _client(payload: dict | None = None) -> _FakeConditionalS3:
        client = _FakeConditionalS3()
        if payload is not None:
            client._body = json.dumps(payload)
            client._etag = _ETAG
        return client

    def _merge(
        self,
        client: _FakeConditionalS3,
        *,
        job_id: str,
        job_created_at: datetime,
        report_created_at: datetime,
        task_status: str | None = "succeeded",
        etag: str | None = None,
    ) -> str:
        return StateStore().merge_completion_report(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            report=BopsCompletionReport(
                created_at=report_created_at,
                entries=(
                    ManifestEntry(
                        _SRC_BUCKET, "key-a", "v1", task_status=task_status
                    ),
                ),
            ),
            replication_config_id="cfg-1",
            job_id=job_id,
            job_created_at=job_created_at,
            current_etag=etag,
        )

    @staticmethod
    def _item(client: _FakeConditionalS3) -> dict:
        payload = json.loads(client._body)
        return payload["completion_items"][_item_key_fn("key-a", "v1")]

    @pytest.mark.parametrize(
        ("task_status", "expected_outcome"),
        [("succeeded", "COMPLETE"), ("failed", "FAILED"), ("unknown", "UNKNOWN")],
    )
    def test_report_rows_persist_each_outcome_mapping(
        self, task_status: str, expected_outcome: str
    ) -> None:
        client = self._client()

        self._merge(
            client,
            job_id=f"job-{expected_outcome.lower()}",
            job_created_at=self._REPORT_T0,
            report_created_at=self._REPORT_T1,
            task_status=task_status,
        )

        item = self._item(client)
        assert item["state"] == "RESOLVED"
        assert item["replication_outcome"] == expected_outcome
        assert item["resolution_method"] == "bops_completion_report"
        assert item["resolved_at"] == self._REPORT_T1.isoformat()

    def test_failed_conditional_write_retains_prior_items_and_processed_ids(self) -> None:
        payload = _payload_with_completion_items({}, processed_job_ids={"job-old"})
        client = self._client(payload)
        client.put_object = MagicMock(side_effect=_client_error("PreconditionFailed"))
        before = client._body

        with pytest.raises(ConditionalWriteError):
            self._merge(
                client,
                job_id="job-new",
                job_created_at=self._REPORT_T0,
                report_created_at=self._REPORT_T1,
            )

        assert client._body == before
        persisted = json.loads(client._body)
        assert persisted["completion_processed_job_ids"] == ["job-old"]
        assert persisted["completion_items"] == {}

    @pytest.mark.parametrize(
        ("first_status", "second_status", "expected_outcome"),
        [("failed", "succeeded", "COMPLETE"), ("succeeded", "failed", "FAILED")],
        ids=["newer-success-replaces-failure", "newer-failure-replaces-success"],
    )
    def test_newer_report_replaces_prior_outcome(
        self, first_status: str, second_status: str, expected_outcome: str
    ) -> None:
        client = self._client()
        first_etag = self._merge(
            client,
            job_id="job-first",
            job_created_at=self._REPORT_T0,
            report_created_at=self._REPORT_T0,
            task_status=first_status,
        )

        self._merge(
            client,
            job_id="job-second",
            job_created_at=self._REPORT_T1,
            report_created_at=self._REPORT_T1,
            task_status=second_status,
            etag=first_etag,
        )

        item = self._item(client)
        assert item["configs"]["cfg-1"]["job_id"] == "job-second"
        assert item["replication_outcome"] == expected_outcome
        assert item["resolved_at"] == self._REPORT_T1.isoformat()

    def test_older_report_arriving_last_records_job_without_regressing_outcome(self) -> None:
        client = self._client()
        latest_etag = self._merge(
            client,
            job_id="job-new",
            job_created_at=self._REPORT_T1,
            report_created_at=self._REPORT_T1,
            task_status="failed",
        )

        self._merge(
            client,
            job_id="job-old",
            job_created_at=self._REPORT_T0,
            report_created_at=self._REPORT_T0,
            task_status="succeeded",
            etag=latest_etag,
        )

        item = self._item(client)
        payload = json.loads(client._body)
        assert item["configs"]["cfg-1"]["job_id"] == "job-new"
        assert item["replication_outcome"] == "FAILED"
        assert item["resolved_at"] == self._REPORT_T1.isoformat()
        assert set(payload["completion_processed_job_ids"]) == {"job-new", "job-old"}

    def test_equal_creation_time_uses_job_id_as_deterministic_tie_break(self) -> None:
        client = self._client()
        first_etag = self._merge(
            client,
            job_id="job-a",
            job_created_at=self._REPORT_T0,
            report_created_at=self._REPORT_T0,
            task_status="failed",
        )

        self._merge(
            client,
            job_id="job-z",
            job_created_at=self._REPORT_T0,
            report_created_at=self._REPORT_T1,
            task_status="succeeded",
            etag=first_etag,
        )

        item = self._item(client)
        assert item["configs"]["cfg-1"]["job_id"] == "job-z"
        assert item["replication_outcome"] == "COMPLETE"
        assert item["resolved_at"] == self._REPORT_T1.isoformat()

    def test_empty_zero_task_report_records_only_the_processed_job_id(self) -> None:
        client = self._client()

        StateStore().merge_completion_report(
            client,
            _STATE_BUCKET,
            _SRC_BUCKET,
            report=BopsCompletionReport(created_at=self._REPORT_T0, entries=()),
            replication_config_id="cfg-1",
            job_id="job-zero-tasks",
            job_created_at=self._REPORT_T0,
            current_etag=None,
        )

        payload = json.loads(client._body)
        assert payload["completion_items"] == {}
        assert payload["completion_processed_job_ids"] == ["job-zero-tasks"]


# ---------------------------------------------------------------------------
# Legacy completion-state migration — task 4.2 (Requirements 4.1–4.3)
# ---------------------------------------------------------------------------


class TestLegacyCompletionOutcomeTolerance:
    """A 1.0.1 state object stays readable, and nothing rewrites it.

    An earlier design migrated legacy 1.0.1 outcomes to ``RESOLVED/UNKNOWN``
    through ``migrate_legacy_completion_items``. That mutation could never
    succeed against an existing state object, and its failure branch skipped the
    affected bucket's whole publish phase on every interval. Upgrading is now a
    reinstall, so the migration is deleted rather than repaired
    (design.md Decision 5). What remains required is that deserialization
    tolerate legacy values, so a rollback to 1.0.1 or a hand-inspected state
    object does not raise.

    **Validates: Requirements 4.1, 4.2, 4.6**
    """

    @staticmethod
    def _item(*, object_key: str, state: CompletionState, outcome: str | None) -> TrackedObject:
        return TrackedObject(
            source_bucket=_SRC_BUCKET,
            object_key=object_key,
            version_id="v1",
            configs={"cfg-1": _make_config_context()},
            state=state,
            resolved_at=_JOB_T0 if state is CompletionState.RESOLVED else None,
            resolution_method="source_status_header" if state is CompletionState.RESOLVED else None,
            replication_outcome=outcome,
            tagged_at=_JOB_T0,
            last_modified=_JOB_T1,
            matched_rules=frozenset({"rule-a"}),
            destinations=frozenset({"destination-a"}),
        )

    def test_every_legacy_shape_deserializes_without_raising(self) -> None:
        items = {
            _item_key_fn("lifecycle-pending", "v1"): self._item(
                object_key="lifecycle-pending",
                state=CompletionState.PENDING,
                outcome=None,
            ),
            _item_key_fn("outcome-pending", "v1"): self._item(
                object_key="outcome-pending",
                state=CompletionState.RESOLVED,
                outcome="PENDING",
            ),
            _item_key_fn("outcome-gone", "v1"): self._item(
                object_key="outcome-gone",
                state=CompletionState.RESOLVED,
                outcome="GONE",
            ),
            _item_key_fn("outcome-expired", "v1"): self._item(
                object_key="outcome-expired",
                state=CompletionState.RESOLVED,
                outcome="EXPIRED",
            ),
        }
        payload = _payload_with_completion_items(items, processed_job_ids={"job-1"})
        client = _mock_s3_get_raw_payload(payload)

        read_back = StateStore().get_all_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET
        )

        assert set(read_back) == set(items)
        assert read_back[_item_key_fn("outcome-gone", "v1")].replication_outcome == "GONE"
        assert read_back[_item_key_fn("outcome-expired", "v1")].replication_outcome == "EXPIRED"
        assert read_back[_item_key_fn("outcome-pending", "v1")].replication_outcome == "PENDING"
        assert (
            read_back[_item_key_fn("lifecycle-pending", "v1")].state
            is CompletionState.PENDING
        )

    def test_reading_legacy_state_writes_nothing(self) -> None:
        """Deserialization neither mutates lifecycle state nor persists."""
        items = {
            _item_key_fn("outcome-gone", "v1"): self._item(
                object_key="outcome-gone",
                state=CompletionState.RESOLVED,
                outcome="GONE",
            ),
        }
        payload = _payload_with_completion_items(items)
        client = _mock_s3_get_raw_payload(payload)

        StateStore().get_all_completion_items(client, _STATE_BUCKET, _SRC_BUCKET)

        client.put_object.assert_not_called()
        assert json.loads(client.get_object.return_value["Body"].read()) == payload

    def test_state_store_exposes_no_migration_method(self) -> None:
        """The deleted path cannot return unnoticed."""
        assert not hasattr(StateStore, "migrate_legacy_completion_items")
        assert not any(
            "migrate" in name for name in dir(StateStore) if not name.startswith("__")
        ), [name for name in dir(StateStore) if "migrate" in name]


# ---------------------------------------------------------------------------
# Per-job keying, pruning, and the record ceiling — bounded-concurrent-jobs
# task 1.5 (Requirements 1.1, 1.2, 3.1, 3.2, 3.3, 7.1)
# ---------------------------------------------------------------------------


class _RecordFixtures:
    """Shared builders for the per-job keying tests below."""

    @staticmethod
    def _record(
        job_id: str,
        *,
        submitted_at: datetime | None = None,
        report_diagnosed: bool = False,
        recovery_scored: bool = False,
    ) -> SubmissionRecord:
        return SubmissionRecord(
            replication_config_id=_SRC_BUCKET,
            source_bucket=_SRC_BUCKET,
            job_id=job_id,
            manifest_key=f"manifests/{_SRC_BUCKET}/{job_id}.csv",
            submitted_at=submitted_at or _NOW,
            status=SubmissionStatus.SUBMITTED,
            watermark_low=_WM_042,
            watermark_high=_WM_077,
            report_diagnosed=report_diagnosed,
            recovery_scored=recovery_scored,
        )

    @classmethod
    def _payload(
        cls,
        records: dict[str, SubmissionRecord],
        processed_job_ids: list[str] | None = None,
    ) -> dict:
        from src.core.checkpoint_serializer import serialize_submission_record

        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = {
            key: serialize_submission_record(rec) for key, rec in records.items()
        }
        if processed_job_ids is not None:
            payload["completion_processed_job_ids"] = processed_job_ids
        return payload

    @staticmethod
    def _written_records(client) -> dict:
        return json.loads(client.put_object.call_args[1]["Body"])["submission_records"]


class TestRecordSubmissionMergesWithoutDisturbingSiblings(_RecordFixtures):
    """The fix for the data loss: a second submission must not displace the first.

    Overwriting discarded the running job's id, and with it the read of its
    completion report, the report-missing check that would have noticed, and any
    watermark_low to roll back to had it later failed (Requirement 1.1).
    """

    def test_adding_preserves_the_other_jobs_entry(self):
        client = _mock_s3_get_raw_payload(
            self._payload({"job-running": self._record("job-running")})
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG
        )

        records = self._written_records(client)
        assert set(records) == {"job-running", "job-new"}
        assert records["job-running"]["job_id"] == "job-running"

    def test_re_submitting_the_same_job_id_updates_in_place(self):
        client = _mock_s3_get_raw_payload(
            self._payload({"job-a": self._record("job-a")})
        )

        updated = SubmissionRecord(
            replication_config_id=_SRC_BUCKET,
            source_bucket=_SRC_BUCKET,
            job_id="job-a",
            manifest_key="manifests/updated.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
            consecutive_failures=7,
        )
        StateStore().record_submission(client, _STATE_BUCKET, updated, _ETAG)

        records = self._written_records(client)
        assert set(records) == {"job-a"}
        assert records["job-a"]["manifest_key"] == "manifests/updated.csv"
        assert records["job-a"]["consecutive_failures"] == 7

    def test_three_concurrent_jobs_all_persist(self):
        client = _mock_s3_get_raw_payload(
            self._payload({
                "job-1": self._record("job-1"),
                "job-2": self._record("job-2"),
            })
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-3"), _ETAG
        )

        assert set(self._written_records(client)) == {"job-1", "job-2", "job-3"}


class TestRecordSubmissionPrunesSettledRecords(_RecordFixtures):
    """Settled means terminal AND diagnosed AND (merged OR tracking disabled)."""

    def test_a_settled_record_is_pruned(self):
        client = _mock_s3_get_raw_payload(
            self._payload(
                {"job-done": self._record("job-done", report_diagnosed=True)},
                processed_job_ids=["job-done"],
            )
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids={"job-done"},
            completion_tracking_enabled=True,
        )

        assert set(self._written_records(client)) == {"job-new"}

    def test_a_terminal_record_whose_report_is_unread_is_kept(self):
        """This is exactly the record check_report_handler needs to raise the
        missing-or-unconsumed report alert, so pruning on terminal alone would
        delete the evidence the alert exists for (Requirement 3.2)."""
        client = _mock_s3_get_raw_payload(
            self._payload(
                {"job-done": self._record("job-done", report_diagnosed=True)},
                processed_job_ids=[],
            )
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids={"job-done"},
            completion_tracking_enabled=True,
        )

        assert set(self._written_records(client)) == {"job-done", "job-new"}

    def test_an_undiagnosed_record_is_kept(self):
        client = _mock_s3_get_raw_payload(
            self._payload(
                {"job-done": self._record("job-done", report_diagnosed=False)},
                processed_job_ids=["job-done"],
            )
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids={"job-done"},
        )

        assert "job-done" in self._written_records(client)

    def test_a_non_terminal_record_is_kept_even_if_somehow_processed(self):
        client = _mock_s3_get_raw_payload(
            self._payload(
                {"job-running": self._record("job-running", report_diagnosed=True)},
                processed_job_ids=["job-running"],
            )
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids=set(),
        )

        assert "job-running" in self._written_records(client)

    def test_with_tracking_disabled_the_processed_set_is_not_required(self):
        """Nothing populates completion_processed_job_ids when tracking is off,
        so requiring it would mean records never prune (Requirement 3.1)."""
        client = _mock_s3_get_raw_payload(
            self._payload(
                {"job-done": self._record("job-done", report_diagnosed=True)},
                processed_job_ids=[],
            )
        )

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids={"job-done"},
            completion_tracking_enabled=False,
        )

        assert set(self._written_records(client)) == {"job-new"}

    def test_the_record_being_written_is_never_pruned(self):
        """It was submitted moments ago, so it cannot have settled — even if the
        caller passes its id in terminal_job_ids by mistake."""
        client = _mock_s3_get_raw_payload(self._payload({}))

        StateStore().record_submission(
            client, _STATE_BUCKET,
            self._record("job-new", report_diagnosed=True), _ETAG,
            terminal_job_ids={"job-new"},
            completion_tracking_enabled=False,
        )

        assert set(self._written_records(client)) == {"job-new"}


class TestRecordSubmissionCeiling(_RecordFixtures):
    """The backstop against a record that can never settle (Requirement 3.3)."""

    def _many(self, count: int) -> dict[str, SubmissionRecord]:
        return {
            f"job-{index:03d}": self._record(
                f"job-{index:03d}",
                submitted_at=_NOW + timedelta(minutes=index),
            )
            for index in range(count)
        }

    def test_the_ceiling_is_the_limit_plus_twenty(self):
        from src.adapters.state_store import submission_record_ceiling

        assert submission_record_ceiling(3) == 23
        assert submission_record_ceiling(1) == 21

    def test_no_eviction_at_the_ceiling(self):
        ceiling = 23  # max_concurrent_jobs=3
        client = _mock_s3_get_raw_payload(self._payload(self._many(ceiling - 1)))

        with patch("src.adapters.state_store.observability.emit") as emit:
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                max_concurrent_jobs=3,
            )

        assert len(self._written_records(client)) == ceiling
        assert emit.call_count == 0

    def test_above_the_ceiling_the_oldest_is_evicted(self):
        client = _mock_s3_get_raw_payload(self._payload(self._many(23)))

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                max_concurrent_jobs=3,
            )

        records = self._written_records(client)
        assert len(records) == 23
        assert "job-000" not in records  # oldest by submitted_at
        assert "job-001" in records
        assert "job-new" in records

    def test_each_eviction_emits_an_error_naming_the_job(self):
        """An error, not an audit entry: this discards tracking state for a job
        whose outcome is unknown, which is a loss rather than a decision."""
        client = _mock_s3_get_raw_payload(self._payload(self._many(25)))

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                max_concurrent_jobs=3,
            )

        errors = [e for e in emitted if e.get("event") == "error"]
        assert len(errors) == 3  # 25 + 1 written, down to 23
        assert len(self._written_records(client)) == 23
        named = " ".join(e["cause"] for e in errors)
        for job_id in ("job-000", "job-001", "job-002"):
            assert job_id in named
        assert all(e["bucket"] == _SRC_BUCKET for e in errors)

    def test_the_record_being_written_survives_eviction(self):
        client = _mock_s3_get_raw_payload(self._payload(self._many(40)))

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET,
                # Oldest of all by submitted_at, and still must survive: it is
                # the job this write exists to record.
                self._record("job-new", submitted_at=_NOW - timedelta(days=30)),
                _ETAG,
                max_concurrent_jobs=3,
            )

        assert "job-new" in self._written_records(client)

    def test_a_naive_submitted_at_does_not_break_the_ordering(self):
        """A hand-edited state object can carry one, and raising here would fail
        the write that persists a submission record."""
        records = self._many(23)
        records["job-naive"] = SubmissionRecord(
            replication_config_id=_SRC_BUCKET,
            source_bucket=_SRC_BUCKET,
            job_id="job-naive",
            manifest_key="manifests/naive.csv",
            submitted_at=datetime(2020, 1, 1),  # noqa: DTZ001 — the point of the test
            status=SubmissionStatus.SUBMITTED,
        )
        client = _mock_s3_get_raw_payload(self._payload(records))

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                max_concurrent_jobs=3,
            )

        records_written = self._written_records(client)
        assert len(records_written) == 23
        assert "job-naive" not in records_written  # oldest, so evicted first

    def test_no_ceiling_when_the_limit_is_not_supplied(self):
        client = _mock_s3_get_raw_payload(self._payload(self._many(40)))

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
        )

        assert len(self._written_records(client)) == 41


# ---------------------------------------------------------------------------
# One bad record must not cost the rest
#
# record_submission now READS the stored records in order to merge into them.
# Before per-job keying it overwrote them unread, so a corrupt entry healed
# itself on the next write. Now an unisolated deserialization failure would make
# every read and every write raise for that bucket, forever, leaving it
# submitting jobs it never records.
# ---------------------------------------------------------------------------


class TestMalformedRecordIsolation(_RecordFixtures):
    def _payload_with_bad_entry(self, bad: object) -> dict:
        from src.core.checkpoint_serializer import serialize_submission_record

        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = {
            "job-good": serialize_submission_record(self._record("job-good")),
            "job-bad": bad,
        }
        return payload

    def test_an_unparseable_status_does_not_lose_the_sibling(self):
        payload = self._payload_with_bad_entry({
            "replication_config_id": _SRC_BUCKET,
            "source_bucket": _SRC_BUCKET,
            "job_id": "job-bad",
            "manifest_key": "manifests/bad.csv",
            "submitted_at": _NOW.isoformat(),
            "status": "NOT_A_REAL_STATUS",
        })
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            result = StateStore().get_submission_records(
                client, _STATE_BUCKET, _SRC_BUCKET
            )

        assert set(result) == {"job-good"}

    def test_a_missing_required_key_does_not_lose_the_sibling(self):
        payload = self._payload_with_bad_entry({"job_id": "job-bad"})
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            result = StateStore().get_submission_records(
                client, _STATE_BUCKET, _SRC_BUCKET
            )

        assert set(result) == {"job-good"}

    def test_an_unparseable_submitted_at_does_not_lose_the_sibling(self):
        payload = self._payload_with_bad_entry({
            "replication_config_id": _SRC_BUCKET,
            "source_bucket": _SRC_BUCKET,
            "job_id": "job-bad",
            "manifest_key": "manifests/bad.csv",
            "submitted_at": "not-a-timestamp",
            "status": SubmissionStatus.SUBMITTED.value,
        })
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            result = StateStore().get_submission_records(
                client, _STATE_BUCKET, _SRC_BUCKET
            )

        assert set(result) == {"job-good"}

    def test_record_submission_still_writes_alongside_a_bad_entry(self):
        """The regression that matters: without isolation the just-submitted job
        would never be recorded, every run."""
        payload = self._payload_with_bad_entry({"job_id": "job-bad"})
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG
            )

        records = self._written_records(client)
        assert set(records) == {"job-good", "job-new"}
        # The bad entry is gone from the object, so it stops costing an error.
        assert "job-bad" not in records

    def test_dropping_a_record_is_reported_as_an_error(self):
        """Every other path in this module that discards persisted state reports
        it, because a silent drop is how tracking is lost unnoticed."""
        payload = self._payload_with_bad_entry({"job_id": "job-bad"})
        client = _mock_s3_get_raw_payload(payload)

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            StateStore().get_submission_records(client, _STATE_BUCKET, _SRC_BUCKET)

        errors = [entry for entry in emitted if entry.get("event") == "error"]
        assert len(errors) == 1
        assert "job-bad" in errors[0]["cause"]
        assert errors[0]["bucket"] == _SRC_BUCKET

    def test_dropping_an_empty_job_id_is_reported_as_an_error(self):
        from src.core.checkpoint_serializer import serialize_submission_record

        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = {
            _SRC_BUCKET: serialize_submission_record(self._record("")),
        }
        client = _mock_s3_get_raw_payload(payload)

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            result = StateStore().get_submission_records(
                client, _STATE_BUCKET, _SRC_BUCKET
            )

        assert result == {}
        errors = [entry for entry in emitted if entry.get("event") == "error"]
        assert len(errors) == 1
        assert "no job_id" in errors[0]["cause"]

    def test_a_non_dict_submission_records_yields_nothing_and_reports(self):
        payload = json.loads(serialize(_make_state()))
        payload["submission_records"] = ["not", "a", "dict"]
        client = _mock_s3_get_raw_payload(payload)

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            result = StateStore().get_submission_records(
                client, _STATE_BUCKET, _SRC_BUCKET
            )

        assert result == {}
        assert [e for e in emitted if e.get("event") == "error"]


class TestEvictionPrefersTerminalRecords(_RecordFixtures):
    """Age alone would evict the longest-running job first — on a
    bandwidth-bound bucket, the one still replicating. Losing its record
    discards the watermark_low its rollback needs and the report its objects
    would be counted from, which is the silent loss this design removes.
    """

    def _payload_at_ceiling(self) -> dict:
        # job-000 is the oldest and is still running; job-001..022 are newer and
        # terminal. Age ordering would take job-000 first.
        records = {
            f"job-{index:03d}": self._record(
                f"job-{index:03d}",
                submitted_at=_NOW + timedelta(minutes=index),
            )
            for index in range(23)
        }
        return self._payload(records)

    def _terminal(self) -> set[str]:
        return {f"job-{index:03d}" for index in range(1, 23)}

    def test_a_terminal_record_is_evicted_before_a_running_one(self):
        client = _mock_s3_get_raw_payload(self._payload_at_ceiling())

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                terminal_job_ids=self._terminal(),
                max_concurrent_jobs=3,
            )

        records = self._written_records(client)
        assert len(records) == 23
        assert "job-000" in records, "the running job's record must survive"
        assert "job-001" not in records, "the oldest terminal record goes first"

    def test_age_still_orders_within_the_terminal_records(self):
        client = _mock_s3_get_raw_payload(self._payload_at_ceiling())

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                terminal_job_ids=self._terminal(),
                max_concurrent_jobs=1,  # ceiling 21, so 3 must go
            )

        records = self._written_records(client)
        assert len(records) == 21
        assert {"job-001", "job-002", "job-003"}.isdisjoint(records)
        assert "job-000" in records

    def test_a_running_record_is_evicted_only_when_nothing_else_is_left(self):
        """Non-vacuous: the preference is an ordering, not an exemption. The
        ceiling has to stay a hard bound or the state object grows unchecked."""
        client = _mock_s3_get_raw_payload(self._payload_at_ceiling())

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                terminal_job_ids=set(),  # nothing terminal to prefer
                max_concurrent_jobs=3,
            )

        records = self._written_records(client)
        assert len(records) == 23
        assert "job-000" not in records


class TestAlertSuppressionIsClearedWithTheRecord(_RecordFixtures):
    """Suppression is keyed by job_id and is otherwise cleared only when a report
    is finally merged. An entry is written because a report is missing, which need
    never recover, so once the record goes nothing could ever match the entry.
    """

    def test_pruning_a_record_clears_its_suppression_entry(self):
        payload = self._payload(
            {"job-done": self._record("job-done", report_diagnosed=True)},
            processed_job_ids=["job-done"],
        )
        payload["completion_report_alerted_configs"] = ["job-done", "job-other"]
        client = _mock_s3_get_raw_payload(payload)

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids={"job-done"},
        )

        written = json.loads(client.put_object.call_args[1]["Body"])
        assert written["completion_report_alerted_configs"] == ["job-other"]

    def test_eviction_clears_its_suppression_entry(self):
        records = {
            f"job-{index:03d}": self._record(
                f"job-{index:03d}", submitted_at=_NOW + timedelta(minutes=index)
            )
            for index in range(23)
        }
        payload = self._payload(records)
        payload["completion_report_alerted_configs"] = ["job-000", "job-022"]
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().record_submission(
                client, _STATE_BUCKET, self._record("job-new"), _ETAG,
                max_concurrent_jobs=3,
            )

        written = json.loads(client.put_object.call_args[1]["Body"])
        assert written["completion_report_alerted_configs"] == ["job-022"]

    def test_a_retained_records_entry_is_left_alone(self):
        """Non-vacuous: suppression must still work for a record that is still
        there, or the missing-report alert repeats every five minutes."""
        payload = self._payload(
            {"job-unread": self._record("job-unread", report_diagnosed=True)},
            processed_job_ids=[],
        )
        payload["completion_report_alerted_configs"] = ["job-unread"]
        client = _mock_s3_get_raw_payload(payload)

        StateStore().record_submission(
            client, _STATE_BUCKET, self._record("job-new"), _ETAG,
            terminal_job_ids={"job-unread"},
        )

        written = json.loads(client.put_object.call_args[1]["Body"])
        assert written["completion_report_alerted_configs"] == ["job-unread"]

    def test_disabling_a_bucket_clears_every_suppression_entry(self):
        payload = self._payload({
            "job-a": self._record("job-a"),
            "job-b": self._record("job-b"),
        })
        payload["completion_report_alerted_configs"] = ["job-a", "job-b"]
        client = _mock_s3_get_raw_payload(payload)

        StateStore().disable_bucket(
            client, _STATE_BUCKET, _SRC_BUCKET,
            reason="breaker tripped", now=_NOW, current_etag=_ETAG,
        )

        written = json.loads(client.put_object.call_args[1]["Body"])
        assert written["submission_records"] == {}
        assert written["completion_report_alerted_configs"] == []


# ---------------------------------------------------------------------------
# mark_report_diagnosed sets either per-job flag, in one write
# ---------------------------------------------------------------------------


class TestMarkJobFlags(_RecordFixtures):
    def _payload_with(self, **flags) -> dict:
        record = self._record("job-abc-123", **flags)
        return self._payload({"job-abc-123": record})

    def _written_record(self, client) -> dict:
        return self._written_records(client)["job-abc-123"]

    def test_recovery_scored_can_be_set_on_its_own(self):
        """The case an unreadable report produces: scored, not diagnosed."""
        client = _mock_s3_get_raw_payload(self._payload_with())

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123",
            current_etag=_ETAG,
            report_diagnosed=False,
            recovery_scored=True,
        )

        written = self._written_record(client)
        assert written["recovery_scored"] is True
        assert written["report_diagnosed"] is False

    def test_both_flags_can_be_set_in_one_write(self):
        client = _mock_s3_get_raw_payload(self._payload_with())

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123",
            current_etag=_ETAG,
            report_diagnosed=True,
            recovery_scored=True,
        )

        assert client.put_object.call_count == 1
        written = self._written_record(client)
        assert written["recovery_scored"] is True
        assert written["report_diagnosed"] is True

    def test_a_false_argument_never_clears_a_set_flag(self):
        """Both flags record something that happened once and cannot un-happen, so
        no path should be able to clear one."""
        client = _mock_s3_get_raw_payload(
            self._payload_with(report_diagnosed=True, recovery_scored=True)
        )

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123",
            current_etag=_ETAG,
            report_diagnosed=False,
            recovery_scored=False,
        )

        written = self._written_record(client)
        assert written["recovery_scored"] is True
        assert written["report_diagnosed"] is True

    def test_the_default_still_sets_report_diagnosed_only(self):
        """Keeps the pre-existing single-argument call sites meaning what they did."""
        client = _mock_s3_get_raw_payload(self._payload_with())

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123", current_etag=_ETAG,
        )

        written = self._written_record(client)
        assert written["report_diagnosed"] is True
        assert written["recovery_scored"] is False

    def test_a_sibling_job_is_untouched(self):
        payload = self._payload({
            "job-abc-123": self._record("job-abc-123"),
            "job-sibling": self._record("job-sibling"),
        })
        client = _mock_s3_get_raw_payload(payload)

        StateStore().mark_report_diagnosed(
            client, _STATE_BUCKET, _SRC_BUCKET, "job-abc-123",
            current_etag=_ETAG, recovery_scored=True,
        )

        records = self._written_records(client)
        assert records["job-abc-123"]["recovery_scored"] is True
        assert records["job-sibling"]["recovery_scored"] is False


class TestCompletionSideMapCeiling:
    """The three per-object completion maps are bounded (Requirement 8.1).

    Before this, none of them had a ceiling, TTL, or eviction, and the only
    prune path (``delete_completion_items``) is reached from the publish phase,
    which returns early when completion tracking is unconfigured — the default.
    A default stack therefore grew ``completion_timestamps`` and
    ``completion_routing`` for the life of the stack, inside the same object
    that carries the checkpoint, the lease, and the submission records.
    """

    @staticmethod
    def _written(client) -> dict:
        return json.loads(client.put_object.call_args.kwargs["Body"])

    @staticmethod
    def _ts_entries(count: int, *, start: int = 0) -> dict[str, dict]:
        return {
            _item_key_fn(f"key-{index:06d}", "v1"): {"tagged_at": _NOW.isoformat()}
            for index in range(start, start + count)
        }

    @staticmethod
    def _routing_entries(count: int, *, start: int = 0) -> dict[str, dict]:
        return {
            _item_key_fn(f"key-{index:06d}", "v1"): {"matched_rules": ["rule-a"]}
            for index in range(start, start + count)
        }

    # Beyond any key the pre-existing-entry helpers generate, so a "new" key
    # is genuinely new rather than a no-op merge into an entry already stored.
    _NEW_BASE = CEILING + 1000

    def _store_timestamps(self, client, *, new_keys: int = 1, routing: bool = False):
        timestamps = {
            (f"key-{self._NEW_BASE + index:06d}", "v1"): (_NOW, None)
            for index in range(new_keys)
        }
        routing_arg = (
            {ident: (["rule-new"], ["dest-new"]) for ident in timestamps}
            if routing
            else None
        )
        return StateStore().store_completion_timestamps(
            client, _STATE_BUCKET, _SRC_BUCKET, timestamps, _ETAG, routing=routing_arg,
        )

    # -- the ceiling itself ------------------------------------------------

    def test_the_ceiling_is_the_same_for_all_three_maps(self):
        """One number for an operator to reason about; per-map so a burst of
        enrichment writes cannot evict the items that carry report outcomes."""
        assert CEILING == 10_000

    def test_no_eviction_at_the_ceiling(self):
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = self._ts_entries(CEILING - 1)
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit") as emit:
            self._store_timestamps(client, new_keys=1)

        assert len(self._written(client)["completion_timestamps"]) == CEILING
        assert emit.call_count == 0

    def test_above_the_ceiling_the_oldest_timestamps_are_evicted(self):
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = self._ts_entries(CEILING)
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            self._store_timestamps(client, new_keys=1)

        stored = self._written(client)["completion_timestamps"]
        assert len(stored) == CEILING
        assert _item_key_fn("key-000000", "v1") not in stored  # oldest by arrival
        assert _item_key_fn("key-000001", "v1") in stored

    def test_above_the_ceiling_the_oldest_routing_entries_are_evicted(self):
        payload = _payload_with_completion_items({})
        payload["completion_routing"] = self._routing_entries(CEILING)
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            self._store_timestamps(client, new_keys=1, routing=True)

        stored = self._written(client)["completion_routing"]
        assert len(stored) == CEILING
        assert _item_key_fn("key-000000", "v1") not in stored
        assert _item_key_fn("key-000001", "v1") in stored

    def test_routing_is_capped_even_when_this_call_supplies_none(self):
        """A caller that stops supplying routing must not leave an
        already-oversized map uncapped, since nothing else would trim it."""
        payload = _payload_with_completion_items({})
        payload["completion_routing"] = self._routing_entries(CEILING + 5)
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            self._store_timestamps(client, new_keys=1, routing=False)

        assert len(self._written(client)["completion_routing"]) == CEILING

    def test_an_absent_routing_map_stays_absent(self):
        payload = _payload_with_completion_items({})
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            self._store_timestamps(client, new_keys=1, routing=False)

        assert "completion_routing" not in self._written(client)

    def test_the_entries_written_by_this_call_survive_eviction(self):
        """The write cannot discard its own work and return an ETag implying it
        persisted — the same protection ``record_submission`` gives its record."""
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = self._ts_entries(CEILING + 50)
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            self._store_timestamps(client, new_keys=3)

        stored = self._written(client)["completion_timestamps"]
        assert len(stored) == CEILING
        for index in range(3):
            assert _item_key_fn(f"key-{self._NEW_BASE + index:06d}", "v1") in stored

    # -- the eviction log --------------------------------------------------

    def test_each_eviction_emits_an_error_with_a_redacted_key(self):
        """An error, not an audit entry: eviction discards tracking state, so it
        is a loss rather than a decision. The key is redacted because an item
        key is an object key."""
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = self._ts_entries(CEILING + 2)
        client = _mock_s3_get_raw_payload(payload)

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            self._store_timestamps(client, new_keys=1)

        errors = [e for e in emitted if e.get("event") == "error"]
        assert len(errors) == 3  # CEILING + 2 stored, 1 written, down to CEILING
        assert all(e["bucket"] == _SRC_BUCKET for e in errors)
        assert all("completion_timestamps" in e["cause"] for e in errors)
        joined = " ".join(e["cause"] for e in errors)
        assert "key-000000" not in joined
        for index in range(3):
            fingerprint = redact_object_key(_item_key_fn(f"key-{index:06d}", "v1"))
            assert fingerprint in joined

    def test_a_write_larger_than_the_ceiling_reports_the_overshoot(self):
        """A single write protecting more entries than the ceiling leaves the map
        above it, since a write never discards its own entries. That breach is
        reported rather than silent (diff scan scan-f1927e8a, f-a26a46fc)."""
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = self._ts_entries(5)
        client = _mock_s3_get_raw_payload(payload)

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            self._store_timestamps(client, new_keys=CEILING + 10)

        stored = self._written(client)["completion_timestamps"]
        assert len(stored) == CEILING + 10, "the write's own entries all persist"
        overshoot = [
            e for e in emitted
            if e.get("event") == "error"
            and "above the" in e["cause"]
            and "completion_timestamps" in e["cause"]
        ]
        assert len(overshoot) == 1
        assert str(CEILING + 10) in overshoot[0]["cause"]
        assert overshoot[0]["bucket"] == _SRC_BUCKET

    def test_no_overshoot_report_when_the_ceiling_is_reached(self):
        """The check is not vacuous: an ordinary eviction down to the ceiling
        reports evictions and no overshoot."""
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = self._ts_entries(CEILING + 2)
        client = _mock_s3_get_raw_payload(payload)

        emitted: list = []
        with patch(
            "src.adapters.state_store.observability.emit",
            side_effect=emitted.append,
        ):
            self._store_timestamps(client, new_keys=1)

        assert not [
            e for e in emitted
            if e.get("event") == "error" and "above the" in e["cause"]
        ]

    # -- completion_items, on the path that writes it ----------------------

    def test_completion_items_is_capped_on_merge(self):
        items = {
            _item_key_fn(f"key-{index:06d}", "v1"): _make_item(
                object_key=f"key-{index:06d}", version_id="v1",
            )
            for index in range(CEILING)
        }
        payload = _payload_with_completion_items(items)
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().merge_completion_report(
                client, _STATE_BUCKET, _SRC_BUCKET,
                report=BopsCompletionReport(
                    created_at=_NOW,
                    entries=(
                        ManifestEntry(_SRC_BUCKET, "key-new", "v1",
                                      task_status="succeeded"),
                    ),
                ),
                replication_config_id="cfg-1",
                job_id="job-report",
                job_created_at=_NOW,
                current_etag=_ETAG,
            )

        stored = self._written(client)["completion_items"]
        assert len(stored) == CEILING
        # The item this report resolved is protected; the oldest goes instead.
        assert _item_key_fn("key-new", "v1") in stored
        assert _item_key_fn("key-000000", "v1") not in stored

    def test_evicting_an_item_takes_its_enrichment_entries_with_it(self):
        """An evicted item can never be published, so its timestamps and
        routing entries are orphans by construction."""
        oldest = _item_key_fn("key-000000", "v1")
        items = {
            _item_key_fn(f"key-{index:06d}", "v1"): _make_item(
                object_key=f"key-{index:06d}", version_id="v1",
            )
            for index in range(CEILING)
        }
        payload = _payload_with_completion_items(items)
        payload["completion_timestamps"] = {oldest: {"tagged_at": _NOW.isoformat()}}
        payload["completion_routing"] = {oldest: {"matched_rules": ["rule-a"]}}
        client = _mock_s3_get_raw_payload(payload)

        with patch("src.adapters.state_store.observability.emit"):
            StateStore().merge_completion_report(
                client, _STATE_BUCKET, _SRC_BUCKET,
                report=BopsCompletionReport(
                    created_at=_NOW,
                    entries=(
                        ManifestEntry(_SRC_BUCKET, "key-new", "v1",
                                      task_status="succeeded"),
                    ),
                ),
                replication_config_id="cfg-1",
                job_id="job-report",
                job_created_at=_NOW,
                current_etag=_ETAG,
            )

        written = self._written(client)
        assert oldest not in written["completion_items"]
        assert oldest not in written.get("completion_timestamps", {})
        assert oldest not in written.get("completion_routing", {})

class TestNullVersionSideMapKey:
    """A null version keys the enrichment maps the same on write and read (Req 8.2).

    ``ManifestGenerator.get_timestamps``/``get_routing`` key their transport
    dicts on ``version_id or ""``, while ``merge_completion_report`` reads
    ``version_id`` from the BOPS report and holds ``None``.
    ``completion_serializer.item_key`` treats the two as distinct identities, so
    an unversioned object's entry was written under ``"a.txt\\x00"`` and looked
    up under ``"a.txt\\x00\\x01"`` — neither readable nor prunable.

    Design R6, option 1: the writer emits the ``None`` form and the reader looks
    up both, so entries already in a deployed state object stay readable rather
    than becoming orphans the moment the fix ships.
    """

    @staticmethod
    def _written(client) -> dict:
        return json.loads(client.put_object.call_args.kwargs["Body"])

    def test_the_two_key_forms_are_distinct(self):
        """The premise. If these were equal there would be no defect."""
        assert _item_key_fn("a.txt", None) == "a.txt\x00\x01"
        assert _item_key_fn("a.txt", "") == "a.txt\x00"

    def test_a_null_version_entry_is_written_under_the_none_form(self):
        client = _mock_s3_get_raw_payload(_payload_with_completion_items({}))

        StateStore().store_completion_timestamps(
            client, _STATE_BUCKET, _SRC_BUCKET,
            {("a.txt", ""): (_JOB_T0, _JOB_T1)}, _ETAG,
            routing={("a.txt", ""): (["rule-a"], ["dest-a"])},
        )

        written = self._written(client)
        assert _item_key_fn("a.txt", None) in written["completion_timestamps"]
        assert _item_key_fn("a.txt", "") not in written["completion_timestamps"]
        assert _item_key_fn("a.txt", None) in written["completion_routing"]
        assert _item_key_fn("a.txt", "") not in written["completion_routing"]

    def test_a_real_version_id_is_unaffected(self):
        client = _mock_s3_get_raw_payload(_payload_with_completion_items({}))

        StateStore().store_completion_timestamps(
            client, _STATE_BUCKET, _SRC_BUCKET,
            {("a.txt", "v1"): (_JOB_T0, _JOB_T1)}, _ETAG,
        )

        assert _item_key_fn("a.txt", "v1") in (
            self._written(client)["completion_timestamps"]
        )

    def _merge_null_version_row(self, client):
        StateStore().merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=BopsCompletionReport(
                created_at=_NOW,
                entries=(
                    ManifestEntry(_SRC_BUCKET, "a.txt", None, task_status="succeeded"),
                ),
            ),
            replication_config_id="cfg-1",
            job_id="job-report",
            job_created_at=_JOB_T1,
            current_etag=_ETAG,
        )

    def test_merge_reads_the_entry_the_writer_wrote(self):
        payload = _payload_with_completion_items({})
        current = _item_key_fn("a.txt", None)
        payload["completion_timestamps"] = {
            current: {
                "tagged_at": _JOB_T0.isoformat(),
                "last_modified": _JOB_T1.isoformat(),
            }
        }
        payload["completion_routing"] = {
            current: {"matched_rules": ["rule-a"], "destinations": ["dest-a"]}
        }
        client = _mock_s3_get_raw_payload(payload)

        self._merge_null_version_row(client)

        item = self._written(client)["completion_items"][current]
        assert item["tagged_at"] == _JOB_T0.isoformat()
        assert item["last_modified"] == _JOB_T1.isoformat()
        assert item["matched_rules"] == ["rule-a"]
        assert item["destinations"] == ["dest-a"]

    def test_merge_still_reads_a_legacy_empty_string_key(self):
        """Entries already in a deployed state object carry the old key."""
        payload = _payload_with_completion_items({})
        legacy = _item_key_fn("a.txt", "")
        payload["completion_timestamps"] = {
            legacy: {"tagged_at": _JOB_T0.isoformat()}
        }
        payload["completion_routing"] = {legacy: {"matched_rules": ["rule-legacy"]}}
        client = _mock_s3_get_raw_payload(payload)

        self._merge_null_version_row(client)

        item = self._written(client)["completion_items"][_item_key_fn("a.txt", None)]
        assert item["tagged_at"] == _JOB_T0.isoformat()
        assert item["matched_rules"] == ["rule-legacy"]

    def test_a_versioned_object_does_not_fall_back_to_the_legacy_key(self):
        """The fallback is scoped to a null version; ``"a.txt\\x00v1"`` has no
        second spelling, and a miss must stay a miss."""
        payload = _payload_with_completion_items({})
        payload["completion_timestamps"] = {
            _item_key_fn("a.txt", ""): {"tagged_at": _JOB_T0.isoformat()}
        }
        client = _mock_s3_get_raw_payload(payload)

        StateStore().merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=BopsCompletionReport(
                created_at=_NOW,
                entries=(
                    ManifestEntry(_SRC_BUCKET, "a.txt", "v1", task_status="succeeded"),
                ),
            ),
            replication_config_id="cfg-1",
            job_id="job-report",
            job_created_at=_JOB_T1,
            current_etag=_ETAG,
        )

        item = self._written(client)["completion_items"][_item_key_fn("a.txt", "v1")]
        assert "tagged_at" not in item

    def test_delete_prunes_both_key_forms(self):
        """The leak half. A legacy-keyed entry survived every prune path, so it
        sat in the state object until the Requirement 8.1 ceiling evicted it."""
        item_key = _item_key_fn("a.txt", None)
        legacy = _item_key_fn("a.txt", "")
        payload = _payload_with_completion_items(
            {item_key: _make_item(object_key="a.txt", version_id=None)}
        )
        payload["completion_timestamps"] = {legacy: {"tagged_at": _JOB_T0.isoformat()}}
        payload["completion_routing"] = {legacy: {"matched_rules": ["rule-a"]}}
        client = _mock_s3_get_raw_payload(payload)

        StateStore().delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET, [item_key], current_etag=_ETAG,
        )

        written = self._written(client)
        assert written["completion_items"] == {}
        assert written["completion_timestamps"] == {}
        assert written["completion_routing"] == {}

    def test_delete_leaves_a_sibling_objects_entries_alone(self):
        """The legacy-key variant is derived from the item key being deleted, so
        it cannot reach another object's entries."""
        payload = _payload_with_completion_items(
            {_item_key_fn("a.txt", None): _make_item(
                object_key="a.txt", version_id=None,
            )}
        )
        payload["completion_timestamps"] = {
            _item_key_fn("b.txt", ""): {"tagged_at": _JOB_T0.isoformat()},
            _item_key_fn("a.txt", ""): {"tagged_at": _JOB_T0.isoformat()},
        }
        client = _mock_s3_get_raw_payload(payload)

        StateStore().delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET,
            [_item_key_fn("a.txt", None)], current_etag=_ETAG,
        )

        stored = self._written(client)["completion_timestamps"]
        assert list(stored) == [_item_key_fn("b.txt", "")]


class TestEnrichmentRoundTrip:
    """One state object, three calls in sequence (Req 8.2).

    ``TestNullVersionSideMapKey`` asserts each method's keying in isolation
    against a hand-built payload. This drives the whole sequence a real interval
    runs — ``store_completion_timestamps`` at manifest generation,
    ``merge_completion_report`` when the BOPS report arrives, then
    ``delete_completion_items`` after publish — against a single evolving state
    object, so the write key and the read key have to agree for the enrichment
    to arrive, rather than agreeing because the test wrote both of them.

    ``_FakeConditionalS3`` carries the payload from each call to the next under
    real If-Match semantics, so the ETag chain is exercised too: a call reading
    a stale payload would fail the precondition instead of quietly enriching
    from nothing.

    The unversioned case is the one the defect hid in. The versioned case is a
    control, so a future change that breaks the ordinary path fails here too.

    What this pins, checked by reverting each half of the R6 option 1 fix: with
    both the writer normalization and the reader's legacy-key fallback removed —
    the genuine pre-fix state — the unversioned case fails on the enrichment miss
    while the versioned control still passes. Removing the writer normalization
    alone does not fail, because the reader's fallback then finds the ``""``-keyed
    entry the writer just produced. So this asserts the round trip works, not that
    both halves are present; the writer half is pinned separately by
    ``TestNullVersionSideMapKey.test_a_null_version_entry_is_written_under_the_none_form``.
    """

    _KEY = "a.txt"

    @staticmethod
    def _state(client: _FakeConditionalS3) -> dict:
        assert client._body is not None
        return json.loads(client._body)

    def _round_trip(self, version_id: str | None) -> None:
        store = StateStore()
        client = _FakeConditionalS3()
        # The transport form the manifest generator produces: its dicts are
        # typed tuple[str, str], so a null version arrives as "".
        transport_id = version_id or ""
        item_key = _item_key_fn(self._KEY, version_id)

        etag = store.store_completion_timestamps(
            client, _STATE_BUCKET, _SRC_BUCKET,
            {(self._KEY, transport_id): (_JOB_T0, _JOB_T1)},
            current_etag=None,
            routing={(self._KEY, transport_id): (["rule-a"], ["dest-a"])},
        )

        etag = store.merge_completion_report(
            client, _STATE_BUCKET, _SRC_BUCKET,
            report=BopsCompletionReport(
                created_at=_NOW,
                entries=(
                    ManifestEntry(
                        _SRC_BUCKET, self._KEY, version_id, task_status="succeeded",
                    ),
                ),
            ),
            replication_config_id="cfg-1",
            job_id="job-report",
            job_created_at=_JOB_T1,
            current_etag=etag,
        )

        items = store.get_all_completion_items(client, _STATE_BUCKET, _SRC_BUCKET)
        assert set(items) == {item_key}
        item = items[item_key]
        assert item.version_id == version_id
        assert item.tagged_at == _JOB_T0
        assert item.last_modified == _JOB_T1
        assert item.matched_rules == frozenset({"rule-a"})
        assert item.destinations == frozenset({"dest-a"})

        store.delete_completion_items(
            client, _STATE_BUCKET, _SRC_BUCKET, [item_key], current_etag=etag,
        )

        final = self._state(client)
        assert final["completion_items"] == {}
        assert final["completion_timestamps"] == {}
        assert final["completion_routing"] == {}

    def test_an_unversioned_object_round_trips(self) -> None:
        self._round_trip(None)

    def test_a_versioned_object_round_trips(self) -> None:
        self._round_trip("v1")
