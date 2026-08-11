"""Unit tests for src/core/checkpoint_serializer.py.

Covers:
- serialize() produces valid JSON
- deserialize(serialize(state)) == state (round-trip) for states with and
  without a lease and with a processed-operation window
- Datetime serialization preserves timezone information (UTC and naive)
- Lease fields round-trip correctly, including the LeaseStatus enum
- deserialize() raises expected exceptions on malformed input
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.core.checkpoint_serializer import deserialize, serialize
from src.core.models import CheckpointState, Lease, LeaseStatus, ProcessedRef
from src.core.watermark import EPOCH_WATERMARK, to_watermark

_NOW_UTC = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
_NOW_NAIVE = datetime(2024, 6, 15, 10, 30, 0)

_WM_100 = "2024-01-01T00:00:01.000000Z"
_WM_200 = "2024-01-01T00:00:02.000000Z"
_WM_500 = "2024-01-01T00:00:05.000000Z"




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_lease(
    lease_id: str = "lease-123",
    candidate_max: str = _WM_500,
    acquired_at: datetime = _NOW_UTC,
    status: LeaseStatus = LeaseStatus.IN_FLIGHT,
) -> Lease:
    return Lease(
        lease_id=lease_id,
        candidate_max_watermark=candidate_max,
        acquired_at=acquired_at,
        status=status,
    )


def make_state(
    source_bucket: str = "my-bucket",
    watermark: str = _WM_100,
    lease: Lease | None = None,
    processed_window: list[ProcessedRef] | None = None,
) -> CheckpointState:
    return CheckpointState(
        source_bucket=source_bucket,
        last_processed_watermark=watermark,
        lease=lease,
        processed_window=processed_window or [],
    )


# ---------------------------------------------------------------------------
# serialize() — structural correctness
# ---------------------------------------------------------------------------


class TestSerialize:
    def test_returns_valid_json(self):
        state = make_state()
        result = serialize(state)
        parsed = json.loads(result)  # must not raise
        assert isinstance(parsed, dict)

    def test_no_lease_produces_null(self):
        state = make_state(lease=None)
        parsed = json.loads(serialize(state))
        assert parsed["lease"] is None

    def test_source_bucket_in_output(self):
        state = make_state(source_bucket="source-bucket-a")
        parsed = json.loads(serialize(state))
        assert parsed["source_bucket"] == "source-bucket-a"

    def test_watermark_in_output(self):
        state = make_state(watermark=_WM_200)
        parsed = json.loads(serialize(state))
        assert parsed["last_processed_watermark"] == _WM_200

    def test_empty_window_serialized_as_list(self):
        state = make_state()
        parsed = json.loads(serialize(state))
        assert parsed["processed_window"] == []

    def test_window_entries_serialized(self):
        window = [
            ProcessedRef(logical_operation_id="op-a", watermark=_WM_100),
            ProcessedRef(logical_operation_id="op-b", watermark=_WM_200),
        ]
        state = make_state(processed_window=window)
        parsed = json.loads(serialize(state))
        assert parsed["processed_window"] == [
            {"logical_operation_id": "op-a", "watermark": _WM_100},
            {"logical_operation_id": "op-b", "watermark": _WM_200},
        ]

    def test_with_lease_encodes_lease_fields(self):
        lease = make_lease(lease_id="lid-001", candidate_max=_WM_500)
        state = make_state(lease=lease)
        parsed = json.loads(serialize(state))
        assert parsed["lease"] is not None
        assert parsed["lease"]["lease_id"] == "lid-001"
        assert parsed["lease"]["candidate_max_watermark"] == _WM_500
        assert parsed["lease"]["status"] == "IN_FLIGHT"

    def test_lease_datetime_is_iso_string(self):
        lease = make_lease(acquired_at=_NOW_UTC)
        state = make_state(lease=lease)
        parsed = json.loads(serialize(state))
        acquired_at_str = parsed["lease"]["acquired_at"]
        assert isinstance(acquired_at_str, str)
        dt = datetime.fromisoformat(acquired_at_str)
        assert isinstance(dt, datetime)

    def test_naive_datetime_serializes(self):
        lease = make_lease(acquired_at=_NOW_NAIVE)
        state = make_state(lease=lease)
        result = serialize(state)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# deserialize(serialize(state)) == state  — round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_no_lease(self):
        state = make_state(source_bucket="bucket-x", watermark=_WM_100, lease=None)
        assert deserialize(serialize(state)) == state

    def test_with_lease_utc(self):
        lease = make_lease(acquired_at=_NOW_UTC)
        state = make_state(source_bucket="bucket-y", watermark=_WM_200, lease=lease)
        restored = deserialize(serialize(state))
        assert restored == state
        assert restored.lease is not None
        assert restored.lease.lease_id == lease.lease_id
        assert restored.lease.candidate_max_watermark == lease.candidate_max_watermark
        assert restored.lease.acquired_at == lease.acquired_at
        assert restored.lease.status == LeaseStatus.IN_FLIGHT

    def test_with_lease_naive_datetime(self):
        lease = make_lease(acquired_at=_NOW_NAIVE)
        state = make_state(lease=lease)
        restored = deserialize(serialize(state))
        assert restored.lease is not None
        assert restored.lease.acquired_at == _NOW_NAIVE

    def test_with_processed_window(self):
        window = [
            ProcessedRef(logical_operation_id="op-a", watermark=_WM_100),
            ProcessedRef(logical_operation_id="op-b", watermark=_WM_200),
        ]
        state = make_state(processed_window=window)
        restored = deserialize(serialize(state))
        assert restored == state
        assert restored.processed_window == window

    def test_source_bucket_preserved(self):
        state = make_state(source_bucket="special-bucket-name")
        assert deserialize(serialize(state)).source_bucket == "special-bucket-name"

    def test_watermark_preserved(self):
        state = make_state(watermark=_WM_500)
        assert deserialize(serialize(state)).last_processed_watermark == _WM_500

    def test_lease_status_preserved(self):
        lease = make_lease(status=LeaseStatus.IN_FLIGHT)
        state = make_state(lease=lease)
        restored = deserialize(serialize(state))
        assert restored.lease.status == LeaseStatus.IN_FLIGHT

    def test_missing_window_field_defaults_to_empty(self):
        """State objects written before the window field existed deserialize cleanly."""
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": None,
        })
        restored = deserialize(payload)
        assert restored.processed_window == []


# ---------------------------------------------------------------------------
# deserialize() — error handling
# ---------------------------------------------------------------------------


class TestDeserializeErrors:
    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            deserialize("not valid json{")

    def test_missing_source_bucket_raises(self):
        payload = json.dumps({
            "last_processed_watermark": _WM_100,
            "lease": None,
        })
        with pytest.raises(KeyError):
            deserialize(payload)

    def test_missing_watermark_raises(self):
        payload = json.dumps({
            "source_bucket": "b",
            "lease": None,
        })
        with pytest.raises(KeyError):
            deserialize(payload)

    def test_invalid_lease_status_is_discarded(self):
        """An unrecognised LeaseStatus drops the lease instead of raising.

        Replaces the former ``test_invalid_lease_status_raises``. The property
        being protected is unchanged — a malformed lease is never honored — but
        raising here would propagate to a per-bucket skip that repeats every
        interval and that no write path can repair, because every write runs
        downstream of the failing read. The rest of the checkpoint must still
        parse so the bucket keeps making progress.
        """
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": {
                "lease_id": "lid",
                "candidate_max_watermark": _WM_200,
                "acquired_at": _NOW_UTC.isoformat(),
                "status": "UNKNOWN_STATUS",
            },
        })
        state = deserialize(payload)
        assert state.lease is None
        assert state.last_processed_watermark == _WM_100
        assert state.source_bucket == "b"

    def test_invalid_datetime_is_discarded(self):
        """An unparseable ``acquired_at`` drops the lease instead of raising.

        Replaces the former ``test_invalid_datetime_raises``; same rationale as
        ``test_invalid_lease_status_is_discarded``.
        """
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": {
                "lease_id": "lid",
                "candidate_max_watermark": _WM_200,
                "acquired_at": "not-a-datetime",
                "status": "IN_FLIGHT",
            },
        })
        state = deserialize(payload)
        assert state.lease is None
        assert state.last_processed_watermark == _WM_100

    def test_non_string_watermark_raises(self):
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": 123,
            "lease": None,
        })
        with pytest.raises(ValueError):
            deserialize(payload)

    def test_window_not_a_list_raises(self):
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": None,
            "processed_window": "not-a-list",
        })
        with pytest.raises(ValueError):
            deserialize(payload)


class TestLeaseWatermarkPlausibilityBound:
    """A lease whose ``candidate_max_watermark`` cannot be trusted is dropped.

    ``is_eligible`` filters every operation at or below a live lease's
    ``candidate_max_watermark``, so a far-future value there suppresses the
    whole journal with no error raised and no journal record able to grow past
    it. That makes it a permanent, silent halt — strictly worse than the
    ``last_processed_watermark`` poisoning case, which at least resolves once
    the clock catches up. The lease is therefore discarded so the next run
    takes it over.
    """

    def _payload(self, candidate_max):
        return json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": {
                "lease_id": "lid",
                "candidate_max_watermark": candidate_max,
                "acquired_at": _NOW_UTC.isoformat(),
                "status": "IN_FLIGHT",
            },
        })

    def test_far_future_lease_watermark_is_discarded(self):
        """The year-9999 poisoning case drops the lease."""
        state = deserialize(self._payload("9999-12-31T23:59:59.000000Z"))
        assert state.lease is None

    def test_non_canonical_lease_watermark_is_discarded(self):
        """A non-canonical string is not comparable and drops the lease."""
        state = deserialize(self._payload("2024-01-01"))
        assert state.lease is None

    def test_non_string_lease_watermark_is_discarded(self):
        """A well-typed JSON number would crash the ``<=`` comparison in
        ``is_eligible``; drop the lease instead."""
        state = deserialize(self._payload(12345))
        assert state.lease is None

    def test_plausible_lease_watermark_is_kept(self):
        """A canonical past watermark is honored — the bound must not be so
        tight that it discards legitimate leases."""
        state = deserialize(self._payload(_WM_200))
        assert state.lease is not None
        assert state.lease.candidate_max_watermark == _WM_200

    def test_discarding_the_lease_preserves_the_rest_of_the_checkpoint(self):
        """Dropping the lease must not cost the watermark or the window, which
        would rewind the bucket rather than just unblock it."""
        payload = json.loads(self._payload("9999-12-31T23:59:59.000000Z"))
        payload["processed_window"] = [
            {"logical_operation_id": "op-1", "watermark": _WM_100}
        ]
        state = deserialize(json.dumps(payload))
        assert state.lease is None
        assert state.last_processed_watermark == _WM_100
        assert [r.logical_operation_id for r in state.processed_window] == ["op-1"]

    def test_lease_missing_a_required_key_is_discarded(self):
        """A lease absent ``lease_id`` was not written by this serializer."""
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": {
                "candidate_max_watermark": _WM_200,
                "acquired_at": _NOW_UTC.isoformat(),
                "status": "IN_FLIGHT",
            },
        })
        assert deserialize(payload).lease is None

    def test_lease_not_an_object_is_discarded(self):
        payload = json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": _WM_100,
            "lease": "not-an-object",
        })
        assert deserialize(payload).lease is None


# ---------------------------------------------------------------------------
# Watermark plausibility bound
#
# AWS Security Agent finding f-5a303b12-11c3-48e2-9e3d-619ff88c3d77: a
# well-typed but adversarial watermark in the state object silently halts
# replication for the bucket, because every subsequent journal query starts
# after that timestamp and returns nothing.
# ---------------------------------------------------------------------------


class TestWatermarkPlausibilityBound:
    @staticmethod
    def _payload(watermark):
        return json.dumps({
            "source_bucket": "b",
            "last_processed_watermark": watermark,
            "lease": None,
        })

    @pytest.mark.parametrize(
        "watermark",
        [
            "9999-12-31T23:59:59.000000Z",  # the poisoning case
            "2999-01-01T00:00:00.000000Z",
        ],
    )
    def test_far_future_watermark_rejected(self, watermark):
        with pytest.raises(ValueError, match="plausible canonical watermark"):
            deserialize(self._payload(watermark))

    @pytest.mark.parametrize(
        "watermark",
        [
            "not-a-timestamp",
            "2024-01-01",                      # date only
            "2024-01-01T00:00:00Z",            # no microseconds
            "2024-01-01T00:00:00.000000+00:00",  # offset instead of Z
            "2024-01-01T00:00:00.000000Z ",    # trailing whitespace
            "0",
        ],
    )
    def test_non_canonical_watermark_rejected(self, watermark):
        with pytest.raises(ValueError, match="plausible canonical watermark"):
            deserialize(self._payload(watermark))

    def test_epoch_watermark_accepted(self):
        """The empty string is the legitimate first-run watermark."""
        state = deserialize(self._payload(""))
        assert state.last_processed_watermark == ""

    def test_canonical_past_watermark_accepted(self):
        state = deserialize(self._payload(_WM_100))
        assert state.last_processed_watermark == _WM_100

    def test_watermark_within_clock_skew_accepted(self):
        """A small forward offset is tolerated — real clock skew between the
        Lambda and the journal writer must not fail the read."""
        near_future = to_watermark(
            datetime.now(timezone.utc) + timedelta(minutes=5)
        )
        state = deserialize(self._payload(near_future))
        assert state.last_processed_watermark == near_future


# ---------------------------------------------------------------------------
# Property 10: Checkpoint state serialization round-trip (task 7.2)
# Feature: tag-based-s3-replication, Property 10: Checkpoint state serialization round-trip
# ---------------------------------------------------------------------------

from hypothesis import given, settings
from hypothesis import strategies as st

# ``last_processed_watermark`` is bounded on read by
# watermark.is_plausible_watermark (security-scan-remediation: a far-future or
# non-canonical value poisons the journal query), so the round-trip property
# holds over canonical watermarks and the epoch, not over arbitrary text.
# Other watermark-typed fields (Lease.candidate_max_watermark,
# ProcessedRef.watermark) are not bounded, so they keep the wider strategy.
_canonical_watermarks = st.builds(
    to_watermark,
    st.datetimes(
        min_value=datetime(2000, 1, 1),
        # Bounded at "now" rather than a fixed future date so the strategy
        # cannot generate a value the plausibility check rejects.
        max_value=datetime.now(timezone.utc).replace(tzinfo=None),
    ),
)
_persisted_watermarks = st.one_of(st.just(EPOCH_WATERMARK), _canonical_watermarks)


class TestProperty10SerializationRoundTrip:
    """deserialize(serialize(s)) == s for any valid CheckpointState.

    # Feature: tag-based-s3-replication, Property 10: Checkpoint state serialization round-trip
    Validates: Requirement 4.3
    """

    @given(
        source_bucket=st.from_regex(r"^[a-z][a-z0-9\-]{2,20}$", fullmatch=True),
        watermark=_persisted_watermarks,
        window=st.lists(
            st.builds(
                ProcessedRef,
                logical_operation_id=st.text(min_size=1, max_size=30),
                watermark=st.text(min_size=0, max_size=50),
            ),
            max_size=8,
        ),
    )
    @settings(max_examples=100)
    def test_round_trip_no_lease(self, source_bucket, watermark, window) -> None:
        """Round-trip without a lease, including the processed-operation window.

        # Feature: tag-based-s3-replication, Property 10: Checkpoint state serialization round-trip
        """
        state = CheckpointState(
            source_bucket=source_bucket,
            last_processed_watermark=watermark,
            lease=None,
            processed_window=window,
        )
        restored = deserialize(serialize(state))
        assert restored.source_bucket == state.source_bucket
        assert restored.last_processed_watermark == state.last_processed_watermark
        assert restored.lease is None
        assert restored.processed_window == window

    @given(
        source_bucket=st.from_regex(r"^[a-z][a-z0-9\-]{2,20}$", fullmatch=True),
        watermark=_persisted_watermarks,
        lease_id=st.text(min_size=1, max_size=36),
        # A real lease's candidate_max_watermark is a max() over journal
        # record_timestamps, so it is always canonical. _deserialize_lease
        # discards a lease whose watermark is not, so drawing arbitrary text
        # here would be asserting a round trip the Solution never performs.
        candidate_max=_persisted_watermarks,
    )
    @settings(max_examples=100)
    def test_round_trip_with_lease(
        self,
        source_bucket: str,
        watermark: str,
        lease_id: str,
        candidate_max: str,
    ) -> None:
        """Round-trip with an embedded Lease.

        # Feature: tag-based-s3-replication, Property 10: Checkpoint state serialization round-trip
        """
        lease = Lease(
            lease_id=lease_id,
            candidate_max_watermark=candidate_max,
            acquired_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            status=LeaseStatus.IN_FLIGHT,
        )
        state = CheckpointState(
            source_bucket=source_bucket,
            last_processed_watermark=watermark,
            lease=lease,
        )
        restored = deserialize(serialize(state))
        assert restored == state
        assert restored.lease is not None
        assert restored.lease.lease_id == lease.lease_id
        assert restored.lease.candidate_max_watermark == lease.candidate_max_watermark
        assert restored.lease.status == LeaseStatus.IN_FLIGHT
