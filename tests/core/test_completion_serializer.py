"""Unit and property tests for src/core/completion_serializer.py.

Feature: source-status-completion-tracking.

Covers:
- serialize_completion_items / deserialize_completion_items round-trip
  (object-level, configs-map shape — design.md Decision 2)
- serialize_processed_job_ids / deserialize_processed_job_ids round-trip
  (UNCHANGED)
- serialize_scan_state / deserialize_scan_state round-trip (UNCHANGED)
- All three round trips leave every pre-existing CheckpointState/Lease/
  submission_records field in the same payload byte-for-byte unchanged
  (Property 16)
- Tolerance of an absent completion_items / completion_processed_job_ids /
  completion_scan_state key
- Error handling for malformed input
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.checkpoint_serializer import serialize as serialize_checkpoint
from src.core.checkpoint_serializer import serialize_submission_record
from src.core.completion_serializer import (
    deserialize_completion_items,
    deserialize_processed_job_ids,
    deserialize_scan_state,
    item_key as _real_item_key,
    serialize_completion_items,
    serialize_processed_job_ids,
    serialize_scan_state,
)
from src.core.models import (
    CheckpointState,
    CompletionState,
    ConfigContext,
    Lease,
    LeaseStatus,
    ScanState,
    SubmissionRecord,
    SubmissionStatus,
    TrackedObject,
)

_T0 = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
_T1 = datetime(2024, 6, 16, 8, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_config_context(
    replication_config_id: str = "cfg-1",
    job_id: str = "job-1",
    manifest_generated_at: datetime = _T0,
    bops_confirmed: bool = True,
) -> ConfigContext:
    return ConfigContext(
        replication_config_id=replication_config_id,
        job_id=job_id,
        manifest_generated_at=manifest_generated_at,
        bops_confirmed=bops_confirmed,
    )


def make_obj(
    source_bucket: str = "example-source-bucket",
    object_key: str = "key-a",
    version_id: str | None = "v1",
    configs: dict[str, ConfigContext] | None = None,
    state: CompletionState = CompletionState.PENDING,
    resolved_at: datetime | None = None,
    resolution_method: str | None = None,
    replication_outcome: str | None = None,
) -> TrackedObject:
    return TrackedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        version_id=version_id,
        configs=configs if configs is not None else {"cfg-1": make_config_context()},
        state=state,
        resolved_at=resolved_at,
        resolution_method=resolution_method,
        replication_outcome=replication_outcome,
    )


def item_key(object_key: str, version_id: str | None) -> str:
    return f"{object_key}\x00{version_id if version_id is not None else ''}"


# ---------------------------------------------------------------------------
# item_key — None vs "" collision fix (code-review-remediation spec Req 8.4)
# ---------------------------------------------------------------------------


class TestItemKeyNullVersionDistinctFromEmptyString:
    def test_none_and_empty_string_produce_different_keys(self):
        """version_id=None (null-version marker) must not collide with a
        (real, non-empty in practice) version_id=''."""
        key_none = _real_item_key("obj-1", None)
        key_empty = _real_item_key("obj-1", "")
        assert key_none != key_empty

    def test_none_key_is_stable(self):
        assert _real_item_key("obj-1", None) == _real_item_key("obj-1", None)

    def test_real_version_id_key_unaffected(self):
        assert _real_item_key("obj-1", "v123") == "obj-1\x00v123"


# ---------------------------------------------------------------------------
# serialize_completion_items / deserialize_completion_items — structural
# ---------------------------------------------------------------------------


class TestSerializeCompletionItems:
    def test_returns_dict_keyed_by_item_key(self):
        obj = make_obj(object_key="obj-1", version_id="v1")
        result = serialize_completion_items({item_key("obj-1", "v1"): obj})
        assert set(result.keys()) == {"obj-1\x00v1"}

    def test_configs_keyed_by_replication_config_id(self):
        obj = make_obj(configs={"cfg-a": make_config_context(replication_config_id="cfg-a")})
        result = serialize_completion_items({"k": obj})
        assert set(result["k"]["configs"].keys()) == {"cfg-a"}

    def test_multi_config_item_serializes_all_configs(self):
        obj = make_obj(
            configs={
                "cfg-a": make_config_context(replication_config_id="cfg-a", job_id="job-a"),
                "cfg-b": make_config_context(replication_config_id="cfg-b", job_id="job-b"),
            }
        )
        result = serialize_completion_items({"k": obj})
        assert set(result["k"]["configs"].keys()) == {"cfg-a", "cfg-b"}
        assert result["k"]["configs"]["cfg-a"]["job_id"] == "job-a"
        assert result["k"]["configs"]["cfg-b"]["job_id"] == "job-b"

    def test_top_level_object_fields_serialized(self):
        obj = make_obj(
            configs={"cfg-1": make_config_context(replication_config_id="cfg-1", job_id="job-99")},
            state=CompletionState.RESOLVED,
            resolved_at=_T1,
            resolution_method="source_status_header",
            replication_outcome="COMPLETE",
        )
        result = serialize_completion_items({"k": obj})
        data = result["k"]
        assert data["source_bucket"] == "example-source-bucket"
        assert data["object_key"] == "key-a"
        assert data["version_id"] == "v1"
        assert data["state"] == "RESOLVED"
        assert data["resolved_at"] == _T1.isoformat()
        assert data["resolution_method"] == "source_status_header"
        assert data["replication_outcome"] == "COMPLETE"
        assert data["configs"]["cfg-1"] == {
            "replication_config_id": "cfg-1",
            "job_id": "job-99",
            "manifest_generated_at": _T0.isoformat(),
            "bops_confirmed": True,
        }

    def test_null_version_id_serializes_as_json_null(self):
        obj = make_obj(object_key="obj-2", version_id=None)
        result = serialize_completion_items({item_key("obj-2", None): obj})
        assert result["obj-2\x00"]["version_id"] is None

    def test_json_serializable(self):
        obj = make_obj()
        result = serialize_completion_items({"k": obj})
        json.dumps(result)  # must not raise

    def test_empty_dict_serializes_to_empty_dict(self):
        assert serialize_completion_items({}) == {}


class TestDeserializeCompletionItems:
    def test_absent_key_returns_empty_dict(self):
        assert deserialize_completion_items({}) == {}

    def test_not_a_dict_raises(self):
        payload = {"completion_items": "not-a-dict"}
        with pytest.raises(ValueError):
            deserialize_completion_items(payload)

    def test_round_trip_preserves_item_key(self):
        obj = make_obj(object_key="obj-1", version_id="v1")
        serialized = serialize_completion_items({item_key("obj-1", "v1"): obj})
        restored = deserialize_completion_items({"completion_items": serialized})
        assert set(restored.keys()) == {"obj-1\x00v1"}
        assert restored["obj-1\x00v1"] == obj

    def test_empty_configs_dict_tolerated(self):
        payload = {
            "completion_items": {
                "obj-1\x00v1": {
                    "source_bucket": "my-bucket",
                    "object_key": "obj-1",
                    "version_id": "v1",
                    "state": "PENDING",
                    "resolved_at": None,
                    "resolution_method": None,
                    "replication_outcome": None,
                    "configs": {},
                }
            }
        }
        restored = deserialize_completion_items(payload)
        assert restored["obj-1\x00v1"].configs == {}

    def test_bad_configs_type_raises(self):
        payload = {
            "completion_items": {
                "obj-1\x00v1": {
                    "source_bucket": "my-bucket",
                    "object_key": "obj-1",
                    "version_id": "v1",
                    "state": "PENDING",
                    "configs": "not-a-dict",
                }
            }
        }
        with pytest.raises(ValueError):
            deserialize_completion_items(payload)

    def test_missing_source_bucket_raises(self):
        payload = {
            "completion_items": {
                "obj-1\x00v1": {
                    "object_key": "obj-1",
                    "version_id": "v1",
                    "state": "PENDING",
                    "configs": {},
                }
            }
        }
        with pytest.raises(KeyError):
            deserialize_completion_items(payload)


# ---------------------------------------------------------------------------
# serialize_processed_job_ids / deserialize_processed_job_ids — structural
# ---------------------------------------------------------------------------


class TestProcessedJobIdsRoundTrip:
    def test_serialize_returns_list(self):
        result = serialize_processed_job_ids({"job-1", "job-2"})
        assert isinstance(result, list)
        assert set(result) == {"job-1", "job-2"}

    def test_round_trip(self):
        ids = {"job-1", "job-2", "job-3"}
        payload = {"completion_processed_job_ids": serialize_processed_job_ids(ids)}
        restored = deserialize_processed_job_ids(payload)
        assert restored == ids

    def test_absent_key_returns_empty_set(self):
        assert deserialize_processed_job_ids({}) == set()

    def test_empty_set_round_trips(self):
        payload = {"completion_processed_job_ids": serialize_processed_job_ids(set())}
        assert deserialize_processed_job_ids(payload) == set()

    def test_not_a_list_raises(self):
        with pytest.raises(ValueError):
            deserialize_processed_job_ids({"completion_processed_job_ids": "not-a-list"})

    def test_non_string_entry_raises(self):
        with pytest.raises(ValueError):
            deserialize_processed_job_ids({"completion_processed_job_ids": ["job-1", 2]})


# ---------------------------------------------------------------------------
# serialize_scan_state / deserialize_scan_state — structural (unchanged)
# ---------------------------------------------------------------------------


class TestScanStateRoundTrip:
    def test_serialize_returns_dict_keyed_by_config_id(self):
        state = ScanState(last_scan_at=_T0, last_scan_match_count=5)
        result = serialize_scan_state({"cfg-1": state})
        assert result == {
            "cfg-1": {"last_scan_at": _T0.isoformat(), "last_scan_match_count": 5}
        }

    def test_round_trip(self):
        state = ScanState(last_scan_at=_T0, last_scan_match_count=0)
        payload = {"completion_scan_state": serialize_scan_state({"cfg-1": state})}
        restored = deserialize_scan_state(payload)
        assert restored == {"cfg-1": state}

    def test_absent_key_returns_empty_dict(self):
        assert deserialize_scan_state({}) == {}

    def test_not_a_dict_raises(self):
        with pytest.raises(ValueError):
            deserialize_scan_state({"completion_scan_state": "not-a-dict"})

    def test_per_entry_not_a_dict_raises(self):
        with pytest.raises(ValueError):
            deserialize_scan_state({"completion_scan_state": {"cfg-1": "bad"}})


# ---------------------------------------------------------------------------
# Property 16: Tracked-object, processed-job-id, and scan-state
# serialization round trip
# Feature: source-status-completion-tracking, Property 16: Tracked-object, processed-job-id, and scan-state serialization round trip
# Validates: Requirements 2.1, 2.2, 2.3
# ---------------------------------------------------------------------------

_object_keys = st.text(min_size=1, max_size=30).filter(lambda s: "\x00" not in s)
_version_ids = st.one_of(st.none(), st.text(min_size=1, max_size=20))
_states = st.sampled_from(list(CompletionState))
_resolution_methods = st.one_of(st.none(), st.just("source_status_header"))
_replication_outcomes = st.one_of(
    st.none(),
    st.sampled_from(["COMPLETE", "PENDING", "FAILED", "UNKNOWN"]),
)

_datetimes = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 1, 1),
).map(lambda dt: dt.replace(tzinfo=timezone.utc))

_config_ids = st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True)
_job_ids = st.from_regex(r"^[a-zA-Z0-9\-]{1,20}$", fullmatch=True)

_config_context_strategy = st.builds(
    make_config_context,
    job_id=_job_ids,
    manifest_generated_at=_datetimes,
    bops_confirmed=st.booleans(),
)


def _configs_dict_strategy(min_size: int = 0, max_size: int = 3):
    """Build a dict[replication_config_id, ConfigContext] with each
    context's own replication_config_id forced to match its dict key."""

    def _build(config_ids: list[str], contexts: list[ConfigContext]) -> dict[str, ConfigContext]:
        result = {}
        for config_id, ctx in zip(config_ids, contexts):
            result[config_id] = ConfigContext(
                replication_config_id=config_id,
                job_id=ctx.job_id,
                manifest_generated_at=ctx.manifest_generated_at,
                bops_confirmed=ctx.bops_confirmed,
            )
        return result

    return st.lists(
        st.tuples(_config_ids, _config_context_strategy), min_size=min_size, max_size=max_size, unique_by=lambda t: t[0]
    ).map(lambda pairs: _build([p[0] for p in pairs], [p[1] for p in pairs]))


def _existing_payload() -> dict:
    """Build a payload dict with checkpoint-style keys already set, via the
    real checkpoint_serializer.serialize + serialize_submission_record, so
    the round-trip assertion exercises the exact shape those functions
    produce rather than a hand-rolled approximation.
    """
    checkpoint = CheckpointState(
        source_bucket="my-bucket",
        last_processed_watermark="2024-01-01T00:00:01.000000Z",
        lease=Lease(
            lease_id="lease-1",
            candidate_max_watermark="2024-01-01T00:00:02.000000Z",
            acquired_at=_T0,
            status=LeaseStatus.IN_FLIGHT,
        ),
    )
    payload = json.loads(serialize_checkpoint(checkpoint))
    submission = SubmissionRecord(
        replication_config_id="cfg-1",
        source_bucket="my-bucket",
        job_id="prior-job",
        manifest_key="manifests/prior.csv",
        submitted_at=_T0,
        status=SubmissionStatus.SUBMITTED,
        watermark_low="a",
        watermark_high="b",
    )
    payload["submission_records"] = {
        "cfg-1": serialize_submission_record(submission)
    }
    return payload


class TestProperty16SerializationRoundTrip:
    """deserialize_completion_items(serialize_completion_items(items)) == items,
    deserialize_processed_job_ids(serialize_processed_job_ids(ids)) == ids, and
    deserialize_scan_state(serialize_scan_state(state)) == state, and every
    pre-existing CheckpointState/Lease/submission_records field is left
    byte-for-byte unchanged by any of these round trips.

    # Feature: source-status-completion-tracking, Property 16: Tracked-object,
    processed-job-id, and scan-state serialization round trip
    Validates: Requirements 2.1, 2.2, 2.3
    """

    @given(
        object_keys_and_versions=st.lists(
            st.tuples(_object_keys, _version_ids), min_size=0, max_size=5, unique_by=lambda t: t
        ),
        configs_list=st.lists(_configs_dict_strategy(min_size=1, max_size=3), min_size=0, max_size=5),
        states=st.lists(_states, min_size=0, max_size=5),
        resolution_methods=st.lists(_resolution_methods, min_size=0, max_size=5),
        replication_outcomes=st.lists(_replication_outcomes, min_size=0, max_size=5),
    )
    @settings(max_examples=100)
    def test_completion_items_round_trip(
        self,
        object_keys_and_versions: list[tuple[str, str | None]],
        configs_list: list[dict[str, ConfigContext]],
        states: list[CompletionState],
        resolution_methods: list[str | None],
        replication_outcomes: list[str | None],
    ) -> None:
        """# Feature: source-status-completion-tracking, Property 16: Tracked-object,
        processed-job-id, and scan-state serialization round trip
        """
        n = min(
            len(object_keys_and_versions),
            len(configs_list),
            len(states),
            len(resolution_methods),
            len(replication_outcomes),
        )
        items: dict[str, TrackedObject] = {}
        for i in range(n):
            object_key, version_id = object_keys_and_versions[i]
            configs = configs_list[i]
            if not configs:
                continue
            state = states[i]
            resolved_at = _T1 if state == CompletionState.RESOLVED else None
            key = item_key(object_key, version_id)
            items[key] = TrackedObject(
                source_bucket="my-bucket",
                object_key=object_key,
                version_id=version_id,
                configs=configs,
                state=state,
                resolved_at=resolved_at,
                resolution_method=resolution_methods[i],
                replication_outcome=replication_outcomes[i],
            )

        payload = _existing_payload()
        original_untouched = {
            k: v for k, v in payload.items() if k != "completion_items"
        }

        payload["completion_items"] = serialize_completion_items(items)
        restored = deserialize_completion_items(payload)

        assert restored == items

        for key, value in original_untouched.items():
            assert payload[key] == value

    @given(ids=st.sets(_job_ids, max_size=10))
    @settings(max_examples=100)
    def test_processed_job_ids_round_trip(self, ids: set[str]) -> None:
        """# Feature: source-status-completion-tracking, Property 16: Tracked-object,
        processed-job-id, and scan-state serialization round trip
        """
        payload = _existing_payload()
        original_untouched = {
            k: v for k, v in payload.items() if k != "completion_processed_job_ids"
        }

        payload["completion_processed_job_ids"] = serialize_processed_job_ids(ids)
        restored = deserialize_processed_job_ids(payload)

        assert restored == ids

        for key, value in original_untouched.items():
            assert payload[key] == value

    @given(
        scan_state=st.dictionaries(
            _config_ids,
            st.builds(
                ScanState,
                last_scan_at=_datetimes,
                last_scan_match_count=st.integers(min_value=0, max_value=1_000_000),
            ),
            max_size=5,
        ),
    )
    @settings(max_examples=100)
    def test_scan_state_round_trip(self, scan_state: dict[str, ScanState]) -> None:
        """# Feature: source-status-completion-tracking, Property 16: Tracked-object,
        processed-job-id, and scan-state serialization round trip
        """
        payload = _existing_payload()
        original_untouched = {
            k: v for k, v in payload.items() if k != "completion_scan_state"
        }

        payload["completion_scan_state"] = serialize_scan_state(scan_state)
        restored = deserialize_scan_state(payload)

        assert restored == scan_state

        for key, value in original_untouched.items():
            assert payload[key] == value

    @given(
        object_key=_object_keys,
        version_id=_version_ids,
        configs=_configs_dict_strategy(min_size=1, max_size=3),
        ids=st.sets(_job_ids, max_size=5),
        scan_state=st.dictionaries(
            _config_ids,
            st.builds(
                ScanState,
                last_scan_at=_datetimes,
                last_scan_match_count=st.integers(min_value=0, max_value=1_000_000),
            ),
            max_size=3,
        ),
    )
    @settings(max_examples=100)
    def test_all_three_round_trips_coexist_in_same_payload(
        self,
        object_key: str,
        version_id: str | None,
        configs: dict[str, ConfigContext],
        ids: set[str],
        scan_state: dict[str, ScanState],
    ) -> None:
        """All three keys can be applied to the same payload dict without any
        round trip disturbing the others, or any pre-existing checkpoint-style
        key.

        # Feature: source-status-completion-tracking, Property 16: Tracked-object,
        processed-job-id, and scan-state serialization round trip
        """
        items = {
            item_key(object_key, version_id): TrackedObject(
                source_bucket="my-bucket",
                object_key=object_key,
                version_id=version_id,
                configs=configs,
                state=CompletionState.PENDING,
            )
        }

        payload = _existing_payload()
        original_untouched = dict(payload)

        payload["completion_items"] = serialize_completion_items(items)
        payload["completion_processed_job_ids"] = serialize_processed_job_ids(ids)
        payload["completion_scan_state"] = serialize_scan_state(scan_state)

        restored_items = deserialize_completion_items(payload)
        restored_ids = deserialize_processed_job_ids(payload)
        restored_scan_state = deserialize_scan_state(payload)

        assert restored_items == items
        assert restored_ids == ids
        assert restored_scan_state == scan_state

        for key, value in original_untouched.items():
            assert payload[key] == value
