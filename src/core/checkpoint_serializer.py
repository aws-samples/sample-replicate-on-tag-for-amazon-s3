"""Serialization and deserialization for CheckpointState, Lease, and SubmissionRecord.

Requirement 4.3: The Journal_Monitor persists the per-bucket checkpoint and
optional lease as a JSON state object in the S3-backed state store. This module
provides the pure serialization layer between the in-memory models and the
persisted JSON form.

Round-trip guarantee: ``deserialize(serialize(state)) == state`` for any valid
``CheckpointState`` including optional ``Lease`` and the processed-operation
window (Property 10).

Submission records are stored alongside the checkpoint in the same JSON object
under the key ``submission_records``. Per design.md Decision D3, a bucket has
a single ``SubmissionRecord`` keyed by the per-bucket sentinel (the
``source_bucket`` name itself). :func:`deserialize_submission_records` iterates
``raw_dict.items()`` generically without assuming what the keys represent, so
both the per-bucket sentinel keying and older per-``replication_config_id``
keying are read without error. The public helpers
:func:`deserialize_submission_records` and :func:`serialize_submission_record`
are used by the state store to read and write these records.

JSON schema
-----------
::

    {
      "source_bucket": "<str>",
      "last_processed_watermark": "<canonical record_timestamp str>",
      "lease": {
        "lease_id": "<str>",
        "candidate_max_watermark": "<canonical record_timestamp str>",
        "acquired_at": "<ISO 8601 datetime>",
        "status": "<LeaseStatus.value>"
      } | null,
      "processed_window": [
        { "logical_operation_id": "<str>", "watermark": "<canonical str>" },
        ...
      ],
      "submission_records": {
        "<source_bucket sentinel>": {
          "replication_config_id": "<str>",
          "source_bucket": "<str>",
          "job_id": "<str>",
          "manifest_key": "<str>",
          "submitted_at": "<ISO 8601 datetime>",
          "status": "<SubmissionStatus.value>",
          "watermark_low": "<canonical str>",
          "watermark_high": "<canonical str>"
        },
        ...
      }
    }
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.core.models import (
    CheckpointState,
    Lease,
    LeaseStatus,
    ProcessedRef,
    SubmissionRecord,
    SubmissionStatus,
)
from src.core.watermark import is_plausible_watermark


# ---------------------------------------------------------------------------
# Private helpers — Lease
# ---------------------------------------------------------------------------


def _serialize_lease(lease: Lease) -> dict[str, Any]:
    """Convert a Lease to a JSON-serializable dict."""
    return {
        "lease_id": lease.lease_id,
        "candidate_max_watermark": lease.candidate_max_watermark,
        "acquired_at": lease.acquired_at.isoformat(),
        "status": lease.status.value,
    }


def _deserialize_lease(data: dict[str, Any]) -> Lease:
    """Reconstruct a Lease from a parsed JSON dict.

    Args:
        data: A dict produced by ``_serialize_lease``.

    Returns:
        The reconstructed ``Lease``.

    Raises:
        KeyError: If a required key is absent.
        ValueError: If ``acquired_at`` or ``status`` cannot be parsed.
    """
    return Lease(
        lease_id=data["lease_id"],
        candidate_max_watermark=data["candidate_max_watermark"],
        acquired_at=datetime.fromisoformat(data["acquired_at"]),
        status=LeaseStatus(data["status"]),
    )


def _serialize_window(window: list[ProcessedRef]) -> list[dict[str, str]]:
    """Convert the processed-operation window to a JSON-serializable list."""
    return [
        {"logical_operation_id": ref.logical_operation_id, "watermark": ref.watermark}
        for ref in window
    ]


def _deserialize_window(data: Any) -> list[ProcessedRef]:
    """Reconstruct the processed-operation window from parsed JSON.

    Tolerates an absent field (older state objects predating the window) by
    returning an empty list.

    Raises:
        ValueError: If the field is present but not a list, or an entry is not
            a JSON object with string fields.
    """
    if data is None:
        return []
    if not isinstance(data, list):
        raise ValueError(
            f"processed_window must be a list, got {type(data).__name__}"
        )
    window: list[ProcessedRef] = []
    for entry in data:
        if not isinstance(entry, dict):
            raise ValueError(
                f"processed_window entry must be an object, got {type(entry).__name__}"
            )
        lid = entry["logical_operation_id"]
        wm = entry["watermark"]
        if not isinstance(lid, str) or not isinstance(wm, str):
            raise ValueError(
                "processed_window entry fields must be strings"
            )
        window.append(ProcessedRef(logical_operation_id=lid, watermark=wm))
    return window


# ---------------------------------------------------------------------------
# Public helpers — SubmissionRecord
# ---------------------------------------------------------------------------


def serialize_submission_record(rec: SubmissionRecord) -> dict[str, Any]:
    """Serialize a single ``SubmissionRecord`` to a JSON-serializable dict.

    Includes ``watermark_low``, ``watermark_high``, and ``consecutive_failures``
    (may be empty strings / zero for pre-existing records).
    """
    return {
        "replication_config_id": rec.replication_config_id,
        "source_bucket": rec.source_bucket,
        "job_id": rec.job_id,
        "manifest_key": rec.manifest_key,
        "submitted_at": rec.submitted_at.isoformat(),
        "status": rec.status.value,
        "watermark_low": rec.watermark_low,
        "watermark_high": rec.watermark_high,
        "consecutive_failures": rec.consecutive_failures,
    }


def _deserialize_submission_record(data: dict[str, Any]) -> SubmissionRecord:
    """Reconstruct a ``SubmissionRecord`` from a parsed JSON dict.

    Tolerates absent ``watermark_low``, ``watermark_high``, and
    ``consecutive_failures`` for backward compatibility with records written
    before those fields existed.
    """
    return SubmissionRecord(
        replication_config_id=data["replication_config_id"],
        source_bucket=data["source_bucket"],
        job_id=data["job_id"],
        manifest_key=data["manifest_key"],
        submitted_at=datetime.fromisoformat(data["submitted_at"]),
        status=SubmissionStatus(data["status"]),
        watermark_low=data.get("watermark_low", ""),
        watermark_high=data.get("watermark_high", ""),
        consecutive_failures=int(data.get("consecutive_failures", 0)),
    )


def deserialize_submission_records(
    payload: dict[str, Any],
) -> dict[str, SubmissionRecord]:
    """Extract and reconstruct all ``SubmissionRecord`` objects from a raw JSON payload.

    Reads the ``submission_records`` key from the payload and deserializes each
    entry. The function iterates ``raw_dict.items()`` generically without
    interpreting the dict keys, so both per-bucket sentinel keying (current) and
    per-``replication_config_id`` keying are read without error.

    Returns an empty dict when ``submission_records`` is absent.

    Args:
        payload: The top-level parsed JSON dict from the state object.

    Returns:
        A ``dict`` mapping the record's key → ``SubmissionRecord``.
        Empty when no submission records are stored.
    """
    raw_dict = payload.get("submission_records")
    if raw_dict is not None:
        if not isinstance(raw_dict, dict):
            raise ValueError(
                f"submission_records must be a JSON object, got {type(raw_dict).__name__}"
            )
        return {
            config_id: _deserialize_submission_record(rec_data)
            for config_id, rec_data in raw_dict.items()
        }

    return {}


# ---------------------------------------------------------------------------
# Public API — CheckpointState
# ---------------------------------------------------------------------------


def serialize(state: CheckpointState) -> str:
    """Serialize a CheckpointState to a JSON string for S3 object storage.

    Datetimes are encoded in ISO 8601 format (preserving any timezone
    information). The ``lease`` field is ``null`` when no lease is held.
    The ``submission_records`` field is not included here — it is managed
    separately by the state store alongside this payload.

    Args:
        state: The ``CheckpointState`` to serialize.

    Returns:
        A JSON string representing the persisted form of ``state``.
    """
    payload: dict[str, Any] = {
        "source_bucket": state.source_bucket,
        "last_processed_watermark": state.last_processed_watermark,
        "lease": _serialize_lease(state.lease) if state.lease is not None else None,
        "processed_window": _serialize_window(state.processed_window),
    }
    return json.dumps(payload)


def deserialize(data: str) -> CheckpointState:
    """Deserialize a JSON string back into a CheckpointState.

    This is the inverse of :func:`serialize`. Datetime fields are parsed from
    ISO 8601 strings; timezone information is preserved.  Extra keys in the
    JSON payload (e.g. ``submission_records``, ``submission_record``) are
    silently ignored — they are consumed by the state store via
    :func:`deserialize_submission_records`.

    Args:
        data: A JSON string produced by :func:`serialize`.

    Returns:
        The reconstructed ``CheckpointState``.

    Raises:
        json.JSONDecodeError: If ``data`` is not valid JSON.
        KeyError: If a required field is missing from the JSON.
        ValueError: If a field value cannot be parsed (bad datetime or
            unrecognised LeaseStatus value), if the top-level payload is not a
            JSON object, if ``source_bucket`` / ``last_processed_watermark``
            is not a string, or if ``last_processed_watermark`` is not a
            plausible canonical watermark.

    Security note
    -------------
    The state object lives in the scratch/state bucket. An actor able to write
    that object could otherwise poison the checkpoint with a value of the wrong
    type — e.g. an integer or object, which would later crash downstream
    comparisons. This function enforces the expected string types up front so a
    malformed state object is rejected with a clear ``ValueError`` at read time
    (surfaced as a per-bucket skip) instead of crashing downstream.

    ``last_processed_watermark`` is additionally bounded by
    :func:`~src.core.watermark.is_plausible_watermark`: it must be the epoch
    watermark or a canonical, fixed-width watermark no more than
    :data:`~src.core.watermark.MAX_FUTURE_SKEW` ahead of now. Without that
    bound a well-typed far-future value (``"9999-12-31T23:59:59.000000Z"``)
    would make every subsequent journal query return zero rows and silently
    halt replication for the bucket. Restricting write access to the state
    bucket at the bucket-policy / IAM layer remains the primary control; this
    is defence in depth.
    """
    payload: Any = json.loads(data)
    if not isinstance(payload, dict):
        raise ValueError(
            f"checkpoint state must be a JSON object, got {type(payload).__name__}"
        )

    source_bucket = payload["source_bucket"]
    if not isinstance(source_bucket, str):
        raise ValueError(
            "source_bucket must be a string, got "
            f"{type(source_bucket).__name__}"
        )

    watermark = payload["last_processed_watermark"]
    if not isinstance(watermark, str):
        raise ValueError(
            "last_processed_watermark must be a string, got "
            f"{type(watermark).__name__}"
        )
    if not is_plausible_watermark(watermark):
        raise ValueError(
            "last_processed_watermark is not a plausible canonical watermark: "
            f"{watermark!r}. Expected the epoch watermark or a "
            "YYYY-MM-DDTHH:MM:SS.ffffffZ value no more than 24 hours ahead "
            "of now."
        )

    lease_data = payload.get("lease")
    lease = _deserialize_lease(lease_data) if lease_data is not None else None
    window = _deserialize_window(payload.get("processed_window"))
    return CheckpointState(
        source_bucket=source_bucket,
        last_processed_watermark=watermark,
        lease=lease,
        processed_window=window,
    )
