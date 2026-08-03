"""Serialization and deserialization for TrackedObject/ConfigContext and ScanState.

Requirement 2: the Completion_Tracker persists per-Tracked_Object
completion-tracking state (``TrackedObject``/``ConfigContext``), the flat
set of already-processed `job_id`s (``completion_processed_job_ids``), and
the per-config quiescence scan state (``ScanState``) inside the *same*
``state/<source_bucket>.json`` object already used for ``CheckpointState``,
``Lease``, and ``SubmissionRecord`` (see ``src/core/checkpoint_serializer.py``
and ``src/adapters/state_store.py``). This module owns exactly three
top-level keys in that shared payload dict — ``completion_items``,
``completion_processed_job_ids``, and ``completion_scan_state`` — and never
reads, writes, or strips any other key. Like
``checkpoint_serializer.py::deserialize``, it tolerates (and ignores) any
other top-level key already present in the payload.

This module serializes the object-level (Tracked_Object) shape described in
design.md Decision 2: ``state``/``resolved_at``/``resolution_method``/
``replication_outcome`` live on the item's top level, and ``configs`` carries
only ``job_id``/``manifest_generated_at``/``bops_confirmed`` per
``replication_config_id`` — no per-config outcome.
``serialize_scan_state``/``deserialize_scan_state`` and the processed-job-id
helpers are structurally independent of the item shape.

Round-trip guarantee (design Property 16):
``deserialize_completion_items(serialize_completion_items(items)) == items``
for any ``dict[str, TrackedObject]``;
``deserialize_processed_job_ids(serialize_processed_job_ids(ids)) == ids``
for any ``set[str]``; and
``deserialize_scan_state(serialize_scan_state(state)) == state`` for any
``dict[str, ScanState]``. Applying any of these round trips leaves every
pre-existing ``CheckpointState``/``Lease``/``submission_records`` field in
the same payload byte-for-byte unchanged.

JSON schema (design.md Decision 2 storage schema)
--------------------------------------------------
::

    {
      ... (CheckpointState / submission_records fields, untouched) ...
      "completion_items": {
        "<object_key>\u0000<version_id-or-empty>": {
          "source_bucket": "<str>",
          "object_key": "<str>",
          "version_id": "<str>|null",
          "state": "PENDING|RESOLVED",
          "resolved_at": "<ISO 8601>|null",
          "resolution_method": "<str>|null",
          "replication_outcome": "<str>|null",
          "configs": {
            "<replication_config_id>": {
              "replication_config_id": "<str>",
              "job_id": "<str>",
              "manifest_generated_at": "<ISO 8601>",
              "bops_confirmed": true
            },
            ...
          }
        },
        ...
      },
      "completion_processed_job_ids": ["<job_id>", ...],
      "completion_scan_state": {
        "<replication_config_id>": {
          "last_scan_at": "<ISO 8601>",
          "last_scan_match_count": 0
        },
        ...
      }
    }

The item key is ``"<object_key>\\x00<version_id-or-empty>"`` — NUL-joined so
an object key containing a literal comma, colon, or other separator can
never collide with the version_id boundary. ``version_id=None`` serializes
as the empty string in the key and as JSON ``null`` in the ``"version_id"``
field. ``deserialize_completion_items`` returns a dict keyed by this same
string item_key (not a tuple) — that is the shape a JSON object's keys must
take regardless, and matches what the state store reads/writes directly.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from src.core.models import (
    CompletionState,
    ConfigContext,
    ScanState,
    TrackedObject,
)


# ---------------------------------------------------------------------------
# Item-key helpers
# ---------------------------------------------------------------------------


# Sentinel distinguishing version_id=None (the null-version marker) from
# version_id="" in the item key (code-review-remediation spec Req 8.4). Real
# S3 version IDs are never empty strings, so this collision has no observed
# real-world trigger, but the key format should not silently coerce two
# distinct identities to the same string. "\x01" cannot appear in a real
# version id (S3 version IDs are URL-safe base64-like tokens) or collide
# with the "\x00" key/version separator.
_NULL_VERSION_SENTINEL = "\x01"


def _item_key(object_key: str, version_id: str | None) -> str:
    """Build the ``"<object_key>\\x00<version_id-or-sentinel>"`` item key.

    NUL-joined so an object key containing a literal comma, colon, or other
    separator can never collide with the version_id boundary.
    ``version_id=None`` (the null-version marker) is encoded as
    :data:`_NULL_VERSION_SENTINEL` rather than the empty string, so it can
    never collide with a (real, non-empty) ``version_id=""`` — code-review-
    remediation spec Req 8.4.
    """
    encoded_version = version_id if version_id is not None else _NULL_VERSION_SENTINEL
    return f"{object_key}\x00{encoded_version}"


def item_key(object_key: str, version_id: str | None) -> str:
    """Public alias of :func:`_item_key`.

    Exposed so other modules (e.g. ``src.adapters.state_store``) that need
    to compute the exact same item-key format used by this serializer's
    ``completion_items`` storage do not have to either duplicate the format
    or reach into a private, underscored helper. Both call sites MUST agree
    on this format byte-for-byte, since they round-trip through the same
    JSON.
    """
    return _item_key(object_key, version_id)


# ---------------------------------------------------------------------------
# Private helpers — ConfigContext
# ---------------------------------------------------------------------------


def _serialize_config_context(ctx: ConfigContext) -> dict[str, Any]:
    """Convert a single ``ConfigContext`` to a JSON-serializable dict."""
    return {
        "replication_config_id": ctx.replication_config_id,
        "job_id": ctx.job_id,
        "manifest_generated_at": ctx.manifest_generated_at.isoformat(),
        "bops_confirmed": ctx.bops_confirmed,
    }


def _deserialize_config_context(data: dict[str, Any]) -> ConfigContext:
    """Reconstruct a ``ConfigContext`` from a parsed JSON dict.

    Raises:
        KeyError: If a required key is absent.
        ValueError: If a datetime field cannot be parsed.
    """
    return ConfigContext(
        replication_config_id=data["replication_config_id"],
        job_id=data["job_id"],
        manifest_generated_at=datetime.fromisoformat(data["manifest_generated_at"]),
        bops_confirmed=bool(data.get("bops_confirmed", False)),
    )


# ---------------------------------------------------------------------------
# Private helpers — TrackedObject
# ---------------------------------------------------------------------------


def _serialize_tracked_object(obj: TrackedObject) -> dict[str, Any]:
    """Convert a single ``TrackedObject`` to a JSON-serializable dict."""
    result = {
        "source_bucket": obj.source_bucket,
        "object_key": obj.object_key,
        "version_id": obj.version_id,
        "state": obj.state.value,
        "resolved_at": obj.resolved_at.isoformat() if obj.resolved_at is not None else None,
        "resolution_method": obj.resolution_method,
        "replication_outcome": obj.replication_outcome,
        "configs": {
            config_id: _serialize_config_context(ctx)
            for config_id, ctx in obj.configs.items()
        },
    }
    if obj.tagged_at is not None:
        result["tagged_at"] = obj.tagged_at.isoformat()
    if obj.last_modified is not None:
        result["last_modified"] = obj.last_modified.isoformat()
    return result


def _deserialize_tracked_object(item_key: str, data: dict[str, Any]) -> TrackedObject:
    """Reconstruct a single ``TrackedObject`` from a parsed JSON dict.

    Args:
        item_key: The dict key this item was stored under in
            ``completion_items`` (used only for error messages here — the
            item's own ``object_key``/``version_id`` fields are read
            directly from ``data``, not derived from the key).
        data: The per-item dict produced by ``_serialize_tracked_object``.

    Raises:
        KeyError: If a required key is absent.
        ValueError: If ``configs`` or a per-config entry cannot be parsed.
    """
    configs_raw = data.get("configs")
    if not isinstance(configs_raw, dict):
        raise ValueError(
            f"completion_items[{item_key!r}].configs must be a JSON object, "
            f"got {type(configs_raw).__name__}"
        )
    configs = {
        config_id: _deserialize_config_context(ctx_data)
        for config_id, ctx_data in configs_raw.items()
    }
    resolved_at_raw = data.get("resolved_at")
    resolved_at = datetime.fromisoformat(resolved_at_raw) if resolved_at_raw is not None else None
    tagged_at_raw = data.get("tagged_at")
    tagged_at = datetime.fromisoformat(tagged_at_raw) if tagged_at_raw is not None else None
    last_modified_raw = data.get("last_modified")
    last_modified = datetime.fromisoformat(last_modified_raw) if last_modified_raw is not None else None
    return TrackedObject(
        source_bucket=data["source_bucket"],
        object_key=data["object_key"],
        version_id=data.get("version_id"),
        configs=configs,
        state=CompletionState(data["state"]),
        resolved_at=resolved_at,
        resolution_method=data.get("resolution_method"),
        replication_outcome=data.get("replication_outcome"),
        tagged_at=tagged_at,
        last_modified=last_modified,
    )


# ---------------------------------------------------------------------------
# Public API — completion_items
# ---------------------------------------------------------------------------


def serialize_completion_items(items: dict[str, TrackedObject]) -> dict[str, Any]:
    """Serialize a ``dict[item_key, TrackedObject]`` to its JSON-serializable form.

    Returns only the ``completion_items`` sub-dict (keyed by item_key,
    ``"<object_key>\\x00<version_id-or-empty>"``) — the caller (the state
    store) is responsible for merging this under the ``completion_items``
    key of the shared per-bucket payload dict, preserving every other key
    already present in that payload.

    Args:
        items: A dict mapping item_key -> ``TrackedObject``.

    Returns:
        A dict mapping item_key -> the item's JSON-serializable dict form,
        suitable for assignment to ``payload["completion_items"]``.
    """
    return {key: _serialize_tracked_object(obj) for key, obj in items.items()}


def deserialize_completion_items(payload: dict[str, Any]) -> dict[str, TrackedObject]:
    """Extract and reconstruct all ``TrackedObject`` objects from a raw JSON payload.

    Reads only the ``completion_items`` top-level key from ``payload``, the
    same tolerant way ``checkpoint_serializer.py::deserialize_submission_records``
    reads ``submission_records`` — this module never touches or strips any
    other key already present in ``payload`` (e.g. ``lease``,
    ``processed_window``, ``submission_records``, ``completion_scan_state``,
    ``completion_processed_job_ids``).

    Args:
        payload: The top-level parsed JSON dict from the state object.

    Returns:
        A ``dict`` mapping item_key -> ``TrackedObject``. Empty when no
        ``completion_items`` key is present. An item with an empty
        ``configs`` dict is tolerated (though that shouldn't normally
        occur) and reconstructed with an empty ``configs`` dict.

    Raises:
        KeyError: If a required per-item field is absent.
        ValueError: If ``completion_items`` is present but not a JSON object,
            or a per-item value is malformed.
    """
    raw_dict = payload.get("completion_items")
    if raw_dict is None:
        return {}
    if not isinstance(raw_dict, dict):
        raise ValueError(
            f"completion_items must be a JSON object, got {type(raw_dict).__name__}"
        )

    return {
        key: _deserialize_tracked_object(key, item_data)
        for key, item_data in raw_dict.items()
    }


# ---------------------------------------------------------------------------
# Public API — completion_processed_job_ids
# ---------------------------------------------------------------------------


def serialize_processed_job_ids(ids: set[str]) -> list[str]:
    """Serialize the ``completion_processed_job_ids`` set to a JSON list.

    Returns only the list value — the caller is responsible for assigning it
    to ``payload["completion_processed_job_ids"]``, preserving every other
    key already present in that payload.

    Args:
        ids: A set of ``job_id`` strings already merged into ``completion_items``.

    Returns:
        A list of ``job_id`` strings, suitable for assignment to
        ``payload["completion_processed_job_ids"]``.
    """
    return list(ids)


def deserialize_processed_job_ids(payload: dict[str, Any]) -> set[str]:
    """Extract ``completion_processed_job_ids`` from a raw JSON payload.

    Reads only the ``completion_processed_job_ids`` top-level key from
    ``payload``; never touches or strips any other key already present in
    ``payload``.

    Args:
        payload: The top-level parsed JSON dict from the state object.

    Returns:
        A ``set[str]`` of ``job_id`` values. Empty when no
        ``completion_processed_job_ids`` key is present.

    Raises:
        ValueError: If ``completion_processed_job_ids`` is present but not a
            JSON list, or contains a non-string entry.
    """
    raw_list = payload.get("completion_processed_job_ids")
    if raw_list is None:
        return set()
    if not isinstance(raw_list, list):
        raise ValueError(
            "completion_processed_job_ids must be a JSON list, got "
            f"{type(raw_list).__name__}"
        )
    for entry in raw_list:
        if not isinstance(entry, str):
            raise ValueError(
                "completion_processed_job_ids entries must be strings, got "
                f"{type(entry).__name__}"
            )
    return set(raw_list)


# ---------------------------------------------------------------------------
# Public API — completion_scan_state
# ---------------------------------------------------------------------------


def serialize_scan_state(scan_state: dict[str, ScanState]) -> dict[str, Any]:
    """Serialize a ``dict[replication_config_id, ScanState]`` to its JSON form.

    Returns only the ``completion_scan_state`` sub-dict (keyed by
    ``replication_config_id``) — the caller is responsible for merging this
    under the ``completion_scan_state`` key of the shared per-bucket payload
    dict, preserving every other key already present in that payload.

    Args:
        scan_state: A dict mapping ``replication_config_id`` -> ``ScanState``.

    Returns:
        A dict mapping ``replication_config_id`` -> the JSON-serializable
        dict form of that config's ``ScanState``.
    """
    return {
        config_id: {
            "last_scan_at": state.last_scan_at.isoformat(),
            "last_scan_match_count": state.last_scan_match_count,
        }
        for config_id, state in scan_state.items()
    }


def deserialize_scan_state(payload: dict[str, Any]) -> dict[str, ScanState]:
    """Extract and reconstruct all ``ScanState`` objects from a raw JSON payload.

    Reads only the ``completion_scan_state`` top-level key from ``payload``;
    never touches or strips any other key already present in ``payload``.

    Args:
        payload: The top-level parsed JSON dict from the state object.

    Returns:
        A ``dict`` mapping ``replication_config_id`` -> ``ScanState``. Empty
        when no ``completion_scan_state`` key is present.

    Raises:
        ValueError: If ``completion_scan_state`` is present but not a JSON
            object, or a per-config value is malformed.
        KeyError: If a required per-config field is absent.
    """
    raw_dict = payload.get("completion_scan_state")
    if raw_dict is None:
        return {}
    if not isinstance(raw_dict, dict):
        raise ValueError(
            "completion_scan_state must be a JSON object, got "
            f"{type(raw_dict).__name__}"
        )

    result: dict[str, ScanState] = {}
    for config_id, data in raw_dict.items():
        if not isinstance(data, dict):
            raise ValueError(
                f"completion_scan_state[{config_id!r}] must be a JSON object, "
                f"got {type(data).__name__}"
            )
        result[config_id] = ScanState(
            last_scan_at=datetime.fromisoformat(data["last_scan_at"]),
            last_scan_match_count=int(data["last_scan_match_count"]),
        )
    return result
