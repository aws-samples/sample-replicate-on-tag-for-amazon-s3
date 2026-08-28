"""S3-object-backed state store for per-bucket checkpoint and concurrency lease.

Uses S3 conditional writes (``If-Match`` / ``If-None-Match``) to provide atomic
compare-and-set semantics for the per-bucket CheckpointState without an external
database.

State object key: ``state/<source_bucket>.json`` within the state bucket.

JSON schema of the persisted object
------------------------------------
The core schema is the ``CheckpointState`` schema from
``src.core.checkpoint_serializer``.  The state store additionally persists a
``submission_records`` dict in the same object so that a single conditional
``PutObject`` can update the checkpoint, the lease, *and* the latest
submission record atomically.

The per-bucket disable flag (``disabled``, ``disabled_reason``,
``disabled_at``) is persisted here too, written by :meth:`StateStore.disable_bucket`
and read by :meth:`StateStore.get_disable_state`.  It is the one part of this
object an operator is expected to edit by hand: setting ``disabled`` to
``false`` re-enables the bucket on the next scheduled run.  See
:class:`~src.core.models.BucketDisableState` for why it lives here rather than
in Solution_Config.

``submission_records`` holds one entry per outstanding or unsettled Batch
Operations job for the bucket, keyed by ``job_id``.
:meth:`StateStore.get_submission_records` re-keys by each record's own
``job_id`` on read regardless of the stored key, and
:meth:`StateStore.record_submission` merges its own entry without disturbing
another job's. A bucket may therefore have several outstanding jobs at once, up
to ``MaxConcurrentJobsPerBucket``; the orchestrator counts them before deciding
whether to submit.

::

    {
      "source_bucket": "<str>",
      "last_processed_watermark": "<canonical record_timestamp str>",
      "lease": { ... } | null,
      "processed_window": [ ... ],
      "disabled": true,                    // absent when the bucket is enabled
      "disabled_reason": "<str>",          // absent when the bucket is enabled
      "disabled_at": "<ISO 8601>",         // absent when the bucket is enabled
      "submission_records": {
        "<job_id>": {
          "replication_config_id": "<str>",
          "source_bucket": "<str>",
          "job_id": "<str>",
          "manifest_key": "<str>",
          "submitted_at": "<ISO 8601>",
          "status": "<SubmissionStatus.value>",
          "watermark_low": "<canonical record_timestamp str>",
          "watermark_high": "<canonical record_timestamp str>"
        },
        ...
      },
      "completion_items": {
        "<object_key>\u0000<version_id-or-empty>": {
          "source_bucket": "<str>",
          "object_key": "<str>",
          "version_id": "<str>|null",
          "state": "PENDING|RESOLVED",
          "resolved_at": "<ISO 8601>|null",
          "resolution_method": "<str>|null",
          "replication_outcome": "<str>|null",
          "tagged_at": "<ISO 8601>",           // omitted when unknown
          "last_modified": "<ISO 8601>",       // omitted when unknown
          "matched_rules": ["<rule_id>", ...],       // omitted when empty
          "destinations": ["<bucket name>", ...],    // omitted when empty
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
      "completion_timestamps": {
        "<object_key>\u0000<version_id-or-empty>": {
          "tagged_at": "<ISO 8601>",
          "last_modified": "<ISO 8601>"
        },
        ...
      },
      "completion_routing": {
        "<object_key>\u0000<version_id-or-empty>": {
          "matched_rules": ["<rule_id>", ...],
          "destinations": ["<bucket name>", ...]
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

``completion_timestamps`` and ``completion_routing`` are side maps written at
manifest-generation time and read back by :meth:`StateStore.merge_completion_configs`,
which creates the ``TrackedObject`` on a later invocation once the job's BOPS
report arrives. Both are keyed identically to ``completion_items`` and are
pruned by :meth:`StateStore.delete_completion_items` when the item they
describe is published and removed.

``completion_items`` is keyed by an item key (not a ``job_id``), since one
Tracked_Object's item can span multiple jobs across multiple replication
rules — see ``src.core.completion_serializer`` and design.md Decision 2.
``completion_processed_job_ids`` is the flat idempotency-gate set: it
records every ``job_id`` that has already had its manifest entries merged
into ``completion_items`` (design.md Decision 6).

A record whose job has settled — terminal, diagnosed, and either merged or with
completion tracking switched off — is pruned by the next
:meth:`record_submission`, in that same conditional write. A terminal record
whose report has *not* been read is deliberately kept, because it is the record
``check_report_handler`` needs in order to raise the missing-report alert.

Requirements: 4.1, 4.2, 4.3, 7.4, 9.1, 9.3, 9.4
"""
from __future__ import annotations

import json
from collections.abc import Collection
from datetime import UTC, datetime, timedelta
from typing import Any

from botocore.exceptions import ClientError

from src.adapters.bops_report_reader import BopsCompletionReport
from src.core.checkpoint_logic import advance_checkpoint
from src.core.checkpoint_serializer import (
    deserialize,
    deserialize_submission_record,
    serialize,
    serialize_submission_record,
)
from src.core import completion_serializer, completion_tracker, observability
from src.core.models import (
    BucketDisableState,
    CheckpointState,
    CompletionState,
    ConfigContext,
    Lease,
    ProcessedRef,
    ScanState,
    SubmissionRecord,
    TrackedObject,
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class ConditionalWriteError(Exception):
    """Raised when an S3 conditional ``PutObject`` fails due to an ETag mismatch.

    The caller should treat this as a lost update or stale lease and retry
    from a fresh :meth:`StateStore.get_checkpoint` call.
    """


# ---------------------------------------------------------------------------
# Object key and payload field names
# ---------------------------------------------------------------------------

# Key in a bucket's state payload mapping bucket name to a consecutive
# submission-failure count, cleared by a successful submission.
_SUBMISSION_FAILURE_STREAK_FIELD = "submission_failure_streaks"

# Key in a bucket's state payload mapping bucket name to the ISO 8601 time of
# the most recent journal-unavailable alert. A timestamp rather than a count
# because it expires on its own, so nothing has to clear it on the healthy path.
_JOURNAL_UNAVAILABLE_ALERT_FIELD = "journal_unavailable_alerts"

# Keys in a bucket's state payload recording that the bucket is disabled and
# why. Written by the circuit breaker via :meth:`StateStore.disable_bucket`,
# read by :meth:`StateStore.get_disable_state`, and cleared by the operator
# setting ``disabled`` to ``false``. See
# :class:`~src.core.models.BucketDisableState` for why they live here rather
# than in Solution_Config or on ``CheckpointState``.
_DISABLED_FIELD = "disabled"
_DISABLED_REASON_FIELD = "disabled_reason"
_DISABLED_AT_FIELD = "disabled_at"

# Component name on this module's structured log entries. Matches the naming of
# the other components in observability's entries so an operator filtering by
# component gets state-object problems as their own stream.
_COMPONENT = "State_Store"

# How many records above ``MaxConcurrentJobsPerBucket`` may accumulate before
# the oldest are evicted. Headroom rather than an exact bound because a record
# is legitimately retained past its job's terminal status: it stays until the
# report is read, and the unconsumed-report alert fires 48 hours after the
# report is written, which is many intervals at any permitted
# ``CheckFrequencyMinutes``. Reaching the ceiling therefore means an operator
# was told and did not act, so eviction is a backstop against unbounded growth
# rather than part of the normal lifecycle.
SUBMISSION_RECORD_CEILING_HEADROOM = 20


def submission_record_ceiling(max_concurrent_jobs: int) -> int:
    """The hard ceiling on stored submission records for one bucket.

    Public so the orchestrator and its tests read the same arithmetic rather
    than each restating ``+ 20``.
    """
    return max_concurrent_jobs + SUBMISSION_RECORD_CEILING_HEADROOM


def _submitted_at_sort_key(record: SubmissionRecord) -> datetime:
    """``record.submitted_at``, made comparable across records.

    A hand-edited state object can carry a naive ``submitted_at``, and sorting
    naive against aware datetimes raises ``TypeError``. Assuming UTC for a naive
    value keeps the eviction ordering total, which matters more here than the
    hour it may be wrong by: the alternative is an exception on the write that
    persists a submission record.
    """
    submitted_at = record.submitted_at
    if submitted_at.tzinfo is None:
        return submitted_at.replace(tzinfo=UTC)
    return submitted_at


def _discard_alert_suppression(
    payload: dict[str, Any],
    removed_job_ids: Collection[str],
) -> None:
    """Drop suppression entries for jobs whose records have gone, in place.

    ``completion_report_alerted_configs`` is keyed by ``job_id``, and the only
    other thing that clears an entry is a report finally being merged. An entry is
    written precisely because a report is missing or unreadable, which is a
    condition that need never recover: an expired report is the alert's own named
    likely cause. Once the record is pruned or evicted, nothing is left that could
    ever match the entry, so without this the list grows for the life of the stack
    inside an object rewritten on nearly every operation.

    Bucket-name keying bounded this at one entry per bucket, which is why it was
    not a concern before.
    """
    if not removed_job_ids:
        return
    existing = payload.get("completion_report_alerted_configs")
    if not existing:
        return
    remaining = [entry for entry in existing if entry not in set(removed_job_ids)]
    if len(remaining) != len(existing):
        payload["completion_report_alerted_configs"] = remaining


def _submission_records_by_job_id(
    payload: dict[str, Any],
    source_bucket: str = "",
) -> dict[str, SubmissionRecord]:
    """Read ``submission_records`` from *payload*, keyed by each record's ``job_id``.

    Re-keying on read is what makes the move to per-job keying free. A state
    object written by 1.0.1, or by the interim 1.1.0 build, holds a single entry
    keyed by the bucket's own name; every ``SubmissionRecord`` carries its own
    ``job_id`` regardless, so that entry loads correctly here with no migration
    write, no schema version, and nothing for an operator to do. The first
    :meth:`StateStore.record_submission` after the upgrade persists the new
    keying as a side effect.

    Deserialization is isolated per entry, and a bad entry is dropped with an
    error rather than taking the whole dict down. This matters because
    :meth:`StateStore.record_submission` now reads the existing records in order
    to merge into them. Before per-job keying it overwrote them unread, so a
    corrupt entry healed itself on the next write; now one unparseable entry would
    otherwise make every subsequent read *and* write raise, leaving the bucket
    submitting jobs it never records. The state object's ``disabled`` flag is
    documented as hand-editable, so a hand-edited neighbouring key is a real
    shape rather than a hypothetical one.

    Two entries are dropped:

    * One that will not deserialize — a missing required key, an unparseable
      ``submitted_at``, or a ``status`` outside the enum.
    * One whose ``job_id`` is empty. It identifies no job, so nothing can be
      described, merged, or rolled back from it, and keeping it would put a ``""``
      key in the returned dict.

    Both are reported, because every other path in this module that discards
    persisted state reports it.

    Requirements: 1.1, 1.2
    """
    raw = payload.get("submission_records")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=source_bucket,
            cause=(
                f"submission_records is a {type(raw).__name__}, not a JSON object. "
                f"No prior Batch Operations job can be tracked for this bucket "
                f"until it is corrected or removed."
            ),
        ))
        return {}

    records: dict[str, SubmissionRecord] = {}
    for stored_key, stored in raw.items():
        try:
            record = deserialize_submission_record(stored)
        except Exception as exc:  # noqa: BLE001 — one bad entry must not lose the rest
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=source_bucket,
                cause=(
                    f"Dropped an unreadable submission record stored under key "
                    f"{stored_key!r}: {exc}. Its job is no longer tracked: no "
                    f"completion report will be read for it and it cannot be "
                    f"rolled back. The remaining records are unaffected."
                ),
            ))
            continue
        if not record.job_id:
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=source_bucket,
                cause=(
                    f"Dropped the submission record stored under key "
                    f"{stored_key!r}: it carries no job_id, so it identifies no "
                    f"Batch Operations job and nothing can be recovered from it."
                ),
            ))
            continue
        records[record.job_id] = record
    return records


def state_object_key(source_bucket: str) -> str:
    """Return the S3 object key for a bucket's state object.

    Public because the bucket-disabled recovery instructions name this key: the
    operator's recovery step is editing this object, so the message has to be
    able to print its exact location rather than describe it.
    """
    return f"state/{source_bucket}.json"


def _default_state(source_bucket: str) -> CheckpointState:
    """Return a fresh ``CheckpointState`` for a bucket with no persisted state.

    The initial ``last_processed_watermark`` is the empty string (the epoch
    watermark), so that every journal record — whose canonical
    ``record_timestamp`` watermark is any non-empty string — is greater and
    therefore eligible for processing on the first run.
    """
    return CheckpointState(
        source_bucket=source_bucket,
        last_processed_watermark="",
        lease=None,
    )


def _read_state_payload(
    s3_client: Any,
    state_bucket: str,
    key: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Read and parse the state object, handling NoSuchKey/404.

    Returns (payload_dict, etag). On NoSuchKey/404, returns (None, None)
    so that each caller can apply its own absent-object behaviour.

    Raises:
        ClientError: For S3 errors other than NoSuchKey/404.
    """
    try:
        response = s3_client.get_object(Bucket=state_bucket, Key=key)
        raw_body = response["Body"].read().decode("utf-8")
        etag = response["ETag"]
        payload: dict[str, Any] = json.loads(raw_body)
        return payload, etag
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            return None, None
        raise


def _write_state_payload(
    s3_client: Any,
    state_bucket: str,
    key: str,
    payload: dict[str, Any],
    expected_etag: str | None,
    kms_key_arn: str | None = None,
) -> str:
    """Serialize a payload dict and write it via _put_object_conditional.

    Returns the new ETag from S3.
    """
    body = json.dumps(payload)
    return _put_object_conditional(
        s3_client, state_bucket, key, body, expected_etag, kms_key_arn
    )


def _put_object_conditional(
    s3_client: Any,
    bucket: str,
    key: str,
    body: str,
    expected_etag: str | None,
    kms_key_arn: str | None = None,
) -> str:
    """Write an object using S3 conditional-write semantics.

    When ``expected_etag`` is ``None``, uses ``If-None-Match: *``
    (create-only — fails if the object already exists).
    Otherwise uses ``If-Match: <expected_etag>``
    (update-only compare-and-set — fails if the ETag has changed).

    When ``kms_key_arn`` is provided, the object is encrypted with
    SSE-KMS using that key; otherwise S3 applies the bucket's default
    encryption (SSE-S3 or a bucket-default KMS key).

    Returns:
        The new ETag returned by S3 on success.

    Raises:
        ConditionalWriteError: When S3 rejects the write because the
            precondition is not satisfied (stale ETag or object already exists
            for a create attempt).
        ClientError: For any other S3 error.
    """
    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "Body": body.encode("utf-8"),
        "ContentType": "application/json",
    }
    if kms_key_arn:
        kwargs["ServerSideEncryption"] = "aws:kms"
        kwargs["SSEKMSKeyId"] = kms_key_arn
    if expected_etag is None:
        kwargs["IfNoneMatch"] = "*"
    else:
        kwargs["IfMatch"] = expected_etag

    try:
        response = s3_client.put_object(**kwargs)
        return response["ETag"]
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("PreconditionFailed", "ConditionalRequestConflict"):
            raise ConditionalWriteError(
                f"Conditional write failed for {bucket}/{key} "
                f"(expected_etag={expected_etag!r}): {error_code}"
            ) from exc
        raise


# ---------------------------------------------------------------------------
# StateStore
# ---------------------------------------------------------------------------


class StateStore:
    """S3-object-backed per-bucket state store.

    Persists one small JSON state object per Monitored_Bucket in the
    source-side scratch/state S3 bucket, keyed as
    ``state/<source_bucket>.json``.

    All mutations are conditional writes:

    * ``PutObject`` with ``If-Match: <ETag>`` to update an existing object
      (compare-and-set — the write fails if any concurrent modification changed
      the object since it was last read).
    * ``PutObject`` with ``If-None-Match: *`` to create the object the first
      time (fails if the object already exists, so the caller falls back to a
      normal ``If-Match`` update after reading the existing ETag).

    This gives checkpoint advancement and lease operations the same
    compare-and-set guarantee previously expected from a database, without
    standing up one.  The lease is embedded *inside* the same state object
    (guarded by the object's ETag) rather than as a separate marker object, so
    a single conditional ``PutObject`` updates the checkpoint and the lease
    atomically.
    """

    def __init__(self, kms_key_arn: str | None = None) -> None:
        """Create a state store.

        Parameters
        ----------
        kms_key_arn:
            Optional KMS key ARN. When provided, every state object written by
            this store is encrypted with SSE-KMS using that key. When ``None``,
            S3 applies the bucket's default encryption.
        """
        self._kms_key_arn = kms_key_arn

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_checkpoint(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> tuple[CheckpointState, str | None]:
        """Read the current ``CheckpointState`` and the object's ETag.

        Issues a ``GetObject`` call.  When the state object does not yet exist
        (first run for this bucket), returns a default ``CheckpointState``
        with an empty sequence number and ``None`` as the ETag.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to read.

        Returns:
            A ``(CheckpointState, etag)`` tuple.  ``etag`` is ``None`` when
            no state object exists yet (i.e., the first run for this bucket).

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.
        """
        key = state_object_key(source_bucket)
        payload, etag = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return _default_state(source_bucket), None
        state = deserialize(json.dumps(payload))
        # A lease present in the payload but absent from the parsed state was
        # discarded by _deserialize_lease as untrustworthy (see its docstring).
        # Discarding is what stops a poisoned lease from blocking the bucket
        # forever, but it is state being dropped, so it is never silent.
        if payload.get("lease") is not None and state.lease is None:
            observability.emit(
                observability.log_audit(
                    action="lease_discarded",
                    source_bucket=source_bucket,
                    details={
                        "reason": (
                            "persisted lease was malformed or carried an "
                            "implausible candidate_max_watermark"
                        ),
                    },
                )
            )
        # Defense-in-depth: verify the source_bucket recorded inside the
        # JSON matches the bucket whose state key we read. A mismatch
        # indicates the state object was tampered with (e.g. content of
        # bucket-a's state copied into bucket-b's key), which could cause
        # checkpoint misdirection. Reject and surface as a per-bucket skip.
        if state.source_bucket != source_bucket:
            raise ValueError(
                f"State object integrity check failed: object key is for "
                f"bucket {source_bucket!r} but embedded source_bucket is "
                f"{state.source_bucket!r}. Possible state-object tampering."
            )
        return state, etag

    # ------------------------------------------------------------------
    # Write — checkpoint
    # ------------------------------------------------------------------

    def put_checkpoint(
        self,
        s3_client: Any,
        state_bucket: str,
        state: CheckpointState,
        expected_etag: str | None,
    ) -> str:
        """Persist a ``CheckpointState`` using a conditional write.

        When ``expected_etag`` is ``None``, uses ``If-None-Match: *``
        (creates the state object for the first time).
        Otherwise uses ``If-Match: <expected_etag>``
        (compare-and-set update — guards against lost updates).

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            state:         The new ``CheckpointState`` to persist.
            expected_etag: The ETag the caller last observed, or ``None``
                           for a create-only write.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the conditional precondition fails
                (ETag mismatch or object-already-exists for a create).
            ClientError: For other S3 errors.

        Note:
            Reads the current raw JSON payload first (tolerating a missing
            object) and overlays only the four ``CheckpointState`` fields
            (``source_bucket``, ``last_processed_watermark``, ``lease``,
            ``processed_window``) onto it before writing. This preserves
            every sibling key not owned by ``CheckpointState`` —
            ``submission_records``, the ``disabled`` flag and its two
            companions, ``completion_items``,
            ``completion_processed_job_ids``, ``completion_scan_state``, and
            ``completion_report_alerted_configs`` — mirroring the
            read-raw-JSON, mutate-one-key pattern used by every other mutator
            in this module (``record_submission``, ``merge_completion_configs``,
            etc.). Without this merge, ``acquire_lease``/``release_lease``
            (which both persist via this method) would silently wipe every
            completion-tracking and submission key on every call.
        """
        key = state_object_key(state.source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            payload = {}

        # Overlay only the CheckpointState-owned keys; every other key already
        # present in the raw payload (submission_records, disabled,
        # disabled_reason, disabled_at, completion_items,
        # completion_processed_job_ids, completion_scan_state,
        # completion_report_alerted_configs) is left untouched. The disable
        # keys matter most here: this overlay is what stops the end-of-interval
        # release_lease from wiping a disable recorded mid-run.
        payload.update(json.loads(serialize(state)))
        return _write_state_payload(
            s3_client, state_bucket, key, payload, expected_etag, self._kms_key_arn
        )

    # ------------------------------------------------------------------
    # Write — lease
    # ------------------------------------------------------------------

    def acquire_lease(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        lease: Lease,
        current_etag: str | None,
    ) -> str:
        """Atomically embed a ``Lease`` in the state object.

        Reads the current ``CheckpointState`` (to preserve the checkpoint
        value), replaces the lease field, and writes the updated state back
        using a conditional write guarded by ``current_etag``.

        When ``current_etag`` is ``None`` the state object does not yet exist
        and ``If-None-Match: *`` is used to create it.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to update.
            lease:         The ``Lease`` to embed.
            current_etag:  The ETag returned by the caller's most recent
                           ``get_checkpoint`` call, or ``None`` for a
                           create-only write.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's ``get_checkpoint`` and this call.
            ClientError: For other S3 errors.
        """
        # Re-read to obtain the latest checkpoint values so that we do not
        # lose any checkpoint advancement that happened before this call.
        state, _ = self.get_checkpoint(s3_client, state_bucket, source_bucket)
        new_state = CheckpointState(
            source_bucket=state.source_bucket,
            last_processed_watermark=state.last_processed_watermark,
            lease=lease,
            processed_window=list(state.processed_window),
        )
        new_etag = self.put_checkpoint(
            s3_client, state_bucket, new_state, current_etag
        )
        # Audit: a lease was acquired (blocks concurrent runs for this bucket).
        # Emitted only after the conditional write succeeds.
        observability.emit(
            observability.log_audit(
                action="lease_acquired",
                source_bucket=source_bucket,
                details={
                    "lease_id": lease.lease_id,
                    "candidate_max_watermark": (
                        lease.candidate_max_watermark
                    ),
                    "lease_status": lease.status.value,
                },
            )
        )
        return new_etag

    def release_lease(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        submitted_refs: list[ProcessedRef] | None,
        lookback: timedelta,
        current_etag: str,
        candidate_max_watermark: str | None = None,
    ) -> str:
        """Advance the watermark and clear the lease atomically.

        Reads the current ``CheckpointState``, calls
        :func:`~src.core.checkpoint_logic.advance_checkpoint` with
        ``submitted_refs`` (which advances the ``record_timestamp`` watermark
        and updates the bounded processed-operation window on success, and
        always clears the lease), and writes the updated state back using a
        conditional write guarded by ``current_etag``.

        When ``submitted_refs`` is ``None`` or empty (job creation or submission
        failed), the watermark and window are left unchanged but the lease is
        still cleared, so the next run is not blocked from reprocessing the same
        candidate range (Requirement 9.3).

        Args:
            s3_client:      A boto3 S3 client.
            state_bucket:   The scratch/state bucket name.
            source_bucket:  The Monitored_Bucket whose state to update.
            submitted_refs: The operations included in a successfully submitted
                            job (each with its ``logical_operation_id`` and
                            canonical watermark), or ``None`` when nothing was
                            submitted.
            lookback:       The lookback window used to bound the
                            processed-operation window.
            current_etag:   The ETag returned by the caller's most recent
                            conditional write (``acquire_lease`` or
                            ``get_checkpoint``).
            candidate_max_watermark:
                            The high-water mark over all eligible operations of
                            the interval.  On success the watermark advances to
                            at least this value, so it passes non-matching and
                            delete-filtered records that carry no ref
                            (retag-suppression Requirement 2.5).

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.
        """
        # Re-read to obtain the latest state (including any watermark that
        # may have been advanced by a prior release_lease call).
        state, _ = self.get_checkpoint(s3_client, state_bucket, source_bucket)
        new_state = advance_checkpoint(
            state, submitted_refs, lookback, candidate_max_watermark
        )
        new_etag = self.put_checkpoint(
            s3_client, state_bucket, new_state, current_etag
        )
        # Audit: lease released and (conditionally) watermark advanced.
        # Records the from/to watermark so an investigation can reconstruct
        # exactly which journal position became the new resume point and
        # therefore which objects are henceforth considered already processed.
        # Emitted only after the conditional write succeeds.
        old_wm = state.last_processed_watermark
        new_wm = new_state.last_processed_watermark
        observability.emit(
            observability.log_audit(
                action="lease_released",
                source_bucket=source_bucket,
                details={
                    "checkpoint_from": old_wm,
                    "checkpoint_to": new_wm,
                    "checkpoint_advanced": new_wm != old_wm,
                    "submitted_operations": len(submitted_refs or []),
                },
            )
        )
        return new_etag

    # ------------------------------------------------------------------
    # Read — submission records
    # ------------------------------------------------------------------

    def get_submission_records(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> dict[str, SubmissionRecord]:
        """Read every ``SubmissionRecord`` for a bucket, keyed by ``job_id``.

        One entry per outstanding or unsettled job. The dict key is each
        record's own ``job_id``, whatever key it was stored under, so a state
        object written by 1.0.1 or by the interim 1.1.0 build — a single entry
        keyed by the bucket's name — loads correctly with no migration write and
        no upgrade step. A record whose ``job_id`` is empty is dropped, since it
        identifies no job. See :func:`_submission_records_by_job_id`.

        Returns an empty dict when no submission records are stored or the state
        object does not yet exist.

        This method should be called right after :meth:`get_checkpoint` so that
        the records from the previous run are available for the failed-job
        recovery check and the outstanding-job count before any conditional
        write touches them.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose records to read.

        Returns:
            A ``dict`` mapping ``job_id`` → ``SubmissionRecord``.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 1.2, 1.3, 4.1, 4.2
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return {}

        return _submission_records_by_job_id(payload, source_bucket)

    # ------------------------------------------------------------------
    # Write — submission record
    # ------------------------------------------------------------------

    def record_submission(
        self,
        s3_client: Any,
        state_bucket: str,
        submission: SubmissionRecord,
        current_etag: str,
        *,
        terminal_job_ids: Collection[str] = (),
        completion_tracking_enabled: bool = True,
        max_concurrent_jobs: int | None = None,
    ) -> str:
        """Persist a ``SubmissionRecord`` into the per-bucket state object.

        The submission record is stored alongside the ``CheckpointState`` in
        the same JSON object under the ETag guard, so that the record and the
        checkpoint remain in a consistent state (Requirement 7.4).

        The serialized ``CheckpointState`` fields (source_bucket,
        last_processed_watermark, lease, processed_window) are preserved
        verbatim. ``submission_records`` gains or updates the entry for
        ``submission.job_id`` and **no other entry is disturbed**, so a job
        submitted while an earlier one is still running cannot displace it.

        Overwriting was the defect this replaces. With one record per bucket, a
        second submission discarded the running job's id, and with it the read
        of that job's completion report, the report-missing check that would
        have noticed, and any ``watermark_low`` to roll back to had the job later
        failed — silent data loss, not merely missing reporting.

        Every other entry is read back through
        :func:`_submission_records_by_job_id`, so a bucket-name-keyed entry left
        by 1.0.1 or by the interim 1.1.0 build is re-keyed by its own ``job_id``
        as a side effect of this write, and a record carrying no ``job_id`` is
        dropped.

        Pruning and the ceiling both happen in this same conditional write, so
        neither costs an extra round trip nor can leave a half-pruned state:

        * A record is pruned when its job has **settled**: present in
          *terminal_job_ids*, ``report_diagnosed``, and either in
          ``completion_processed_job_ids`` or with completion tracking switched
          off for the stack. Both halves matter. Terminal alone would delete
          exactly the records ``check_report_handler`` needs to raise a
          missing-report alert; requiring the processed set alone would never
          prune anything on a stack with completion tracking off, since nothing
          populates that set.
        * Above :func:`submission_record_ceiling`, the oldest records by
          ``submitted_at`` are evicted and each eviction emits an error naming
          the ``job_id``. An error rather than an audit entry: audit records a
          decision the Solution is entitled to make, and discarding tracking
          state for a job whose outcome is unknown is a loss.

        ``watermark_low`` and ``watermark_high`` (set by the orchestrator at
        submission time) are included in the persisted record so that the next
        run's recovery check can determine how far back to roll the watermark if
        this job is found to have failed.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            submission:    The ``SubmissionRecord`` to persist.
            current_etag:  The ETag returned by the caller's most recent
                           conditional write.
            terminal_job_ids:
                           Job IDs the caller observed at a terminal status this
                           run. Empty prunes nothing, which is the safe default:
                           a caller that did not describe the prior jobs knows
                           of no settled record. The record's own ``status``
                           field cannot serve here — it is written as
                           ``SUBMITTED`` and never updated.
            completion_tracking_enabled:
                           Whether the stack has a completion-report topic. Not
                           discoverable at this layer, so it is passed in.
            max_concurrent_jobs:
                           The bucket's concurrency limit, from which the record
                           ceiling is derived. ``None`` enforces no ceiling.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 1.1, 3.1, 3.2, 3.3, 4.1, 4.2, 7.4
        """
        source_bucket = submission.source_bucket
        key = state_object_key(source_bucket)

        # Read the current raw JSON so that we preserve all existing fields
        # (checkpoint + lease + other submission records) while
        # adding/replacing the entry for this job.
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            # No state object yet — start from the default checkpoint.
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        records = _submission_records_by_job_id(payload, source_bucket)
        before = set(records)
        # An empty job_id would key this entry as "", which the next read drops.
        # Nothing upstream produces one — CreateJob assigns the id — so this is
        # left to self-clean rather than guarded against.
        records[submission.job_id] = submission

        self._prune_settled_records(
            records,
            payload=payload,
            keep_job_id=submission.job_id,
            terminal_job_ids=terminal_job_ids,
            completion_tracking_enabled=completion_tracking_enabled,
        )
        if max_concurrent_jobs is not None:
            self._evict_above_ceiling(
                records,
                source_bucket=source_bucket,
                keep_job_id=submission.job_id,
                terminal_job_ids=terminal_job_ids,
                ceiling=submission_record_ceiling(max_concurrent_jobs),
            )

        _discard_alert_suppression(payload, before - set(records))

        payload["submission_records"] = {
            job_id: serialize_submission_record(record)
            for job_id, record in records.items()
        }

        # Remove the legacy singular field if it was present so the state
        # object doesn't carry both forms.
        payload.pop("submission_record", None)

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    @staticmethod
    def _prune_settled_records(
        records: dict[str, SubmissionRecord],
        *,
        payload: dict[str, Any],
        keep_job_id: str,
        terminal_job_ids: Collection[str],
        completion_tracking_enabled: bool,
    ) -> None:
        """Drop every settled record from *records*, in place.

        *keep_job_id* is the job being written by this call and is never
        considered: it was submitted moments ago and cannot have settled.

        Requirements: 3.1, 3.2
        """
        terminal = set(terminal_job_ids)
        if not terminal:
            return
        processed = completion_serializer.deserialize_processed_job_ids(payload)
        for job_id, record in list(records.items()):
            if job_id == keep_job_id or job_id not in terminal:
                continue
            if not record.report_diagnosed:
                continue
            if completion_tracking_enabled and job_id not in processed:
                # Terminal and diagnosed, but its report has not been merged.
                # This is precisely the record check_report_handler reads to
                # raise the missing-or-unconsumed report alert, so it stays.
                continue
            del records[job_id]

    @staticmethod
    def _evict_above_ceiling(
        records: dict[str, SubmissionRecord],
        *,
        source_bucket: str,
        keep_job_id: str,
        terminal_job_ids: Collection[str],
        ceiling: int,
    ) -> None:
        """Evict from *records*, in place, down to *ceiling*.

        A backstop against a record that can never settle — the clearest case
        being a report the State Bucket lifecycle rule expired before it was
        read. Each eviction is reported as an error, not an audit entry, because
        it discards tracking state for a job whose outcome is still unknown.

        A job observed at a terminal status this run is evicted before one that
        has not finished, and only then is age the tiebreaker. Age alone would put
        the longest-running job first, and on a bandwidth-bound bucket — the
        workload the concurrency bound exists for — that is a job that may still
        be replicating. Evicting it discards the ``watermark_low`` its rollback
        would need and the report its objects would be counted from, which is
        exactly the silent loss this design removes. A terminal record has already
        had its recovery effect, so losing it costs a report rather than a retry.

        Requirements: 3.3
        """
        if len(records) <= ceiling:
            return
        terminal = set(terminal_job_ids)
        evictable = sorted(
            (
                (job_id, record)
                for job_id, record in records.items()
                if job_id != keep_job_id
            ),
            key=lambda item: (
                item[0] not in terminal,
                _submitted_at_sort_key(item[1]),
            ),
        )
        for job_id, record in evictable:
            if len(records) <= ceiling:
                return
            del records[job_id]
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=source_bucket,
                cause=(
                    f"Evicted submission record for job {job_id!r} "
                    f"(submitted {record.submitted_at.isoformat()}): more than "
                    f"{ceiling} records are stored for this bucket, which means "
                    f"a job's outcome was never settled. Tracking state for "
                    f"that job is now discarded: its completion report will not "
                    f"be read and its objects will not be reported. An "
                    f"unconsumed-report alert precedes this by a long margin, "
                    f"so check whether one was missed."
                ),
            ))

    # ------------------------------------------------------------------
    # Read / write — per-bucket disable flag (self-healing bucket re-enable)
    # ------------------------------------------------------------------

    def get_disable_state(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> BucketDisableState:
        """Read whether ``source_bucket`` is disabled, and why.

        Reads the raw payload rather than going through :func:`deserialize`,
        deliberately: ``deserialize`` raises on an implausible
        ``last_processed_watermark``, and a disabled bucket must not read back
        as enabled because its watermark was corrupted or hand-edited. This
        mirrors :meth:`get_submission_records`, which reads the same object the
        same way for the same reason.

        An absent state object, an absent ``disabled`` key, and a falsy
        ``disabled`` value all yield an enabled
        :class:`~src.core.models.BucketDisableState`, so an operator can
        re-enable a bucket either by setting ``"disabled": false`` or by
        removing the key.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose flag to read.

        Returns:
            The bucket's :class:`~src.core.models.BucketDisableState`.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None or not payload.get(_DISABLED_FIELD, False):
            return BucketDisableState()
        return BucketDisableState(
            disabled=True,
            reason=str(payload.get(_DISABLED_REASON_FIELD, "")),
            at=str(payload.get(_DISABLED_AT_FIELD, "")),
        )

    def disable_bucket(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        reason: str,
        now: datetime,
        current_etag: str | None = None,
    ) -> str:
        """Record that ``source_bucket`` is disabled and clear its job history.

        One conditional write does both, so the bucket can never be left
        disabled with a stale ``SubmissionRecord`` still in place, or the
        reverse. The operator's only recovery step is setting
        ``"disabled": false`` in the state object — no Lambda invocation and
        no redeploy.

        Clearing ``submission_records`` is what makes that one step
        sufficient. The records that triggered the disable keep pointing at the
        same terminal ``Failed``/``Cancelled`` ``job_id``s forever: the only
        pruning path runs inside :meth:`record_submission`, and a disabled bucket
        never submits, so nothing would clear them. On the first run after
        re-enabling, the circuit breaker's ``DescribeJob`` check would find those
        same dead jobs Failed again, push ``bucket_consecutive_failures`` past the
        threshold a second time, and immediately re-disable the bucket — before a
        new job (for instance one benefiting from a fix deployed in between) ever
        got a chance to be submitted. With the records cleared,
        :meth:`get_submission_records` returns ``{}``, so the breaker seeds
        its counter at 0 and proceeds to a normal submission attempt.

        Every outstanding job's tracking is discarded along with it, so a job
        still running at the moment of the disable loses its completion report.
        That is accepted: the bucket is being stopped because its jobs keep
        failing, and the alternative — keeping records so the breaker can re-trip
        on them — defeats the manual re-enable this method exists to support.

        Safe with respect to the checkpoint watermark: a bucket is only ever
        disabled *before* any watermark advancement for the failing job's
        range is persisted, so the on-disk ``last_processed_watermark``
        already precedes the failed job's ``watermark_low``. The same journal
        range is re-read and resubmitted on the first run after re-enabling,
        exactly as the breaker's own in-memory rollback would have done had
        the bucket not been disabled.

        The submission-failure streak is deliberately **not** cleared. That
        counter tracks a request the Solution builds which botocore rejects
        outright, which is a code defect rather than a transient condition, so
        re-enabling without deploying a fix should re-trip promptly rather
        than start from zero.

        Takes ``current_etag`` from the caller rather than re-reading its own,
        because the circuit-breaker and submission-streak call sites both run
        inside the orchestrator's per-bucket ETag chain — the streak path from
        inside the lease scope. A self-managed ETag here would invalidate the
        ``StateWriter``'s held one and make the ``release_lease`` in
        ``_lease_scope``'s ``finally`` fail, stranding the lease in the state
        object.

        Preserves every other top-level key (checkpoint, lease,
        ``completion_items``, ``completion_processed_job_ids``,
        ``completion_scan_state``, ``completion_report_alerted_configs``).

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket to disable.
            reason:        Operator-facing explanation, persisted verbatim.
            now:           Timestamp recorded as ``disabled_at``.
            current_etag:  The ETag from the caller's most recent observation.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: On ETag mismatch (concurrent modification),
                in which case the bucket is **not** disabled and the caller
                must not announce it as such.
            ClientError: For other S3 errors.
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            payload = json.loads(serialize(_default_state(source_bucket)))

        payload[_DISABLED_FIELD] = True
        payload[_DISABLED_REASON_FIELD] = reason
        payload[_DISABLED_AT_FIELD] = now.isoformat()
        # Every job's report-missing suppression entry goes with its record.
        # Suppression is keyed by job_id, so an entry left behind here could never
        # be matched again and would accumulate for the life of the stack.
        _discard_alert_suppression(
            payload, list(_submission_records_by_job_id(payload, source_bucket))
        )
        payload["submission_records"] = {}
        # Remove the legacy singular field too, if present, so no dead job_id
        # survives under either schema form.
        payload.pop("submission_record", None)

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    # ------------------------------------------------------------------
    # Read — completion items / processed job ids
    # ------------------------------------------------------------------

    def completion_job_exists(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        job_id: str,
    ) -> bool:
        """Return whether ``job_id`` has already been merged into ``completion_items``.

        Read-only — issues a ``GetObject`` and deserializes the
        ``completion_processed_job_ids`` key via
        :func:`~src.core.completion_serializer.deserialize_processed_job_ids`.
        Tolerates a missing state object (first run) by returning ``False``.

        Under the per-object (``TrackedObject``) model, a ``TrackedObject``
        no longer has a 1:1 relationship with ``job_id`` — a single object
        can be created by one job and later extended by a sibling rule's
        job (Requirement 2.6), so "has this job already been processed" can
        no longer be answered by looking a ``TrackedObject`` up by ``job_id``
        directly. ``completion_processed_job_ids`` is the lightweight,
        job_id-keyed idempotency gate that replaces that lookup (design.md
        Decision 2): this is the gate the orchestrator uses so that
        :meth:`merge_completion_configs` is never called twice for the same
        ``job_id``.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to read.
            job_id:        The ``job_id`` to look up.

        Returns:
            ``True`` if ``completion_processed_job_ids`` already contains
            ``job_id``, ``False`` otherwise (including when no state object
            exists yet).

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 2.4
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return False

        processed_ids = completion_serializer.deserialize_processed_job_ids(payload)
        return job_id in processed_ids

    def get_all_completion_items(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> dict[str, TrackedObject]:
        """Read every ``TrackedObject`` for ``source_bucket``, regardless of resolution state.

        Read-only — issues a ``GetObject`` and deserializes the
        ``completion_items`` key via
        :func:`~src.core.completion_serializer.deserialize_completion_items`,
        with NO filtering applied. Tolerates a missing state object (first
        run) by returning ``{}``.

        The publish phase (``_run_completion_tracking_interval``) needs every
        item, including one that already reached ``RESOLVED`` in a prior
        interval and is now only waiting on quiescence.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose items to read.

        Returns:
            A ``dict`` mapping item_key -> ``TrackedObject``, unfiltered.
            Empty when no completion items are stored or the state object
            does not exist.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 4.1, 4.4, 5.4
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return {}

        return completion_serializer.deserialize_completion_items(payload)

    # ------------------------------------------------------------------
    # Write — completion items / processed job ids
    # ------------------------------------------------------------------

    def merge_completion_report(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        report: BopsCompletionReport,
        replication_config_id: str,
        job_id: str,
        job_created_at: datetime,
        current_etag: str | None,
        timestamps: dict[tuple[str, str], tuple[datetime | None, datetime | None]] | None = None,
    ) -> str:
        """Atomically resolve a ready completion report and record its job ID.

        The method builds and serializes all resolved objects before adding
        ``job_id`` to the processed set. It then writes both fields in one
        ETag-guarded state payload, so a mapping, serialization, or conditional
        write failure cannot record a processed job without its outcomes.
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            payload = json.loads(serialize(_default_state(source_bucket)))

        existing_items = completion_serializer.deserialize_completion_items(payload)
        stored_ts: dict[str, dict] = payload.get("completion_timestamps", {})
        stored_routing: dict[str, dict] = payload.get("completion_routing", {})
        seen_identities: set[tuple[str, str | None]] = set()

        for entry in report.entries:
            identity = (entry.object_key, entry.version_id)
            if identity in seen_identities:
                raise ValueError("completion report contains duplicate object identity")
            seen_identities.add(identity)

            object_key, version_id = identity
            merge_key = completion_serializer.item_key(object_key, version_id)
            existing_item = existing_items.get(merge_key)
            if existing_item is None:
                tagged_at = last_modified = None
                if timestamps and (timestamp := timestamps.get((object_key, version_id or ""))):
                    tagged_at, last_modified = timestamp
                if tagged_at is None and last_modified is None:
                    ts_entry = stored_ts.get(merge_key) or {}
                    if tagged_at_raw := ts_entry.get("tagged_at"):
                        tagged_at = datetime.fromisoformat(tagged_at_raw)
                    if last_modified_raw := ts_entry.get("last_modified"):
                        last_modified = datetime.fromisoformat(last_modified_raw)
                routing_entry = stored_routing.get(merge_key) or {}
                existing_item = TrackedObject(
                    source_bucket=source_bucket,
                    object_key=object_key,
                    version_id=version_id,
                    configs={},
                    tagged_at=tagged_at,
                    last_modified=last_modified,
                    matched_rules=frozenset(routing_entry.get("matched_rules") or ()),
                    destinations=frozenset(routing_entry.get("destinations") or ()),
                )

            stored_context = existing_item.configs.get(replication_config_id)
            if stored_context is not None and (job_created_at, job_id) < (
                stored_context.manifest_generated_at,
                stored_context.job_id,
            ):
                # A ready report can arrive after a newer job's report. Retain
                # that newer outcome and context, but record this job below so
                # it is not reprocessed on a later interval.
                continue

            configs = dict(existing_item.configs)
            configs[replication_config_id] = ConfigContext(
                replication_config_id=replication_config_id,
                job_id=job_id,
                manifest_generated_at=job_created_at,
                bops_confirmed=True,
            )
            existing_items[merge_key] = TrackedObject(
                source_bucket=existing_item.source_bucket,
                object_key=existing_item.object_key,
                version_id=existing_item.version_id,
                configs=configs,
                state=CompletionState.RESOLVED,
                resolved_at=report.created_at,
                resolution_method="bops_completion_report",
                replication_outcome=completion_tracker.outcome_from_report_row(entry),
                tagged_at=existing_item.tagged_at,
                last_modified=existing_item.last_modified,
                matched_rules=existing_item.matched_rules,
                destinations=existing_item.destinations,
            )

        serialized_items = completion_serializer.serialize_completion_items(existing_items)
        processed_ids = completion_serializer.deserialize_processed_job_ids(payload)
        processed_ids.add(job_id)
        serialized_processed_ids = completion_serializer.serialize_processed_job_ids(
            processed_ids
        )
        payload["completion_items"] = serialized_items
        payload["completion_processed_job_ids"] = serialized_processed_ids
        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    def store_completion_timestamps(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        timestamps: dict[tuple[str, str], tuple[datetime | None, datetime | None]],
        current_etag: str | None,
        routing: dict[tuple[str, str], tuple[list[str], list[str]]] | None = None,
    ) -> str:
        """Persist per-object report metadata for the completion report email.

        Stores (tagged_at, last_modified) under ``completion_timestamps`` and,
        when *routing* is supplied, (matched_rules, destinations) under
        ``completion_routing`` — both keyed by
        ``completion_serializer.item_key`` so that merge_completion_configs can
        enrich TrackedObjects on a later invocation when the BOPS report
        arrives.

        Both maps are written in one read-modify-write so a single ETag hop
        covers them; they are produced together at manifest-generation time.

        Merges into existing entries (does not overwrite previously stored
        entries for objects already in either map).
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        stored: dict[str, dict] = payload.get("completion_timestamps", {})
        for (object_key, version_id), (tagged_at, last_modified) in timestamps.items():
            merge_key = completion_serializer.item_key(object_key, version_id)
            if merge_key not in stored:
                entry: dict[str, str | None] = {}
                if tagged_at is not None:
                    entry["tagged_at"] = tagged_at.isoformat()
                if last_modified is not None:
                    entry["last_modified"] = last_modified.isoformat()
                if entry:
                    stored[merge_key] = entry

        payload["completion_timestamps"] = stored

        if routing:
            stored_routing: dict[str, dict] = payload.get("completion_routing", {})
            for (object_key, version_id), (rules, destinations) in routing.items():
                merge_key = completion_serializer.item_key(object_key, version_id)
                if merge_key not in stored_routing:
                    routing_entry: dict[str, list[str]] = {}
                    if rules:
                        routing_entry["matched_rules"] = sorted(rules)
                    if destinations:
                        routing_entry["destinations"] = sorted(destinations)
                    if routing_entry:
                        stored_routing[merge_key] = routing_entry
            payload["completion_routing"] = stored_routing

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    def delete_completion_items(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        item_keys: list[str],
        current_etag: str,
    ) -> str:
        """Remove a batch of ``TrackedObject`` entries from the state object.

        Per design Decision 8, every item covered by a successfully
        published ``Completion_Report`` is deleted from ``completion_items``
        immediately after publish — this both bounds state size and
        structurally guarantees a report is never published twice for the
        same Tracked_Object (there is nothing left to re-publish once
        deleted). This removes a BATCH of item entries by item_key in one
        call, since a single publish batch can span multiple items.

        Reads the current raw JSON, removes ``completion_items[item_key]``
        for every ``item_key`` in ``item_keys`` while preserving every other
        existing entry in ``completion_items`` and every other top-level key
        (checkpoint, lease, ``submission_records``, ``completion_scan_state``),
        and writes the updated payload back using a conditional write
        guarded by ``current_etag``. An ``item_key`` that is not present is a
        no-op (the payload is unchanged aside from re-serialization) rather
        than an error.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to update.
            item_keys:     The item_keys whose entries to remove.
            current_etag:  The ETag returned by the caller's most recent
                           conditional write.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 1.1, 1.4, 1.6
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        existing_items = completion_serializer.deserialize_completion_items(payload)
        stored_ts: dict = payload.get("completion_timestamps", {})
        stored_routing: dict = payload.get("completion_routing", {})
        for item_key in item_keys:
            existing_items.pop(item_key, None)
            stored_ts.pop(item_key, None)
            # Pruned alongside the timestamps: both are per-item side maps that
            # would otherwise grow without bound, since nothing else removes
            # them once the item they describe has been published and deleted.
            stored_routing.pop(item_key, None)
        payload["completion_items"] = completion_serializer.serialize_completion_items(
            existing_items
        )
        payload["completion_timestamps"] = stored_ts
        payload["completion_routing"] = stored_routing

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    # ------------------------------------------------------------------
    # Read/Write — report-missing alert suppression (design.md Decision 9)
    # ------------------------------------------------------------------

    def get_alerted_configs(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> set[str]:
        """Read the set of ``replication_config_id``s currently under
        report-missing alert suppression.

        Read-only — issues a ``GetObject`` and reads the
        ``completion_report_alerted_configs`` top-level key (a flat JSON
        list of ``replication_config_id`` strings), tolerating both a
        missing key (returns ``set()``) and a missing state object (first
        run — returns ``set()``).

        This is the persisted marker described in design.md Decision 9:
        ``check_report_handler`` consults this set to decide whether a
        terminal, unconfirmed job's missing report has already been
        alerted (Requirement 8.5); the creation hook
        (``_process_bucket``, task 17.1) clears an entry from this set once
        that config's report is observed and merged (Requirement 8.6).

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to read.

        Returns:
            A ``set[str]`` of ``replication_config_id`` values currently
            suppressed. Empty when no alerted configs are stored or the
            state object does not exist.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 8.5, 8.6
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return set()

        raw_list = payload.get("completion_report_alerted_configs")
        if raw_list is None:
            return set()
        return set(raw_list)

    def add_alerted_config(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        replication_config_id: str,
        current_etag: str | None = None,
    ) -> str:
        """Add ``replication_config_id`` to the report-missing alert
        suppression set.

        Mirrors :meth:`record_scan_result`'s exact read-raw-JSON,
        mutate-one-key, single-attempt-conditional-write pattern: this
        method runs inside ``check_report_handler``'s own single-threaded
        per-config loop within one Lambda invocation (a separate
        invocation entirely from ``run_interval``'s own ETag chain — design
        Decision 9's "runs independently... in its own Lambda invocation"),
        so a bounded retry is neither necessary nor mirrors the isolation
        boundary of that invocation.

        Reads the current raw JSON payload (defaulting to a fresh
        ``CheckpointState`` when the object does not exist yet), adds
        ``replication_config_id`` to the
        ``completion_report_alerted_configs`` set while preserving every
        other existing entry in that set and every other top-level key
        (checkpoint, lease, ``submission_records``, ``completion_items``,
        ``completion_processed_job_ids``, ``completion_scan_state``), and
        writes the updated payload back using a conditional write guarded
        by ``current_etag``. Adding an already-present
        ``replication_config_id`` is a no-op write (idempotent — Requirement
        8.5's "alert at most once ... while its report-missing condition
        persists").

        When ``current_etag`` is ``None`` (the caller did not supply one),
        the method captures the ETag from its own ``get_object`` response
        and uses that as the precondition, unless the object does not exist
        yet (``NoSuchKey``), in which case it remains a create-only write.
        This follows the same ETag-capture pattern used by the state-store
        mutators and ensures a call from ``check_report_handler`` — which calls
        ``get_alerted_configs`` first (proving the object exists) but does
        not capture that ETag — does not fail spuriously with
        ``ConditionalWriteError`` due to using ``IfNoneMatch: "*"`` on an
        already-existing object.

        Args:
            s3_client:              A boto3 S3 client.
            state_bucket:           The scratch/state bucket name.
            source_bucket:          The Monitored_Bucket whose state to update.
            replication_config_id:  The config to mark as alerted.
            current_etag:           The ETag returned by the caller's most
                                     recent conditional write, or ``None``
                                     for a create-only write. When ``None``
                                     and the object exists, the method
                                     captures the ETag from its own read.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 8.5
        """
        key = state_object_key(source_bucket)

        payload, read_etag = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))
            # current_etag remains None, which _put_object_conditional
            # interprets as create-only (IfNoneMatch: "*")
        else:
            # When current_etag is None and we successfully read the object,
            # use the ETag we just obtained as the precondition.
            if current_etag is None:
                current_etag = read_etag

        existing = set(payload.get("completion_report_alerted_configs") or [])
        existing.add(replication_config_id)
        payload["completion_report_alerted_configs"] = list(existing)

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    def clear_alerted_config(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        replication_config_id: str,
        current_etag: str | None = None,
    ) -> str:
        """Remove ``replication_config_id`` from the report-missing alert
        suppression set.

        Mirrors :meth:`add_alerted_config`'s exact read-raw-JSON,
        mutate-one-key, single-attempt-conditional-write pattern. This is
        the integration point design.md Decision 9 describes: the creation
        hook (``_process_bucket``, task 17.1) calls this immediately after
        a successful ``merge_completion_configs`` for a
        ``replication_config_id``, so a report observed via the main
        interval's own DescribeJob loop resets suppression for that config
        (Requirement 8.6). Removing an absent ``replication_config_id`` is
        a no-op write (the payload is unchanged aside from
        re-serialization) rather than an error.

        When ``current_etag`` is ``None`` (the caller did not supply one),
        the method captures the ETag from its own ``get_object`` response
        and uses that as the precondition, unless the object does not exist
        yet (``NoSuchKey``), in which case it remains a create-only write.
        This matches the pattern used by ``add_alerted_config`` and ensures
        consistency across the alert-suppression API.

        Args:
            s3_client:              A boto3 S3 client.
            state_bucket:           The scratch/state bucket name.
            source_bucket:          The Monitored_Bucket whose state to update.
            replication_config_id:  The config to clear the alert marker for.
            current_etag:           The ETag returned by the caller's most
                                     recent conditional write, or ``None``
                                     for a create-only write. When ``None``
                                     and the object exists, the method
                                     captures the ETag from its own read.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 8.6
        """
        key = state_object_key(source_bucket)

        payload, read_etag = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))
            # current_etag remains None, which _put_object_conditional
            # interprets as create-only (IfNoneMatch: "*")
        else:
            # When current_etag is None and we successfully read the object,
            # use the ETag we just obtained as the precondition.
            if current_etag is None:
                current_etag = read_etag

        existing = set(payload.get("completion_report_alerted_configs") or [])
        existing.discard(replication_config_id)
        payload["completion_report_alerted_configs"] = list(existing)

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    # ------------------------------------------------------------------
    # Read/Write — quiescence scan state
    # ------------------------------------------------------------------

    def record_scan_result(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        replication_config_id: str,
        scan_at: datetime,
        match_count: int,
        current_etag: str | None = None,
    ) -> str:
        """Persist the latest per-bucket tag-matching scan result.

        Per design.md (single-batch-job-per-bucket) Decision D5, the
        orchestrator calls this once per bucket per interval — over the
        union of the bucket's rules — rather than once per
        ``replication_config_id``. ``replication_config_id`` is passed the
        per-bucket sentinel value (the bucket's own name, i.e. equal to
        ``source_bucket``), the same sentinel used to key the object's
        single ``ConfigContext`` (design.md D4) — so the entry this method
        writes under ``completion_scan_state`` is looked up later, by
        ``should_publish``, using that identical bucket-name key. The
        parameter is still named ``replication_config_id`` for signature
        continuity with :func:`~src.core.completion_tracker.quiescence_check`
        and the persisted JSON schema (which is generic over whatever
        string keys ``completion_scan_state`` holds), but under the current
        design it is always the per-bucket sentinel, not a rule/config
        identifier.

        Mirrors the other per-bucket read-raw-JSON, mutate-one-key,
        single-attempt conditional writes: this method
        runs inside the orchestrator's per-bucket path (design.md D1/D5), on
        the same single-threaded ETag chain as the rest of
        ``_process_bucket``, so a bounded retry is neither necessary nor
        safe (a retry here would re-read state a caller elsewhere in the
        same chain has not yet observed).

        Reads the current raw JSON payload (defaulting to a fresh
        ``CheckpointState`` when the object does not exist yet), adds or
        replaces the entry for ``replication_config_id`` (the per-bucket
        sentinel) under ``completion_scan_state`` with a
        ``ScanState(last_scan_at=scan_at, last_scan_match_count=match_count)``
        while preserving every other existing entry in
        ``completion_scan_state`` and every other top-level key (checkpoint,
        lease, ``submission_records``, ``completion_items``), and writes the
        updated payload back using a conditional write guarded by
        ``current_etag``.

        Args:
            s3_client:              A boto3 S3 client.
            state_bucket:           The scratch/state bucket name.
            source_bucket:          The Monitored_Bucket whose state to update.
            replication_config_id:  The per-bucket sentinel to key this scan
                                     result under (the bucket's own name,
                                     design.md D5).
            scan_at:                The timestamp of this scan.
            match_count:            The number of new Matched_Object entries
                                     this scan found (the union preflight
                                     count over all of the bucket's rules).
            current_etag:           The ETag returned by the caller's most
                                     recent conditional write, or ``None``
                                     for a create-only write.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 5.1, 5.2, 5.3, 3.3
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        existing_scan_state = completion_serializer.deserialize_scan_state(payload)
        existing_scan_state[replication_config_id] = ScanState(
            last_scan_at=scan_at, last_scan_match_count=match_count
        )
        payload["completion_scan_state"] = completion_serializer.serialize_scan_state(
            existing_scan_state
        )

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    def get_scan_state(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> dict[str, ScanState]:
        """Read the per-bucket ``ScanState`` values from the state object.

        Read-only — issues a ``GetObject`` and deserializes the
        ``completion_scan_state`` key via
        :func:`~src.core.completion_serializer.deserialize_scan_state`.
        Tolerates a missing state object (first run) by returning ``{}``.

        Per design.md D5, the returned dict holds exactly one entry under
        current-design code, keyed by the per-bucket sentinel (the bucket's
        own name) written by :meth:`record_scan_result`. This is the same
        key ``should_publish`` looks up via ``obj.configs.keys()`` (design.md
        D4), so the caller's ``scan_state_by_config.get(config_id)`` lookup
        resolves against this dict correctly without any translation.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to read.

        Returns:
            A ``dict`` mapping the per-bucket sentinel -> ``ScanState``.
            Empty when no scan state is stored or the state object does not
            exist.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 5.1, 5.2, 5.3, 3.3
        """
        key = state_object_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return {}

        return completion_serializer.deserialize_scan_state(payload)

    def increment_submission_failure_streak(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        bucket_name: str,
        current_etag: str | None = None,
    ) -> tuple[int, str]:
        """Increment the creation-failure streak for a bucket by one.

        Follows the same read-raw-JSON, mutate-one-key, single-attempt
        conditional-write pattern as :meth:`record_scan_result`.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state object to update.
            bucket_name:   The bucket whose streak to increment.
            current_etag:  The ETag from the caller's most recent observation.

        Returns:
            A tuple of (new_streak_value, new_etag).

        Raises:
            ConditionalWriteError: On ETag mismatch (concurrent modification).
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        streaks = payload.setdefault(_SUBMISSION_FAILURE_STREAK_FIELD, {})
        new_value = int(streaks.get(bucket_name, 0)) + 1
        streaks[bucket_name] = new_value

        new_etag = _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )
        return new_value, new_etag

    def clear_submission_failure_streak(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        bucket_name: str,
        current_etag: str | None = None,
    ) -> str:
        """Clear (delete) the creation-failure streak for a bucket.

        Called after a successful submission so that any recurrence after
        a fix is detected and alerted again.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state object to update.
            bucket_name:   The bucket whose streak to clear.
            current_etag:  The ETag from the caller's most recent observation.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: On ETag mismatch (concurrent modification).
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        streaks = payload.get(_SUBMISSION_FAILURE_STREAK_FIELD, {})
        if bucket_name in streaks:
            del streaks[bucket_name]
            payload[_SUBMISSION_FAILURE_STREAK_FIELD] = streaks

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    def claim_journal_unavailable_alert(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        bucket_name: str,
        now: datetime,
        min_interval: timedelta,
        current_etag: str | None = None,
    ) -> tuple[bool, str | None]:
        """Claim the right to send one journal-unavailable alert for a bucket.

        Returns ``(True, new_etag)`` when no alert has been recorded for
        *bucket_name* within *min_interval* of *now*, having recorded *now* as
        the latest alert time. Returns ``(False, current_etag)`` otherwise,
        without writing.

        A recorded timestamp that expires on its own is what makes this
        self-clearing. The alternative, a counter cleared on the next
        successful read, would put a write on the healthy path purely to delete
        a key that is almost never present, and would thread that write into
        the per-bucket conditional-write ETag chain for no benefit. It would
        also go permanently quiet after a single notification, which is the
        wrong behavior for a condition nobody has acted on yet.

        A timestamp that cannot be parsed is treated as absent, so a
        hand-edited or truncated value produces an extra alert rather than
        suppressing alerts indefinitely.

        Returns:
            A tuple of (alert_should_be_sent, etag). The ETag is *current_etag*
            unchanged when nothing was written.
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        alerts = payload.setdefault(_JOURNAL_UNAVAILABLE_ALERT_FIELD, {})
        last_raw = alerts.get(bucket_name)
        if last_raw:
            try:
                last = datetime.fromisoformat(str(last_raw))
            except (TypeError, ValueError):
                last = None
            if last is not None:
                if last.tzinfo is None:
                    last = last.replace(tzinfo=now.tzinfo)
                if now - last < min_interval:
                    return False, current_etag

        alerts[bucket_name] = now.isoformat()
        new_etag = _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )
        return True, new_etag

    def mark_report_diagnosed(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        job_id: str,
        current_etag: str | None = None,
        *,
        report_diagnosed: bool = True,
        recovery_scored: bool = False,
    ) -> str:
        """Set the named per-job flags on the submission record for *job_id*.

        Only ever sets a flag to ``True``; passing ``False`` leaves it alone. Both
        flags record something that has happened once and cannot un-happen, so
        there is no path that should clear one, and a single method setting either
        or both keeps the terminal-job path to one conditional write.

        * ``report_diagnosed`` — this job's completion report has been read and its
          task errors logged.
        * ``recovery_scored`` — this job's terminal outcome has been folded into
          the bucket's recovery arithmetic. See
          :class:`~src.core.models.SubmissionRecord` for why a record outliving
          its run makes this necessary.

        Follows the same read-raw-JSON, mutate-one-key, single-attempt
        conditional-write pattern as :meth:`increment_submission_failure_streak`.

        Matches on the record's own ``job_id`` field rather than on the dict key,
        so the flag lands on the right record whether the payload is keyed by
        ``job_id`` (current) or by the bucket's name (1.0.1 and the interim
        1.1.0 build, before the first :meth:`record_submission` re-keys it).
        Keying the lookup would silently stop writing the flags on a state object
        that has not been rewritten yet.

        The caller (``StateWriter``) wraps this in a best-effort handler. A failed
        write costs one duplicate diagnostic log, and one duplicate rollback and
        resubmission, on the next run. Neither is a correctness problem, and both
        stop as soon as a write lands.

        Args:
            s3_client:        A boto3 S3 client.
            state_bucket:     The scratch/state bucket name.
            source_bucket:    The Monitored_Bucket whose state object to update.
            job_id:           The ``job_id`` of the record to mark.
            current_etag:     The ETag from the caller's most recent observation.
            report_diagnosed: Set the flag when ``True``.
            recovery_scored:  Set the flag when ``True``.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: On ETag mismatch (concurrent modification).
        """
        key = state_object_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        records = payload.get("submission_records", {})
        if isinstance(records, dict):
            for stored in records.values():
                if not isinstance(stored, dict) or stored.get("job_id") != job_id:
                    continue
                if report_diagnosed:
                    stored["report_diagnosed"] = True
                if recovery_scored:
                    stored["recovery_scored"] = True
            payload["submission_records"] = records

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )
