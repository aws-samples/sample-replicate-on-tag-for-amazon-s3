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

Per design.md Decision D3 (one Batch Operations job per bucket), a bucket has
a single ``SubmissionRecord``, and :meth:`StateStore.record_submission` writes
``submission_records`` as exactly one entry, keyed by the per-bucket sentinel
— the ``source_bucket`` name itself. :meth:`StateStore.get_submission_records`
(via :func:`~src.core.checkpoint_serializer.deserialize_submission_records`)
reads whatever keys are present in that dict without assuming what they mean.
The orchestrator's per-bucket circuit breaker (``_process_bucket``) reads the
prior record before calling :meth:`record_submission`; that call then
overwrites ``submission_records`` with the single bucket-keyed entry.

::

    {
      "source_bucket": "<str>",
      "last_processed_watermark": "<canonical record_timestamp str>",
      "lease": { ... } | null,
      "processed_window": [ ... ],
      "submission_records": {
        "<source_bucket sentinel>": {
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

``completion_items`` is keyed by an item key (not a ``job_id``), since one
Tracked_Object's item can span multiple jobs across multiple replication
rules — see ``src.core.completion_serializer`` and design.md Decision 2.
``completion_processed_job_ids`` is the flat idempotency-gate set: it
records every ``job_id`` that has already had its manifest entries merged
into ``completion_items`` (design.md Decision 6).

``submission_records`` holds exactly one entry, keyed by the bucket's own
name. :meth:`get_submission_records` reads whatever keys are present in that
dict without interpreting them; a payload carrying only the legacy singular
``submission_record`` field, or entries keyed by ``replication_config_id``
from a deployment older than 0.1.19, is not migrated — the sentinel-keyed
entry is simply absent, so :meth:`get_submission_records` returns an empty
dict for that bucket. :meth:`record_submission` always writes the single
bucket-keyed entry, overwriting whatever was there before.

Requirements: 4.1, 4.2, 4.3, 7.4, 9.1, 9.3, 9.4
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from collections.abc import Callable

from botocore.exceptions import ClientError

from src.core.checkpoint_logic import advance_checkpoint
from src.core.checkpoint_serializer import (
    deserialize,
    deserialize_submission_records,
    serialize,
    serialize_submission_record,
)
from src.core import completion_serializer, completion_tracker, observability
from src.core.models import (
    CheckpointState,
    CompletionState,
    Lease,
    ManifestEntry,
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
# Private helpers
# ---------------------------------------------------------------------------


def _state_key(source_bucket: str) -> str:
    """Return the S3 object key for a bucket's state object."""
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
        key = _state_key(source_bucket)
        payload, etag = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return _default_state(source_bucket), None
        state = deserialize(json.dumps(payload))
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
            ``submission_records``, ``completion_items``,
            ``completion_processed_job_ids``, ``completion_scan_state``, and
            ``completion_report_alerted_configs`` — mirroring the
            read-raw-JSON, mutate-one-key pattern used by every other mutator
            in this module (``record_submission``, ``merge_completion_configs``,
            etc.). Without this merge, ``acquire_lease``/``release_lease``
            (which both persist via this method) would silently wipe every
            completion-tracking and submission key on every call.
        """
        key = _state_key(state.source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            payload = {}

        # Overlay only the CheckpointState-owned keys; every other key already
        # present in the raw payload (submission_records, completion_items,
        # completion_processed_job_ids, completion_scan_state,
        # completion_report_alerted_configs) is left untouched.
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
        """Read all ``SubmissionRecord`` objects from the state object.

        Returns a dict of whatever keys are present in ``submission_records``.
        As of design.md D3, a state object written by current code holds one
        entry keyed by the per-bucket sentinel (the bucket's own name), since
        a bucket has a single ``SubmissionRecord``. Returns an empty dict
        when no submission records are stored, when the payload carries only
        the pre-0.1.19 singular ``submission_record`` field, or when the
        state object does not yet exist.

        This method should be called right after :meth:`get_checkpoint` so that
        the records from the previous run are available for the failed-job
        recovery check before any conditional write overwrites them.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose records to read.

        Returns:
            A ``dict`` mapping the per-bucket sentinel key → ``SubmissionRecord``.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 4.1, 4.2
        """
        key = _state_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return {}

        return deserialize_submission_records(payload)

    # ------------------------------------------------------------------
    # Write — submission record
    # ------------------------------------------------------------------

    def record_submission(
        self,
        s3_client: Any,
        state_bucket: str,
        submission: SubmissionRecord,
        current_etag: str,
    ) -> str:
        """Persist a ``SubmissionRecord`` into the per-bucket state object.

        The submission record is stored alongside the ``CheckpointState`` in
        the same JSON object under the ETag guard, so that the record and the
        checkpoint remain in a consistent state (Requirement 7.4).

        The serialized ``CheckpointState`` fields (source_bucket,
        last_processed_watermark, lease, processed_window) are preserved
        verbatim.  The ``submission_records`` dict is **collapsed** to
        exactly one entry, keyed by the per-bucket sentinel
        (``submission.source_bucket`` — design.md Decision D3, one
        ``SubmissionRecord`` per bucket): ``{source_bucket: <record>}``.

        This write replaces whatever was previously stored under
        ``submission_records``. The orchestrator's per-bucket
        circuit-breaker recovery (``_process_bucket``) already read the
        prior record via :meth:`get_submission_records` before this call,
        rolled the watermark back, and folded the prior record's
        ``consecutive_failures`` into the single bucket-level counter passed
        in on ``submission``, so no in-flight recovery information is lost
        by the overwrite.

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

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 4.1, 4.2, 7.4, 2.2, 2.4
        """
        source_bucket = submission.source_bucket
        key = _state_key(source_bucket)

        # Read the current raw JSON so that we preserve all existing fields
        # (checkpoint + lease + other submission records) while
        # adding/replacing the entry for this bucket.
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            # No state object yet — start from the default checkpoint.
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        # Collapse submission_records to exactly one entry, keyed by the
        # stable per-bucket sentinel (the bucket name itself) rather than by
        # replication_config_id (design.md D3). Any legacy config_id-keyed
        # entries (or a stale bucket-keyed entry from a prior run) are
        # dropped here — this write IS the migration-on-write point (task
        # 4.2): the caller (the orchestrator's per-bucket circuit-breaker
        # recovery) has already read every prior record via
        # get_submission_records, rolled the watermark back, and folded
        # every prior record's consecutive_failures into the single
        # bucket-level counter carried on `submission`, so nothing is lost
        # by dropping the old entries.
        payload["submission_records"] = {
            submission.source_bucket: serialize_submission_record(submission)
        }

        # Remove the legacy singular field if it was present so the state
        # object doesn't carry both forms.
        payload.pop("submission_record", None)

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    # ------------------------------------------------------------------
    # Write — clear submission records (self-healing bucket re-enable)
    # ------------------------------------------------------------------

    def clear_submission_records(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> str:
        """Remove every persisted ``SubmissionRecord`` for ``source_bucket``.

        Called when the orchestrator's circuit breaker disables a bucket
        (``on_bucket_disable``), so that a customer's only recovery step is
        flipping ``disabled: false`` back in ``solution-config.json`` — no
        Lambda invocation or manual state-object edit is required.

        Without this call, the ``SubmissionRecord`` that triggered the
        disable keeps pointing at the same terminal ``Failed``/``Cancelled``
        ``job_id`` forever (nothing else clears it). On the next run after
        re-enabling, the circuit breaker's ``DescribeJob`` check would find
        that same dead job Failed again, push
        ``bucket_consecutive_failures`` past the threshold a second time,
        and immediately re-disable the bucket — before a new job (e.g. one
        that benefits from a fix deployed in between) ever gets a chance to
        be submitted. Clearing ``submission_records`` here gives the next
        run a clean job history: :meth:`get_submission_records` returns
        ``{}``, so the circuit breaker seeds its counter at 0 and proceeds
        to a normal submission attempt instead of re-tripping.

        This is safe with respect to the checkpoint watermark: a bucket is
        only ever disabled *before* any watermark advancement for the
        failing job's range is persisted (the circuit breaker check runs
        before lease acquisition/journal read/``release_lease``), so the
        on-disk ``last_processed_watermark`` already precedes the failed
        job's ``watermark_low`` — the same journal range will be re-read and
        resubmitted on the next run after re-enabling, exactly as the
        circuit breaker's own (in-memory) watermark rollback would have done
        had the bucket not been disabled.

        Self-contained read-fresh-ETag-then-conditional-write, single
        attempt: this method is called from the Lambda handler's disable
        path, not from inside the orchestrator's own per-bucket ETag chain,
        so it manages its own ETag rather than requiring the caller to
        supply one. A best-effort caller (see ``lambda_handler``) should
        catch and log :class:`ConditionalWriteError` rather than letting it
        block the (more important) disable-flag write.

        Preserves every other top-level key (checkpoint, lease,
        ``completion_items``, ``completion_processed_job_ids``,
        ``completion_scan_state``, ``completion_report_alerted_configs``).

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose submission records to
                            clear.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between this method's own read and its write.
            ClientError: For other S3 errors.
        """
        key = _state_key(source_bucket)

        payload, current_etag = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))
            current_etag = None

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
        key = _state_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return False

        processed_ids = completion_serializer.deserialize_processed_job_ids(payload)
        return job_id in processed_ids

    def get_check_eligible_items(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
    ) -> dict[str, TrackedObject]:
        """Read every ``TrackedObject`` eligible for a Source_Status_Check.

        Read-only — issues a ``GetObject`` and deserializes the
        ``completion_items`` key via
        :func:`~src.core.completion_serializer.deserialize_completion_items`,
        then filters to items with ``state == CompletionState.PENDING`` AND
        every ``ConfigContext`` in ``configs`` marked ``bops_confirmed``
        (the exact input ``completion_tracker.select_check_candidates``
        needs — design.md Decision 3). A ``TrackedObject`` with any
        unconfirmed ``ConfigContext`` is excluded, since the aggregate
        ``x-amz-replication-status`` header must not be read until every
        routing job has confirmed the object (Requirement 3.1). Tolerates a
        missing state object (first run) by returning ``{}``.

        The per-object model filters on the object-level ``state`` plus the
        BOPS-confirmation gate, rather than on any per-destination state.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose items to read.

        Returns:
            A ``dict`` mapping item_key -> ``TrackedObject``, restricted to
            ``PENDING`` items with every config ``bops_confirmed``. Empty
            when no completion items are stored or the state object does
            not exist.

        Raises:
            ClientError: For S3 errors other than ``NoSuchKey``.

        Requirements: 2.1, 2.6, 3.1
        """
        key = _state_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return {}

        items = completion_serializer.deserialize_completion_items(payload)
        return {
            item_key: obj
            for item_key, obj in items.items()
            if obj.state == CompletionState.PENDING
            and all(ctx.bops_confirmed for ctx in obj.configs.values())
        }

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

        This is the unfiltered counterpart to :meth:`get_check_eligible_items`,
        which excludes any item not currently ``PENDING`` and fully
        BOPS-confirmed. The publish phase (``_run_completion_tracking_interval``)
        needs exactly this unfiltered set: ``completion_tracker.should_publish``
        must be evaluated against every item, including one that already
        reached ``RESOLVED`` in a prior interval and is now only waiting on
        quiescence — a case :meth:`get_check_eligible_items` would silently
        exclude, since a ``RESOLVED`` item is never check-eligible.

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
        key = _state_key(source_bucket)
        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            return {}

        return completion_serializer.deserialize_completion_items(payload)

    # ------------------------------------------------------------------
    # Write — completion items / processed job ids
    # ------------------------------------------------------------------

    def merge_completion_configs(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        entries: list[ManifestEntry],
        replication_config_id: str,
        job_id: str,
        manifest_generated_at: datetime,
        current_etag: str | None,
        timestamps: dict[tuple[str, str], tuple[datetime | None, datetime | None]] | None = None,
    ) -> str:
        """Merge a batch of new ``ConfigContext``s into ``completion_items``.

        Mirrors :meth:`record_submission`'s exact read-raw-JSON,
        mutate-one-key, single-attempt-conditional-write pattern (not
        :meth:`apply_completion_resolutions`'s retrying pattern) — this
        method runs inside the orchestrator's single-threaded per-bucket
        ETag chain, on the same chain as the rest of ``_process_bucket``, so
        a bounded retry is neither necessary nor safe here, for the same
        reason :meth:`record_scan_result` already gives.

        For each entry in ``entries`` (the parsed rows of a terminal job's
        BOPS_Completion_Report), calls
        :func:`~src.core.completion_tracker.create_pending_tracked_object_updates`
        to build the new, ``bops_confirmed=True`` ``ConfigContext`` for that
        entry's ``(object_key, version_id)`` under ``replication_config_id``,
        then merges it into the ``TrackedObject`` for that identity —
        creating the ``TrackedObject`` in ``PENDING`` state if it doesn't
        exist yet, or adding this ``ConfigContext`` alongside any
        pre-existing configs from a different rule's job if the object
        already exists (Requirement 2.6). After merging every entry, adds
        ``job_id`` to ``completion_processed_job_ids`` in the same payload.
        Preserves every other existing entry in ``completion_items`` and
        every other top-level key (checkpoint, lease, ``submission_records``,
        ``completion_scan_state``), and writes the updated payload back
        using a conditional write guarded by ``current_etag``.

        This method merges ``ConfigContext``s only — resolution state lives
        at the object level, set later by :meth:`apply_completion_resolutions`.

        Callers are expected to have already checked
        :meth:`completion_job_exists` — this method does not itself guard
        against re-merging an already-processed ``job_id`` (design Decision
        2: the idempotency check is the orchestrator's responsibility, kept
        as a separate read so it can be evaluated before the (potentially
        expensive) report re-read). If a config already exists for the
        exact same ``replication_config_id`` on an object (e.g. a re-run bug
        bypassing the gate), this method overwrites that single config
        entry rather than raising — the gate at the call site is what
        should normally prevent that, not this method's per-object merge
        logic.

        Args:
            s3_client:              A boto3 S3 client.
            state_bucket:           The scratch/state bucket name.
            source_bucket:          The Monitored_Bucket whose state to update.
            entries:                The job's BOPS_Completion_Report entries
                                     (``ManifestEntry``) to merge as new configs.
            replication_config_id:  The config whose job produced ``entries``.
            job_id:                 The ``Batch_Replication_Job`` id that
                                     produced ``entries``.
            manifest_generated_at:  The job's ``Job.CreationTime``.
            current_etag:           The ETag returned by the caller's most
                                     recent conditional write, or ``None``
                                     for a create-only write.

        Returns:
            The new ETag returned by S3.

        Raises:
            ConditionalWriteError: When the state object was modified
                between the caller's last ETag observation and this call.
            ClientError: For other S3 errors.

        Requirements: 2.1, 2.4, 2.6
        """
        key = _state_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        existing_items = completion_serializer.deserialize_completion_items(payload)

        # Look up pre-stored timestamps from the state payload (written at
        # manifest-generation time by store_completion_timestamps).
        stored_ts: dict[str, dict] = payload.get("completion_timestamps", {})

        updates = completion_tracker.create_pending_tracked_object_updates(
            entries=entries,
            replication_config_id=replication_config_id,
            job_id=job_id,
            manifest_generated_at=manifest_generated_at,
        )
        for (object_key, version_id), new_config in updates.items():
            merge_key = completion_serializer.item_key(object_key, version_id)
            existing_item = existing_items.get(merge_key)
            if existing_item is None:
                tagged_at = None
                last_modified = None
                # Prefer caller-supplied timestamps (available when called
                # from the same invocation that generated the manifest).
                if timestamps:
                    ts = timestamps.get((object_key, version_id or ""))
                    if ts:
                        tagged_at, last_modified = ts
                # Fall back to pre-stored timestamps in the state object
                # (written by store_completion_timestamps on a prior invocation).
                if tagged_at is None and last_modified is None:
                    ts_entry = stored_ts.get(merge_key)
                    if ts_entry:
                        tagged_at_raw = ts_entry.get("tagged_at")
                        last_modified_raw = ts_entry.get("last_modified")
                        if tagged_at_raw:
                            tagged_at = datetime.fromisoformat(tagged_at_raw)
                        if last_modified_raw:
                            last_modified = datetime.fromisoformat(last_modified_raw)
                existing_item = TrackedObject(
                    source_bucket=source_bucket,
                    object_key=object_key,
                    version_id=version_id,
                    configs={},
                    tagged_at=tagged_at,
                    last_modified=last_modified,
                )
            existing_item.configs[replication_config_id] = new_config
            existing_items[merge_key] = existing_item

        payload["completion_items"] = completion_serializer.serialize_completion_items(
            existing_items
        )

        existing_processed_ids = completion_serializer.deserialize_processed_job_ids(
            payload
        )
        existing_processed_ids.add(job_id)
        payload["completion_processed_job_ids"] = (
            completion_serializer.serialize_processed_job_ids(existing_processed_ids)
        )

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
    ) -> str:
        """Persist per-object timestamps for the completion report email.

        Stores (tagged_at, last_modified) keyed by completion_serializer.item_key
        so that merge_completion_configs can enrich TrackedObjects on a later
        invocation when the BOPS report arrives.

        Merges into existing timestamps (does not overwrite previously stored
        entries for objects already in the map).
        """
        key = _state_key(source_bucket)
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
        key = _state_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        existing_items = completion_serializer.deserialize_completion_items(payload)
        stored_ts: dict = payload.get("completion_timestamps", {})
        for item_key in item_keys:
            existing_items.pop(item_key, None)
            stored_ts.pop(item_key, None)
        payload["completion_items"] = completion_serializer.serialize_completion_items(
            existing_items
        )
        payload["completion_timestamps"] = stored_ts

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )

    def apply_completion_resolutions(
        self,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        mutate_fn: Callable[[dict[str, Any]], dict[str, Any]],
        max_attempts: int = 5,
    ) -> str:
        """Read-modify-conditional-write ``completion_items`` with bounded retry.

        Unlike :meth:`merge_completion_configs` / :meth:`delete_completion_items`
        (single-attempt conditional writes, safe because they only ever run
        inside the orchestrator's single-threaded per-bucket ETag chain), this
        method is used by the isolated ``_run_completion_tracking_interval``
        step (design Decision 5), which can race against a concurrent
        ``run_interval``'s own writes to the same
        ``state/<source_bucket>.json`` object. A single-attempt write would
        therefore fail spuriously on an ETag conflict that has nothing to do
        with a genuine double-resolution; this method instead re-reads and
        retries the mutation up to ``max_attempts`` times before giving up.

        On each attempt:

        1. Read the current raw JSON payload (preserving every existing key
           — checkpoint fields, lease, ``submission_records``,
           ``completion_scan_state`` — exactly like :meth:`merge_completion_configs`
           / :meth:`delete_completion_items` already do; tolerate a missing
           state object by starting from :func:`_default_state`).
        2. Call ``mutate_fn(payload)``, which reads and writes only
           ``payload["completion_items"]`` (the caller — the orchestrator's
           poll-and-reconcile phase — builds ``mutate_fn`` from
           ``completion_tracker.reconcile_source_status_check``, operating on
           the object-level (``TrackedObject``) shape). The dict
           ``mutate_fn`` returns becomes the new payload for this attempt.
        3. Attempt a conditional ``PutObject`` via :func:`_put_object_conditional`
           guarded by the ETag observed in step 1.
        4. On :class:`ConditionalWriteError`, the ETag is now stale — go back
           to step 1 (re-read the current state) and retry ``mutate_fn``
           against the freshly-read payload, up to ``max_attempts`` total
           attempts.

        Args:
            s3_client:     A boto3 S3 client.
            state_bucket:  The scratch/state bucket name.
            source_bucket: The Monitored_Bucket whose state to update.
            mutate_fn:     A pure function ``(dict) -> dict`` that reads and
                           writes only ``payload["completion_items"]``.
            max_attempts:  The maximum number of read-mutate-write attempts
                           before giving up. Defaults to 5.

        Returns:
            The new ETag returned by S3 on success.

        Raises:
            ValueError: If ``max_attempts`` is not a positive integer — a
                loop that never runs cannot produce a result, so this is
                rejected up front with a clear message instead of falling
                through to a bare ``AssertionError``.
            ConditionalWriteError: When every attempt up to ``max_attempts``
                fails the conditional-write precondition. Propagated to the
                caller (per design.md's Error Handling table) rather than
                swallowed, so that the caller's isolation boundary — the
                per-bucket inner loop inside
                ``_run_completion_tracking_interval`` — can catch and log it
                without affecting other buckets.
            ClientError: For other S3 errors.

        Requirements: 2.5, 3.6
        """
        if max_attempts <= 0:
            raise ValueError(
                f"max_attempts must be a positive integer, got {max_attempts!r}"
            )

        key = _state_key(source_bucket)

        last_error: ConditionalWriteError | None = None
        for _ in range(max_attempts):
            payload, current_etag = _read_state_payload(
                s3_client, state_bucket, key
            )
            if payload is None:
                state = _default_state(source_bucket)
                payload = json.loads(serialize(state))
                current_etag = None

            payload = mutate_fn(payload)
            try:
                return _write_state_payload(
                    s3_client,
                    state_bucket,
                    key,
                    payload,
                    current_etag,
                    self._kms_key_arn,
                )
            except ConditionalWriteError as exc:
                last_error = exc
                continue

        assert last_error is not None
        raise last_error

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
        key = _state_key(source_bucket)
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
        This matches the pattern used by ``apply_completion_resolutions``
        and ensures a call from ``check_report_handler`` — which calls
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
        key = _state_key(source_bucket)

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
        key = _state_key(source_bucket)

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

        Mirrors :meth:`merge_completion_configs`'s exact read-raw-JSON,
        mutate-one-key, single-attempt-conditional-write pattern (not
        :meth:`apply_completion_resolutions`'s retrying pattern): this method
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
        key = _state_key(source_bucket)

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
        key = _state_key(source_bucket)
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
        key = _state_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        streaks = payload.setdefault("submission_failure_streaks", {})
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
        key = _state_key(source_bucket)

        payload, _ = _read_state_payload(s3_client, state_bucket, key)
        if payload is None:
            state = _default_state(source_bucket)
            payload = json.loads(serialize(state))

        streaks = payload.get("submission_failure_streaks", {})
        if bucket_name in streaks:
            del streaks[bucket_name]
            payload["submission_failure_streaks"] = streaks

        return _write_state_payload(
            s3_client, state_bucket, key, payload, current_etag, self._kms_key_arn
        )
