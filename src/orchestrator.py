"""Run Orchestrator — coordinates a single Processing_Interval.

Loads configuration, derives rules per Monitored_Bucket, reads and deduplicates
Tagging_Operations from the journal, matches them against derived rules, accumulates
Matched_Objects, finalises manifests, submits at most one Batch_Replication_Job per
bucket replication configuration, advances checkpoints only on success, and emits a
single summary log entry.

The orchestrator owns no business logic itself; it delegates to:
  - core.config_loader      (Configuration_Loader)
  - core.rule_deriver       (Rule_Deriver — via replication_config_adapter)
  - core.rule_matcher       (Rule_Matcher)
  - core.journal_dedup      (Journal_Monitor dedup)
  - core.manifest_generator (Manifest_Generator)
  - core.observability      (Observability / Logger)
  - adapters.*              (thin AWS I/O shells)

When a bucket must be disabled, the orchestrator writes the ``disabled``,
``disabled_reason``, and ``disabled_at`` keys into that bucket's own state
object (``state/<bucket>.json``) through the same conditional-write ETag chain
as every other per-bucket write, then calls
``runtime_config['on_bucket_disabled'](bucket_name, reason)`` if provided so
the lambda handler can publish the recovery-instructions alert. The callback is
notification only: it fires after the flag is persisted and never when the
write failed, so an alert cannot tell an operator to re-enable a bucket that
was never disabled. The bucket is then skipped and the remaining buckets
continue.

The flag lives in the state object rather than in ``solution-config.json``
because the config custom resource rewrites that object wholesale from
template parameters on every stack create/update, which would clear a disable
on an unrelated deploy. See ``src.core.models.BucketDisableState``.

When a bucket's S3 Metadata journal cannot be found at all, the orchestrator
calls ``runtime_config['on_journal_unavailable'](bucket_name, cause)`` on the
first interval the condition is seen, then skips that bucket. The bucket is
left enabled: the remedy is in the operator's account, and the bucket resumes
on the next interval once the journal exists.

Requirements: 4.3, 6.1, 7.3, 8.1, 8.2, 8.3, 9.1, 9.3, 9.4, 11.1
"""
from __future__ import annotations

import contextlib
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from math import floor
from typing import Any
from collections.abc import Callable, Collection

from src.adapters import athena_journal_adapter
from src.adapters import batch_operations_adapter
from src.adapters import bops_report_reader
from src.adapters import replication_config_adapter
from src.adapters import state_store as state_store_module
from src.adapters.client_factory import ClientFactory
from src.adapters.inventory_manifest_writer import (
    InventoryManifestWriteError,
    WrittenManifest,
    write_in_memory_inventory_manifest,
)
from src.adapters.metrics_publisher import MetricsPublisher
from src.adapters.permanent_delete_reader import read_permanent_deletes
from src.adapters.preflight_counter import preflight_count
from src.adapters import sns_report_adapter
from src.core import completion_tracker
from src.core import config_loader
from src.core import journal_dedup
from src.core import observability
from src.core import rule_matcher
from src.core.archived_filter import (
    count_by_storage_class,
    filter_archived_operations,
)
from src.core.delete_filter import filter_deleted_versions
from src.core.manifest_generator import ManifestGenerator, serialize
from src.core.manifest_strategy import (
    JOURNAL_READ_ROW_CAP_DEFAULT,
    MIN_JOURNAL_READ_ROW_CAP,
    TAIL_ROW_BUDGET_FRACTION,
    ManifestFormat,
)
from src.core.models import (
    BucketMetrics,
    FailureClass,
    Lease,
    LeaseStatus,
    ManifestEntry,
    MatchedObject,
    MonitoredBucket,
    RunOutcome,
    RunResult,
    SubmissionRecord,
    SubmissionStatus,
    TrackedObject,
)
from src.core.job_recovery import (
    JobOutcome,
    RecoveryPlan,
    is_effective_failure,
    is_terminal_status,
    plan_recovery,
)
from src.core.row_cap_validation import split_row_budget
from src.core.watermark import subtract as watermark_subtract

logger = logging.getLogger(__name__)

_COMPONENT = "Orchestrator"

# Default journal lookback window.  Each run re-scans the journal from
# (watermark - lookback) so records that S3 Metadata delivered late (the
# journal is eventually consistent) are still picked up.  The bounded
# processed-operation window suppresses re-submission of records already
# included in a job, so the lookback never causes redundant replication.
#
# A record delivered later than this window is excluded permanently: once the
# watermark has advanced past (record_timestamp + lookback), is_eligible in
# src/core/checkpoint_logic.py returns False for it on every subsequent run, and
# nothing retries or alerts.  The window is therefore sized for observed S3
# Metadata delivery latency rather than for scan cost, since a missed tagging
# event is silently unreplicated until someone backfills it by hand
# (docs/backfill.md).
#
# Must stay in sync with the JournalLookbackSeconds default in
# deploy/template.yaml; tests/test_template.py asserts the two agree.  This
# value applies only when JOURNAL_LOOKBACK_SECONDS is unset, which a
# CloudFormation deploy never leaves unset, so it governs direct invocation and
# tests.
DEFAULT_JOURNAL_LOOKBACK = timedelta(hours=2)

# Minimum spacing between journal-unavailable alerts for the same bucket. The
# condition recurs on every interval until the journal exists, so an unbounded
# alert would arrive every CheckFrequencyMinutes indefinitely. A day keeps an
# unmet prerequisite in view without burying it.
JOURNAL_UNAVAILABLE_REALERT_INTERVAL = timedelta(hours=24)

# Default ceiling on Batch Operations jobs outstanding at once for one bucket.
#
# Three is enough that a job outlasting an interval or two does not stall the
# bucket, and small enough that a pathologically slow destination cannot run up
# per-job charges unnoticed. A value of 1 reproduces strict serialization, which
# is why the parameter's floor is 1 rather than 2.
#
# Must stay in sync with the MaxConcurrentJobsPerBucket default in
# deploy/template.yaml; tests/test_template.py asserts the two agree. This value
# applies only when MAX_CONCURRENT_JOBS_PER_BUCKET is unset, which a
# CloudFormation deploy never leaves unset, so it governs direct invocation and
# tests.
MAX_CONCURRENT_JOBS_DEFAULT = 3

# KMS key ARN format: arn:<partition>:kms:<region>:<account-id>:key/<key-id>
# or arn:<partition>:kms:<region>:<account-id>:alias/<alias-name>

_KMS_ARN_RE = re.compile(
    r"^arn:(aws|aws-cn|aws-us-gov):kms:[a-z0-9-]+:\d{12}:(key|alias)/.+$"
)


def _completion_report_prefix(replication_config_id: str, manifest_key: str) -> str:
    """Derive the State_Bucket prefix a job's BOPS_Completion_Report is
    written to (submission time) and read back from (DescribeJob loop),
    per design.md Decision 4.

    Keyed by ``replication_config_id`` and the job's ``manifest_key``, so a
    terminal job's report can be located and read back independently of any
    other job's report (Requirement 1.3). ``manifest_key`` (rather than
    ``job_id``) is used because it is the only per-job-unique identifier
    known BEFORE ``CreateJob`` is called — the ``Report.Prefix`` must be
    supplied in the CreateJob request itself, before S3 Batch Operations
    assigns a ``job_id`` (design.md Decision 4's "the prefix must be known
    at CreateJob time but the job_id is assigned by CreateJob" note). The
    same ``manifest_key`` is persisted on ``SubmissionRecord.manifest_key``
    and is available again later when the DescribeJob loop needs to read
    the report back for a given ``job_id``, so the submission-time and
    read-time prefixes always agree.

    S3 Batch Operations writes the top-level report manifest at
    ``job-<job_id>/manifest.json`` under this prefix. The manifest names the
    result CSV objects and their checksums; ``bops_report_reader`` reads that
    manifest and validates every declared result object.
    """
    sanitized_manifest_key = manifest_key.replace("/", "_")
    return f"completion-reports/{replication_config_id}/{sanitized_manifest_key}/"


# ---------------------------------------------------------------------------
# Permission-shaped BOPS_Completion_Report ErrorCode diagnosis
# ---------------------------------------------------------------------------

# ErrorCode values a BOPS_Completion_Report task row carries when the Batch
# Operations job role the stack created lacks a permission Batch Replication
# needs. Distinguished from an ordinary per-object failure (throttling, a
# missing source object, etc.) because every task in the job fails with the
# SAME one of these codes, and because the cause is the deployment's own role
# rather than anything about the objects themselves.
#
# These two are not the only codes reported: they are the two that get a
# cause-specific message. Every other non-empty ErrorCode is reported
# generically by _log_report_task_errors rather than being discarded.
_PERMISSION_SHAPED_ERROR_CODES = frozenset({
    "InitiateReplicationNotPermitted",
    "AccessDenied",
})

# The ErrorCode S3 Batch Operations reports when the source object is not
# eligible for replication. Verified against job
# 17a27c3a-aa18-4bc7-91a6-caeaaa28dd8c in the us-west-2 test deployment: an
# object in the GLACIER storage class, tagged to match the bucket's
# replication rule, failed with
# ``failed,500,SrcObjectNotEligible,Object is not eligible for replication``
# while an otherwise identical STANDARD object in the same job succeeded.
#
# The code is generic — the message names no storage class, and the same code
# covers other ineligibility conditions — so the diagnostic for it names
# archived storage classes as one possibility rather than as the cause.
#
# A second cause was confirmed by probe on 2026-08-28 (job
# a5fb3a8c-cea7-4668-b80b-0336c3d6c1ce, recorded in
# experiments/scan-aa27a832-probes/README.md): an object matched only by a
# replication rule whose ``Status`` is ``Disabled`` fails with the identical
# code and message. ``derive_rules`` now excludes such a rule, so the
# Solution no longer submits those objects, but a job submitted before that
# change can still report this. Treating the code as an archival signal
# therefore points an operator at the wrong thing.
#
# Two properties of the archived case are worth recording, because both are
# counter-intuitive and both were confirmed rather than assumed. They are
# specific to that case and are not claimed for the others:
#
# 1. It is reported with HTTP 500, but it is permanent and deterministic. No
#    retry can succeed while the object remains archived. Nothing should
#    treat this status code as transient.
# 2. The object is left completely untouched: replication is never
#    initiated, so the object acquires no x-amz-replication-status at all.
#    It does NOT enter FAILED, which matters because S3 Lifecycle blocks
#    transition and expiration on objects whose replication status is
#    PENDING or FAILED. An archived object rejected this way is therefore
#    not lifecycle-frozen by the attempt.
_SRC_OBJECT_NOT_ELIGIBLE = "SrcObjectNotEligible"


def _log_report_task_errors(
    entries: list[ManifestEntry],
    bucket_name: str,
    job_id: str,
    config_id: str,
) -> None:
    """Emit one diagnostic ``error`` log entry per distinct ``ErrorCode``
    found in a job's BOPS_Completion_Report entries.

    A job can reach ``Status: Complete`` with tasks that failed, and the
    job's own status never surfaces that. This reads the same
    completion-report entries already fetched for the completion-tracking
    merge, so no extra AWS call is made.

    Every distinct non-empty ``ErrorCode`` is reported. Three shapes of
    message are produced:

    * ``InitiateReplicationNotPermitted`` / ``AccessDenied`` — the job role
      created by this Solution's stack is missing a permission Batch
      Replication needs. The stack grants ``s3:InitiateReplication`` on every
      bucket in ``SourceBucketNames``, so either code indicates a defect in
      the deployment rather than something an operator misconfigured on a
      role they own. The message says so and names the
      ``BatchOperationsRoleArn`` output, matching README.md's "Every task in
      a job fails on a permission error" section.
    * ``SrcObjectNotEligible`` — the source object was not eligible for
      replication. The service code covers several conditions and its
      message names none of them, so the diagnostic lists the causes known
      to produce it — an archived storage class, and a rule whose ``Status``
      is ``Disabled`` — as possibilities rather than asserting either.
    * anything else — reported with the code, the count, and the report's
      own ``ResultMessage`` verbatim, so an unanticipated failure is visible
      rather than silently dropped. This is the case that previously
      produced no output at all.

    Deliberately does not attempt to distinguish "every task failed this
    way" from "one object out of many failed this way" — a single object can
    legitimately fail with ``AccessDenied`` for reasons unrelated to the job
    role (e.g. an object-level bucket policy deny), so this surfaces the code
    and count rather than asserting a root cause.

    Never raises — this is a best-effort diagnostic aid, called from within
    an already-isolated try/except in ``_CompletionHooks.on_job_terminal``.
    """
    # Count per code, and keep the first ResultMessage seen for each so the
    # generic branch has the service's own wording to quote. Sorted output
    # keeps a multi-code job's log entries in a deterministic order.
    counts: dict[str, int] = {}
    messages: dict[str, str | None] = {}
    for entry in entries:
        code = entry.error_code
        if not code:
            continue
        counts[code] = counts.get(code, 0) + 1
        if code not in messages:
            messages[code] = entry.result_message

    for code in sorted(counts):
        count = counts[code]
        prefix = (
            f"Batch Operations job {job_id!r} (config {config_id!r}) "
            f"reported {count} task(s) failing with {code!r}. "
        )
        if code in _PERMISSION_SHAPED_ERROR_CODES:
            cause = prefix + (
                "This "
                "usually means the stack-created Batch Operations job role "
                "is missing s3:InitiateReplication on this source bucket. "
                "The stack grants that action for every bucket in "
                "SourceBucketNames, so this is a defect in the deployment "
                "rather than a role you own: confirm the bucket is named in "
                "SourceBucketNames and check the policy on the role named by "
                "the BatchOperationsRoleArn stack output. See "
                "docs/permissions.md, Batch Operations job role."
            )
        elif code == _SRC_OBJECT_NOT_ELIGIBLE:
            cause = prefix + (
                "S3 reports this code for several ineligibility conditions "
                "and its message names none of them, so read it as one of "
                "the following rather than as a storage class problem. One "
                "possibility is an object in an archived storage class: S3 "
                "does not replicate objects in GLACIER or DEEP_ARCHIVE, or "
                "in the S3 Intelligent-Tiering Archive Access or Deep "
                "Archive Access tiers, until they are restored and copied to "
                "another storage class. Objects the Solution can identify as "
                "archived from the journal are excluded before submission "
                "and reported as archived_objects_excluded instead, so this "
                "code covers the objects it could not. In that case the "
                "object is left unchanged and no replication is initiated, "
                "so it carries no replication status and its lifecycle rules "
                "are unaffected. Another is an object matched only by a "
                "replication rule whose Status is Disabled; the Solution no "
                "longer derives such a rule, so a job submitted before that "
                "change can still report this. Other ineligibility "
                "conditions produce the same code. See README.md, Objects "
                "That Are Not Replicated."
            )
        else:
            detail = f" Reported message: {messages[code]!r}." if messages[code] else ""
            cause = prefix + (
                "This is a per-task failure, so the object was not "
                "replicated even if the job itself reached Complete." + detail +
                " The full completion report is under the completion-reports/ "
                "prefix of the State Bucket."
            )
        observability.emit(observability.log_error(
            component="Completion_Tracker",
            bucket=bucket_name,
            cause=cause,
        ))


# ---------------------------------------------------------------------------
# Completion-tracking collaborator — collapses six if-gates to one selection
# (design.md D5, Requirements 4.1–4.5).
# ---------------------------------------------------------------------------


class _CompletionHooks:
    """Active completion-tracking hooks for _process_bucket phases.

    Each method wraps a single best-effort operation; isolation is structural
    (the try/except lives here, once) rather than repeated at each call site.
    """

    def __init__(self, state_bucket: str, account_id: str) -> None:
        self._state_bucket = state_bucket
        self._account_id = account_id

    def on_job_terminal(
        self,
        store: state_store_module.StateStore,
        s3_client,
        writer: StateWriter,
        bucket_name: str,
        config_id: str,
        rec,
        job_status: str,
        job_response,
        *,
        report: Callable[[], bops_report_reader.BopsCompletionReport],
    ) -> None:
        """Atomically resolve a ready report and clear any stale alert.

        Accepts a lazy *report* accessor so the hook shares the one validated
        typed report read by the diagnosis block in ``_describe_prior_jobs``.
        Unknown task statuses are reported only after the state write succeeds.
        """
        if not is_terminal_status(job_status):
            return
        try:
            if not store.completion_job_exists(
                s3_client, self._state_bucket, bucket_name, rec.job_id
            ):
                completion_report = report()
                writer.merge_completion_report(
                    report=completion_report,
                    replication_config_id=bucket_name,
                    job_id=rec.job_id,
                    job_created_at=job_response["Job"]["CreationTime"],
                )
                unknown_count = sum(
                    completion_tracker.outcome_from_report_row(entry) == "UNKNOWN"
                    for entry in completion_report.entries
                )
                if unknown_count:
                    observability.emit(observability.log_error(
                        component="Completion_Tracker",
                        bucket=bucket_name,
                        cause=(
                            "BOPS completion report for job "
                            f"{rec.job_id!r} (config {config_id!r}) mapped "
                            f"{unknown_count} row(s) to UNKNOWN due to an "
                            "unrecognised task status."
                        ),
                    ))
                # Suppression is keyed by job_id, matching what
                # check_report_handler writes, so clearing one job's alert
                # cannot un-suppress another's. A bucket-name entry left by an
                # earlier build is inert: nothing reads it and nothing clears
                # it, and the list holds arbitrary strings so it needs no
                # schema change.
                alerted = store.get_alerted_configs(
                    s3_client, self._state_bucket, bucket_name
                )
                if rec.job_id in alerted:
                    writer.clear_alerted_config(
                        replication_config_id=rec.job_id,
                    )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.1 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket_name,
                cause=(
                    f"Failed to merge Config_Contexts for job "
                    f"{rec.job_id!r} (config {config_id!r}): {exc}"
                ),
            ))

    def record_scan(self, writer: StateWriter, bucket_name: str, match_count: int) -> None:
        """Record a scan result after preflight count."""
        try:
            writer.record_scan_result(
                bucket_name,
                scan_at=datetime.now(tz=UTC),
                match_count=match_count,
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.1/6.2 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket_name,
                cause=f"Failed to record scan result: {exc}",
            ))

    def record_idle_scan(self, writer: StateWriter, bucket_name: str) -> None:
        """Record an idle scan (zero matches) for quiescence tracking."""
        try:
            writer.record_scan_result(
                bucket_name,
                scan_at=datetime.now(tz=UTC),
                match_count=0,
            )
        except Exception as exc:  # noqa: BLE001 — Req 6.1/6.2 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket_name,
                cause=f"Failed to record idle scan result: {exc}",
            ))

class _NullCompletionHooks:
    """No-op implementation when completion tracking is unconfigured.

    Every method is a no-op returning None, so the unconfigured path issues
    no AWS call at all (Requirement 4.2).
    """

    def on_job_terminal(self, *_args, **_kwargs) -> None:  # noqa: ANN002
        return None

    def record_scan(self, *_args, **_kwargs) -> None:  # noqa: ANN002
        return None

    def record_idle_scan(self, *_args, **_kwargs) -> None:  # noqa: ANN002
        return None


_CompletionTracking = _CompletionHooks | _NullCompletionHooks
"""Named type for the completion-tracking collaborator parameter."""


def _validate_kms_key_arn(value: str) -> None:
    """Validate that *value* looks like a well-formed KMS key or alias ARN.

    Raises ``config_loader.ConfigError`` on format mismatch so the run fails
    fast at startup with a clear message rather than producing a cryptic
    ``InvalidKeyId`` or ``AccessDenied`` from a mid-run ``PutObject`` call.
    """
    stripped = value.strip()
    if not _KMS_ARN_RE.match(stripped):
        raise config_loader.ConfigError(
            f"Invalid kms_key_arn {value!r}. Expected format: "
            "arn:<partition>:kms:<region>:<account-id>:key/<key-id> or "
            "arn:<partition>:kms:<region>:<account-id>:alias/<alias-name>."
        )


# ---------------------------------------------------------------------------
# Per-bucket context — immutable invariants for a single _process_bucket run
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BucketContext:
    """Per-bucket invariants carried through every phase of _process_bucket.

    Frozen because nothing in a run should mutate its own context.  The mutable
    run-scoped things — the ETag, the accumulating _BucketResult — are passed
    separately and visibly, so a reader can tell at a glance which arguments a
    phase can change.
    """

    bucket: MonitoredBucket
    bucket_name: str
    s3_client: Any
    athena_client: Any
    s3control_client: Any
    state_bucket: str
    athena_workgroup: str
    athena_output_location: str
    account_id: str
    # The stack-created S3 Batch Operations job role, passed as every job's
    # RoleArn. A deployment-derived identifier in the same class as
    # ``state_bucket`` and ``account_id``, never read from customer data.
    batch_operations_role_arn: str
    kms_key_arn: str
    lookback: timedelta
    journal_read_row_cap: int
    max_batch_job_failures: int
    # The most Batch Operations jobs that may be outstanding for this bucket at
    # once. Submission is deferred at the limit rather than serialized, because
    # a job's duration is set by replication throughput rather than task count:
    # a bandwidth-bound bucket of large objects has jobs lasting hours, so
    # serializing would extend head-of-line blocking across batches without
    # bound. 1 reproduces strict serialization for an operator who wants it.
    max_concurrent_jobs: int
    on_bucket_disabled: Callable | None
    on_submission_failure: Callable | None
    # Invoked on the first interval in which the bucket's S3 Metadata journal
    # is found to be absent. Defaults to None so a library caller that does not
    # supply it keeps the log-only behavior.
    on_journal_unavailable: Callable | None = None


# ---------------------------------------------------------------------------
# StateWriter — owns the live conditional-write ETag for one bucket (D3)
# ---------------------------------------------------------------------------


class StateWriter:
    """Owns the live conditional-write ETag for one bucket's state object.

    Each method calls the corresponding ``StateStore`` method with the held
    ETag and stores the returned one.  Callers never see an ETag, so they
    cannot pass a stale one — Requirement 3.3 becomes structural.

    Constructed inside ``src/orchestrator.py`` from the ``StateStore``
    instance the module already builds, so ``state_store_module.StateStore``
    remains a valid patch target (D1).
    """

    def __init__(
        self,
        store: state_store_module.StateStore,
        s3_client: Any,
        state_bucket: str,
        source_bucket: str,
        etag: str | None,
    ) -> None:
        self._store = store
        self._s3_client = s3_client
        self._state_bucket = state_bucket
        self._source_bucket = source_bucket
        self._etag = etag

    def acquire_lease(self, lease: Lease) -> None:
        """Acquire a lease, updating the held ETag."""
        self._etag = self._store.acquire_lease(
            self._s3_client, self._state_bucket, self._source_bucket,
            lease, self._etag,
        )

    def release_lease(
        self,
        submitted_refs,
        lookback: timedelta,
        candidate_max_watermark: str | None = None,
    ) -> None:
        """Release the lease, updating the held ETag."""
        self._etag = self._store.release_lease(
            s3_client=self._s3_client,
            state_bucket=self._state_bucket,
            source_bucket=self._source_bucket,
            submitted_refs=submitted_refs,
            lookback=lookback,
            current_etag=self._etag,
            candidate_max_watermark=candidate_max_watermark,
        )

    def record_submission(
        self,
        record: SubmissionRecord,
        *,
        terminal_job_ids: Collection[str] = (),
        completion_tracking_enabled: bool = True,
        max_concurrent_jobs: int | None = None,
    ) -> None:
        """Persist a submission record, updating the held ETag.

        The pruning arguments ride along on this one write rather than getting
        their own: the record for the new job and the removal of settled ones are
        the same edit to the same key, so splitting them would cost a second ETag
        hop and could leave the two out of step.
        """
        self._etag = self._store.record_submission(
            self._s3_client, self._state_bucket, record, self._etag,
            terminal_job_ids=terminal_job_ids,
            completion_tracking_enabled=completion_tracking_enabled,
            max_concurrent_jobs=max_concurrent_jobs,
        )

    def record_scan_result(
        self, config_id: str, scan_at: datetime, match_count: int,
    ) -> None:
        """Persist a scan result, updating the held ETag."""
        self._etag = self._store.record_scan_result(
            self._s3_client, self._state_bucket, self._source_bucket,
            config_id,
            scan_at=scan_at,
            match_count=match_count,
            current_etag=self._etag,
        )

    def merge_completion_report(
        self,
        report: bops_report_reader.BopsCompletionReport,
        replication_config_id: str,
        job_id: str,
        job_created_at: datetime,
        timestamps: dict | None = None,
    ) -> None:
        """Resolve a ready report and record its job ID, updating the ETag."""
        self._etag = self._store.merge_completion_report(
            self._s3_client, self._state_bucket, self._source_bucket,
            report=report,
            replication_config_id=replication_config_id,
            job_id=job_id,
            job_created_at=job_created_at,
            current_etag=self._etag,
            timestamps=timestamps,
        )

    def store_completion_timestamps(
        self, timestamps: dict, routing: dict | None = None,
    ) -> None:
        """Persist per-object report metadata, updating the held ETag.

        Timestamps and routing (matched rules, destination buckets) are
        written together in one call so a single ETag hop covers both.
        """
        self._etag = self._store.store_completion_timestamps(
            self._s3_client, self._state_bucket, self._source_bucket,
            timestamps=timestamps,
            current_etag=self._etag,
            routing=routing,
        )

    def clear_alerted_config(self, replication_config_id: str) -> None:
        """Clear an alerted config, updating the held ETag."""
        self._etag = self._store.clear_alerted_config(
            self._s3_client, self._state_bucket, self._source_bucket,
            replication_config_id=replication_config_id,
            current_etag=self._etag,
        )

    def increment_submission_failure_streak(self, bucket_name: str) -> int:
        """Increment the failure streak, updating the held ETag.

        Returns the new streak value.
        """
        new_value, new_etag = self._store.increment_submission_failure_streak(
            self._s3_client, self._state_bucket, self._source_bucket,
            bucket_name, current_etag=self._etag,
        )
        self._etag = new_etag
        return new_value

    def clear_submission_failure_streak(self, bucket_name: str) -> None:
        """Clear the failure streak, updating the held ETag."""
        self._etag = self._store.clear_submission_failure_streak(
            self._s3_client, self._state_bucket, self._source_bucket,
            bucket_name, current_etag=self._etag,
        )

    def disable_bucket(self, reason: str, now: datetime) -> None:
        """Record the bucket as disabled and clear its job history, updating
        the held ETag.

        Routed through the writer rather than written self-contained because
        the submission-streak call site runs inside ``_lease_scope``: a write
        managing its own ETag would invalidate the one held here, and the
        ``release_lease`` in that scope's ``finally`` would then fail and
        strand the lease in the state object.
        """
        self._etag = self._store.disable_bucket(
            self._s3_client, self._state_bucket, self._source_bucket,
            reason=reason, now=now, current_etag=self._etag,
        )

    def journal_unavailable_alert_due(
        self, bucket_name: str, now: datetime,
    ) -> bool:
        """Whether this bucket's journal-unavailable alert is due to be sent.

        Reads only, so nothing is committed before delivery is attempted. The
        matching :meth:`record_journal_unavailable_alert` does the write.
        """
        return self._store.journal_unavailable_alert_due(
            self._s3_client, self._state_bucket, self._source_bucket,
            bucket_name, now=now,
            min_interval=JOURNAL_UNAVAILABLE_REALERT_INTERVAL,
        )

    def record_journal_unavailable_alert(
        self, bucket_name: str, now: datetime,
    ) -> None:
        """Record that this bucket's journal-unavailable alert was sent.

        Reached only on a path that abandons the bucket for this run, so this
        write never contributes a link to the ETag chain used by a subsequent
        write.
        """
        self._etag = self._store.record_journal_unavailable_alert(
            self._s3_client, self._state_bucket, self._source_bucket,
            bucket_name, now=now, current_etag=self._etag,
        )

    def mark_report_diagnosed(
        self,
        job_id: str,
        *,
        report_diagnosed: bool = True,
        recovery_scored: bool = False,
    ) -> None:
        """Best-effort: set the named per-job flags on the job's record.

        A failure is logged and swallowed. It costs one duplicate diagnostic log
        and, where ``recovery_scored`` was the flag that did not land, one
        duplicate watermark rollback and resubmission on the next run. Both stop
        as soon as a write lands, and neither is worth failing a run that has
        already submitted a job over.

        Requirement 3.1.
        """
        flags = [
            name for name, on in (
                ("report_diagnosed", report_diagnosed),
                ("recovery_scored", recovery_scored),
            ) if on
        ]
        if not flags:
            return
        try:
            self._etag = self._store.mark_report_diagnosed(
                self._s3_client, self._state_bucket, self._source_bucket,
                job_id, current_etag=self._etag,
                report_diagnosed=report_diagnosed,
                recovery_scored=recovery_scored,
            )
        except Exception as exc:  # noqa: BLE001 — best-effort, Req 3.1
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=self._source_bucket,
                cause=(
                    f"Failed to persist {', '.join(flags)} for job "
                    f"{job_id!r}: {exc}. The work those flags record as done may "
                    f"repeat on the next run."
                ),
            ))


# ---------------------------------------------------------------------------
# Internal result type — carries per-bucket counters back to the caller
# ---------------------------------------------------------------------------


@dataclass
class _BucketResult:
    """Aggregated counters from processing one Monitored_Bucket."""

    ops_read: int = 0       # distinct logical operations forwarded to matching (after dedup and the Archived_Object_Filter)
    raw_records: int = 0    # raw journal records read before dedup
    archived_excluded: int = 0  # operations dropped by the Archived_Object_Filter (GLACIER / DEEP_ARCHIVE)
    matched: int = 0        # total Matched_Object entries accumulated
    submitted: int = 0      # successful Batch_Replication_Job submissions
    errored: bool = False   # True when the bucket was skipped due to a processing error
    capped: bool = False    # True when this run was a Capped_Run (journal_until is not None)
    progressed: bool = False  # True when a job was submitted AND the checkpoint advanced
    # True when the lookback tail was truncated from below to fit the row budget,
    # so part of the configured lookback window was not re-scanned this run. Not
    # an error in itself, but a reduction in late-arrival tolerance forced by a
    # backlog, so it is surfaced rather than absorbed.
    tail_shortened: bool = False
    # True when this bucket was skipped because its outstanding Batch Operations
    # job count had reached MaxConcurrentJobsPerBucket. Not an error: the work is
    # deferred, not lost.
    submission_deferred: bool = False
    # Batch Operations jobs outstanding for this bucket, including any this run
    # submitted. Reported to completion-report subscribers as `outstanding_jobs`.
    #
    # None until the DescribeJob loop has actually run, and it stays None on every
    # path that returns before then: a client-creation failure, a rule-resolution
    # failure, or a checkpoint read failure. Zero would be a claim, and the claim
    # would be wrong — a report saying nothing remains in tracking for a bucket
    # whose jobs were never checked is the false all-clear this design removes.
    outstanding_jobs: int | None = None


@dataclass(frozen=True)
class _BucketRunState:
    """What the publish phase needs to know about this run's per-bucket outcome.

    The publish phase reads only completion items and scan state from the state
    object, deliberately: it is isolated so a per-bucket processing failure
    cannot affect it. These two values are the exception — they are produced
    during processing and cannot be re-derived at publish time without another
    round trip — so they are threaded across that boundary rather than the two
    phases being merged.

    ``outstanding_jobs`` of ``None`` means the count is not known, which is the
    right answer for a bucket that never got as far as checking its jobs and for
    one skipped as disabled. The default therefore has to be ``None`` rather than
    zero: a bucket is skipped as disabled precisely because its jobs kept failing,
    and its report claiming nothing remains in tracking would be the worst place
    to be wrong.

    ``submission_deferred`` needs no unknown case. It answers "did the most recent
    run skip this bucket at the concurrency limit", and for a bucket that never ran
    the answer is a plain no.
    """

    outstanding_jobs: int | None = None
    submission_deferred: bool = False


# ---------------------------------------------------------------------------
# Extracted phases — called from _process_bucket
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BucketSkip:
    """Why ``run_interval`` is skipping a bucket before ``_process_bucket`` runs.

    ``counts_as_disabled`` feeds the run-level ``DisabledBuckets`` metric.
    ``metrics`` is the ``BucketMetrics`` entry to publish for the bucket, or
    ``None`` to publish nothing — a disabled bucket deliberately publishes
    nothing, so that a missing ``BucketErrors`` datum stays a usable signal
    that a bucket has stopped being processed.
    """

    counts_as_disabled: bool
    metrics: BucketMetrics | None


def _check_bucket_disabled(
    bucket: MonitoredBucket,
    factory: ClientFactory,
    store: state_store_module.StateStore,
    state_bucket: str,
) -> _BucketSkip | None:
    """Return why to skip *bucket*, or ``None`` to process it.

    A bucket whose disable state cannot be read is skipped rather than assumed
    enabled. The flag is a cost control: guessing "enabled" on a transient read
    error is what would let a bucket whose jobs keep failing resume submitting
    billable jobs. Guessing costs at most one interval, because the same read
    is retried on the next run, and the skip is reported as a bucket error so
    it is not silent.
    """
    bucket_name = bucket.name
    try:
        s3_client = factory.create_s3_client(region=bucket.region)
        disable_state = store.get_disable_state(
            s3_client, state_bucket, bucket_name
        )
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"Failed to read the disabled flag from the state object: "
                f"{exc}. Skipping the bucket this run rather than processing "
                f"it as enabled."
            ),
        ))
        return _BucketSkip(
            counts_as_disabled=False,
            metrics=BucketMetrics(
                source_bucket=bucket_name,
                ops_read=0,
                matched=0,
                submitted=0,
                errored=True,
            ),
        )

    if not disable_state.disabled:
        return None

    observability.emit(observability.log_error(
        component=_COMPONENT,
        bucket=bucket_name,
        cause=(
            f"Bucket is disabled in its state object "
            f"(disabled_at={disable_state.at!r}, "
            f"reason={disable_state.reason!r}). Re-enable by setting "
            f'"disabled": false in '
            f"s3://{state_bucket}/"
            f"{state_store_module.state_object_key(bucket_name)}."
        ),
    ))
    return _BucketSkip(counts_as_disabled=True, metrics=None)


def _create_clients(
    bucket: MonitoredBucket,
    factory: ClientFactory,
    result: _BucketResult,
) -> tuple[Any, Any, Any] | None:
    """Step 0: create per-bucket regional AWS clients.

    Returns (s3_client, athena_client, s3control_client) on success, or None
    if client creation fails (in which case *result* is marked errored and the
    failure is logged).
    """
    bucket_name = bucket.name
    try:
        s3_client = factory.create_s3_client(region=bucket.region)
        athena_client = factory.create_athena_client(region=bucket.region)
        s3control_client = factory.create_s3control_client(
            region=bucket.region
        )
    except Exception as exc:  # noqa: BLE001
        entry = observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"Failed to create regional clients for region "
                f"{bucket.region!r}: {exc}"
            ),
        )
        observability.emit(entry)
        result.errored = True
        return None
    return s3_client, athena_client, s3control_client


def _resolve_rules(
    ctx: _BucketContext,
) -> list | None:
    """Step a: derive the bucket's tag-scoped replication rules.

    Returns the list of rules on success, or None when the bucket has no
    tag-scoped rule to act on. Nothing about the bucket's replication role is
    inspected: the job role is the stack-created Batch Operations role, so a
    bucket is never skipped for a role-shaped reason.
    """
    rules, skip_reports = replication_config_adapter.get_replication_rules(
        ctx.s3_client, ctx.bucket
    )
    for skip in skip_reports:
        entry = observability.log_error(
            component=skip.component,
            bucket=skip.source_bucket,
            cause=skip.reason,
        )
        observability.emit(entry)

    if not rules:
        return None

    return rules


# ---------------------------------------------------------------------------
# Failed-job recovery phase — extracted from Step b2
# ---------------------------------------------------------------------------


# The status recorded for a job whose DescribeJob call failed. Such a job counts
# toward the concurrency limit: its status is unknown, and assuming it finished
# is the unsafe assumption — it would let the bucket look under its limit and
# admit a job it should not.
_UNKNOWN_JOB_STATUS = "Unknown"

# How long a record whose DescribeJob keeps failing goes on counting toward the
# concurrency limit before it stops.
#
# Without a bound the bucket stalls permanently. A record only leaves the count by
# being described at a terminal status, and the two paths that delete a record —
# pruning and ceiling eviction — both run inside the write that persists a
# submission, which the deferral itself prevents. The circuit breaker cannot break
# the tie either: with every describe failing, `outcomes` is empty, so plan_recovery
# holds the counter and never disables the bucket. Reachable without anything
# exotic: a lost `s3:DescribeJob` permission, a hand-edited job id, or a job whose
# record has aged out of Batch Operations retention. At MaxConcurrentJobsPerBucket
# of 1 a single such record stops the bucket for good.
#
# Ageing it out of the *count* loses nothing, because the record itself is kept:
# check_report_handler still alerts on it and eviction still reaches it eventually.
# And the bound only governs cost, not correctness — the data loss this release
# fixes is fixed by keying records per job, so admitting one extra job costs one
# per-job charge rather than risking an abandoned object.
#
# 14 days is far beyond any plausible job. The probe behind this design measured
# 50 GiB in five minutes cross-Region, which puts even a petabyte inside a week,
# and it sits well within the 90 days S3 Batch Operations retains a job record.
_UNDESCRIBABLE_JOB_MAX_AGE = timedelta(days=14)


def _record_age(record: SubmissionRecord, now: datetime) -> timedelta | None:
    """How long ago *record* was submitted, or ``None`` if that cannot be told.

    A hand-edited state object can carry a naive ``submitted_at``; UTC is assumed
    rather than raising, because the alternative is an exception on the path that
    decides whether a bucket may submit.
    """
    submitted_at = record.submitted_at
    if submitted_at is None:
        return None
    if submitted_at.tzinfo is None:
        submitted_at = submitted_at.replace(tzinfo=UTC)
    return now - submitted_at


@dataclass(frozen=True)
class _InFlightJob:
    """A prior job for this bucket that has not been shown to have finished."""

    job_id: str
    status: str
    created_at: datetime | None
    # From the bucket's SubmissionRecord rather than from DescribeJob, so a job
    # whose describe call failed still orders against one that succeeded. The
    # oldest outstanding job by this field is the one an operator investigates.
    submitted_at: datetime | None = None

    def elapsed_seconds(self, now: datetime) -> float | None:
        """Seconds since the job was created, or ``None`` if unknown."""
        if self.created_at is None:
            return None
        return (now - self.created_at).total_seconds()


def _oldest_outstanding(jobs: list[_InFlightJob]) -> _InFlightJob | None:
    """The longest-outstanding job in *jobs* by ``submitted_at``.

    A job with no ``submitted_at`` sorts last rather than raising, so an
    unparseable timestamp cannot break the deferral audit entry. Returns
    ``None`` for an empty list.
    """
    if not jobs:
        return None
    return min(
        jobs,
        key=lambda job: (
            job.submitted_at is None,
            job.submitted_at.timestamp() if job.submitted_at is not None else 0.0,
        ),
    )


@dataclass
class _JobCheckResult:
    """Outcome of the DescribeJob loop for one bucket's prior submissions."""

    failed_lows: list[str]
    any_check_ran: bool
    any_check_failed: bool
    outcomes: list[JobOutcome]
    # Every prior job for this bucket not shown to have finished: those at a
    # non-terminal status, and those whose DescribeJob call failed. Submission is
    # deferred once this reaches MaxConcurrentJobsPerBucket — see _process_bucket.
    outstanding: list[_InFlightJob] = field(default_factory=list)
    # Job IDs observed at a terminal status this run. The state store needs them
    # to decide which submission records have settled and may be pruned; the
    # record's own `status` field is written as SUBMITTED and never updated, so it
    # cannot answer that.
    terminal_job_ids: list[str] = field(default_factory=list)
    # Job IDs scored for the first time this run, whose `recovery_scored` flag has
    # NOT been committed yet. The flag is a durable once-only gate on work that is
    # not itself durable — the rollback it authorizes is an in-memory mutation of
    # `last_processed_watermark` that only becomes real once the readmitted range
    # is resubmitted — so committing it here would let any early return between
    # this loop and submission consume the readmission and drop the range for
    # good. The caller commits these once the work has landed; see
    # `_commit_recovery_scored`. Requirements 5.1, 5.2.
    newly_scored_job_ids: list[str] = field(default_factory=list)


def _describe_prior_jobs(
    ctx: _BucketContext,
    writer: StateWriter,
    store: state_store_module.StateStore,
    prev_submissions: dict[str, SubmissionRecord],
    tracking: _CompletionTracking,
) -> _JobCheckResult:
    """Wrap the DescribeJob loop: check every prior submission's job status.

    For each prior submission record with a job_id, calls DescribeJob to get
    the terminal status.  Runs the completion-merge hook (positioned before
    the Failed/Cancelled circuit-breaker logic so an exception there cannot
    reach it).  Collects watermark_lows of failed jobs and emits readmission
    audit logs.

    ``prev_submissions`` is keyed by ``job_id`` and holds one entry per
    outstanding or unsettled job, so this loop visits every job the bucket has
    in flight rather than a single record. Each terminal job's report is merged
    and diagnosed exactly once, under the existing
    ``completion_processed_job_ids`` idempotency gate.

    The consolidation arithmetic is performed by ``plan_recovery`` in
    ``src.core.job_recovery`` — this function collects the raw outcomes and
    the caller passes them to that pure decision function.

    Returns a _JobCheckResult with the loop's collected state, including the ids
    of the jobs scored for the first time this run. Their ``recovery_scored``
    flags are deliberately left uncommitted here — see ``_commit_recovery_scored``
    for why the caller commits them instead.

    Requirements: 1.3, 2.1, 5.1, 5.2
    """
    bucket_name = ctx.bucket_name
    s3_client = ctx.s3_client
    s3control_client = ctx.s3control_client
    account_id = ctx.account_id

    failed_lows: list[str] = []
    any_check_ran = False
    any_check_failed = False
    outcomes: list[JobOutcome] = []
    outstanding: list[_InFlightJob] = []
    terminal_job_ids: list[str] = []
    newly_scored_job_ids: list[str] = []

    for rec in prev_submissions.values():
        if not rec.job_id:
            continue
        # The completion report's State_Bucket prefix is written at submission
        # time from the bucket name (see _leased_manifest_and_submit), so the
        # read side must derive it the same way. Deliberately not the record's
        # dict key, which is the job_id, nor rec.replication_config_id, which a
        # hand-edited or legacy record could carry something else in.
        config_id = bucket_name
        try:
            resp = s3control_client.describe_job(
                AccountId=account_id, JobId=rec.job_id
            )
            job_status = resp["Job"]["Status"]
            progress_summary = resp["Job"].get("ProgressSummary", {})
            tasks_succeeded = progress_summary.get("NumberOfTasksSucceeded")
            tasks_failed = progress_summary.get("NumberOfTasksFailed")
        except Exception as exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"DescribeJob {rec.job_id!r} failed (best-effort): {exc}",
            ))
            # Counted as outstanding. A transport error says nothing about
            # whether the job finished, and treating it as finished would let the
            # bucket look under its concurrency limit and submit alongside a job
            # that is still running. One record's describe failing does not
            # disturb the others.
            #
            # But only up to _UNDESCRIBABLE_JOB_MAX_AGE, or the bucket stalls for
            # good — see that constant for why nothing else can free the slot. The
            # record is kept either way; it just stops blocking.
            age = _record_age(rec, datetime.now(tz=UTC))
            if age is not None and age > _UNDESCRIBABLE_JOB_MAX_AGE:
                observability.emit(observability.log_error(
                    component=_COMPONENT,
                    bucket=bucket_name,
                    cause=(
                        f"Job {rec.job_id!r} was submitted {age.days} days ago and "
                        f"still cannot be described, so it no longer counts toward "
                        f"MaxConcurrentJobsPerBucket and this bucket can submit "
                        f"again. Its outcome is unknown: no completion report will "
                        f"be read for it and it cannot be rolled back. Check that "
                        f"the execution role still holds s3:DescribeJob and that "
                        f"the job id in the state object is one this account owns."
                    ),
                ))
                continue
            outstanding.append(_InFlightJob(
                job_id=rec.job_id,
                status=_UNKNOWN_JOB_STATUS,
                created_at=None,
                submitted_at=rec.submitted_at,
            ))
            continue

        # A job that has not finished is reported, not scored. Everything below
        # this point either self-gates on a terminal status (the completion merge
        # and the report diagnosis) or would draw a wrong conclusion from a
        # running job, so the record is carried out as outstanding and skipped.
        #
        # Deliberately NOT appended to `outcomes`. plan_recovery reads a
        # non-empty `outcomes` with no failures as "a check ran and passed" and
        # resets the bucket's consecutive-failure counter, so counting a running
        # job there would let a bucket whose jobs keep failing reset its own
        # counter whenever a fresh job happened to be in flight, and the circuit
        # breaker would never trip. An empty `outcomes` takes plan_recovery's
        # rule 4 and holds the counter, which is the correct reading: nothing has
        # been learned yet.
        if not is_terminal_status(job_status):
            outstanding.append(_InFlightJob(
                job_id=rec.job_id,
                status=job_status,
                created_at=resp["Job"].get("CreationTime"),
                submitted_at=rec.submitted_at,
            ))
            continue

        terminal_job_ids.append(rec.job_id)

        outcome = JobOutcome(
            config_id=config_id,
            job_id=rec.job_id,
            status=job_status,
            watermark_low=rec.watermark_low,
            watermark_high=rec.watermark_high,
            consecutive_failures=rec.consecutive_failures,
            tasks_succeeded=tasks_succeeded,
            tasks_failed=tasks_failed,
        )

        # Score a terminal job's outcome exactly once, ever.
        #
        # A record now outlives the run in which its job finished: Requirement 3.2
        # keeps a terminal record whose completion report has not been read, so
        # check_report_handler can alert on it. Re-scoring such a record every run
        # would roll the watermark back to the same watermark_low each time,
        # resubmit the same journal range at a fresh per-job charge each time, and
        # climb consecutive_failures until the breaker disabled the bucket with a
        # reason claiming N consecutive failures for one job that failed once.
        #
        # `any_check_ran` is set here too rather than for every terminal job. An
        # already-scored job is not evidence about this interval: counting it would
        # take plan_recovery's rule 3 and reset the bucket's failure counter on a
        # run that learned nothing, which is the same defect a non-terminal job
        # already avoids by staying out of `outcomes`.
        newly_scored = not rec.recovery_scored
        if newly_scored:
            any_check_ran = True
            outcomes.append(outcome)

        # Cache one validated typed report per terminal job. Both the merge
        # and diagnosis consume its entries, so a missing or malformed report
        # cannot be mistaken for a valid empty report. Zero invoked tasks are
        # the sole exception: S3 does not emit a report, so recovery uses a
        # synthetic empty report anchored at the terminal time.
        _report: bops_report_reader.BopsCompletionReport | None = None
        _report_error: Exception | None = None
        _report_attempted = False

        def _completion_report() -> bops_report_reader.BopsCompletionReport:
            nonlocal _report, _report_attempted, _report_error
            if _report is not None:
                return _report
            if _report_error is not None:
                raise _report_error
            if _report_attempted:
                raise RuntimeError("completion report read did not produce a result")

            _report_attempted = True
            # An absent ProgressSummary means the invoked-task count is
            # unknown, not zero. Treating it as zero would take the
            # synthetic-empty-report branch below and mark the job processed
            # with no outcomes, silently dropping every object in it. Leave the
            # job retryable instead, matching how job_recovery and
            # check_report_handler already refuse to read absence as zero.
            if tasks_succeeded is None or tasks_failed is None:
                _report_error = bops_report_reader.CompletionReportNotReady(
                    f"DescribeJob for job {rec.job_id!r} carried no ProgressSummary; "
                    "invoked task count is unknown"
                )
                raise _report_error

            expected_row_count = tasks_succeeded + tasks_failed
            try:
                if expected_row_count == 0:
                    # A terminal job should carry TerminationDate, but fall
                    # back rather than raising KeyError into _report_error,
                    # which would make the job retry every interval forever.
                    # check_report_handler already uses the same fallback.
                    job = resp["Job"]
                    terminal_at = job.get("TerminationDate") or job.get("CreationTime")
                    if terminal_at is None:
                        raise bops_report_reader.CompletionReportNotReady(
                            f"DescribeJob for job {rec.job_id!r} carried neither "
                            "TerminationDate nor CreationTime"
                        )
                    _report = bops_report_reader.BopsCompletionReport(
                        created_at=terminal_at,
                        entries=(),
                    )
                else:
                    _report = bops_report_reader.read_bops_completion_report(
                        s3_client,
                        ctx.state_bucket,
                        _completion_report_prefix(config_id, rec.manifest_key),
                        rec.job_id,
                        expected_row_count,
                    )
            except Exception as exc:
                _report_error = exc
                raise
            return _report

        def _report_entries() -> list[ManifestEntry]:
            return list(_completion_report().entries)

        # Completion-tracking merge hook — positioned before the
        # Failed/Cancelled circuit-breaker logic so an exception here cannot
        # reach it (design.md Decision 9.1).
        tracking.on_job_terminal(
            store=store,
            s3_client=s3_client,
            writer=writer,
            bucket_name=bucket_name,
            config_id=config_id,
            rec=rec,
            job_status=job_status,
            job_response=resp,
            report=_completion_report,
        )

        # Report-based diagnosis — isolated so a read or parse failure
        # cannot disturb the recovery arithmetic below.  Fires for any
        # terminal job whose report has not yet been diagnosed, including
        # partial failures that is_effective_failure deliberately does not
        # flag (Requirement 2.2).
        diagnosed_now = False
        try:
            if is_terminal_status(job_status) and not rec.report_diagnosed:
                _log_report_task_errors(
                    entries=_report_entries(),
                    bucket_name=bucket_name,
                    job_id=rec.job_id,
                    config_id=config_id,
                )
                diagnosed_now = True
        except Exception as exc:  # noqa: BLE001 — Requirement 2.5 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket_name,
                cause=(
                    f"Failed to read or diagnose BOPS_Completion_Report for "
                    f"job {rec.job_id!r} (config {config_id!r}): {exc}"
                ),
            ))

        # `report_diagnosed` is committed here and `recovery_scored` is not, even
        # though the store can set both in one write. They are set on different
        # conditions — a job whose report is unreadable is scored but not
        # diagnosed, which is the case that makes re-scoring reachable at all —
        # and, more importantly, they gate work of different durability.
        #
        # Diagnosis is complete the moment its log entry is emitted, so a flag
        # written now can only ever suppress a duplicate log. Scoring authorizes a
        # watermark rollback that lives in memory until a resubmission persists
        # it, so a flag written now would suppress the rollback on every later run
        # while the range it readmitted was never resubmitted. The scored ids are
        # carried out for the caller to commit after the work lands.
        if diagnosed_now:
            writer.mark_report_diagnosed(
                rec.job_id,
                report_diagnosed=True,
            )
        if newly_scored:
            newly_scored_job_ids.append(rec.job_id)

        if not newly_scored:
            # Already scored on an earlier run. Everything below is a
            # once-per-job effect, so re-running it would repeat a rollback, a
            # resubmission, and a failure count for an outcome already acted on.
            continue

        # A job that reaches Complete with every task failed replicated
        # nothing, even though Job.Status alone reads as success.  The
        # report-based entry (_log_report_task_errors) carries the actual
        # ErrorCode for every task failure; this entry records the
        # task-count evidence only.
        if is_effective_failure(outcome) and job_status == "Complete":
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=(
                    f"Batch Operations job {rec.job_id!r} (config {config_id!r}) "
                    f"reached Complete but every task failed "
                    f"(NumberOfTasksSucceeded={tasks_succeeded}, "
                    f"NumberOfTasksFailed={tasks_failed}). No object was "
                    f"replicated. Check the completion-report diagnostic "
                    f"entry for the specific error code."
                ),
            ))

        if is_effective_failure(outcome):
            any_check_failed = True
            failed_lows.append(rec.watermark_low)
            observability.emit(observability.log_audit(
                action="batch_job_failure_readmit",
                source_bucket=bucket_name,
                details={
                    "job_id": rec.job_id,
                    "config_id": config_id,
                    "watermark_low": rec.watermark_low,
                    "watermark_high": rec.watermark_high,
                },
            ))

    return _JobCheckResult(
        failed_lows=failed_lows,
        any_check_ran=any_check_ran,
        any_check_failed=any_check_failed,
        outcomes=outcomes,
        outstanding=outstanding,
        terminal_job_ids=terminal_job_ids,
        newly_scored_job_ids=newly_scored_job_ids,
    )


def _commit_recovery_scored(
    writer: StateWriter,
    job_ids: list[str],
) -> None:
    """Commit the `recovery_scored` flag for jobs whose scoring has taken effect.

    Called only from a point past which the work the flag authorizes has
    happened: either the readmitted range has been resubmitted, or the bucket has
    been disabled by a threshold breach and will submit nothing again until an
    operator intervenes. Committing earlier is the defect Requirement 5 describes
    — `advance_checkpoint` is monotonic by design (design R3), so nothing persists
    a rollback, and a flag committed before the resubmission would leave the
    readmitted range gated out on every later run.

    Best-effort, like every other call into `mark_report_diagnosed`: a failed
    write costs one duplicate rollback and resubmission on the next run, which is
    the pre-existing cost of that path and not worth failing a run over.

    Requirements: 5.1, 5.2
    """
    for job_id in job_ids:
        writer.mark_report_diagnosed(
            job_id,
            report_diagnosed=False,
            recovery_scored=True,
        )


def _disable_bucket(
    ctx: _BucketContext,
    writer: StateWriter,
    reason: str,
) -> None:
    """Persist the bucket's disable flag, then notify.

    Order matters: the notification follows a successful write and nothing
    else, so an alert never tells an operator to re-enable a bucket that was
    never disabled. A failed write still leaves the caller skipping the bucket
    for this run, and the condition that triggered the disable is re-evaluated
    on the next run and disables it then, so a lost write costs one interval
    rather than the protection itself.

    Deliberately returns nothing. Whether the write landed makes no difference
    to either caller: both abandon the bucket for this run either way, so a
    success flag here would be an affordance to branch on a decision that does
    not exist. The outcome is reported through the log entries instead.

    Both call sites reach this from inside the orchestrator's per-bucket ETag
    chain, which is why the write goes through *writer* — see
    :meth:`StateWriter.disable_bucket`.
    """
    bucket_name = ctx.bucket_name
    try:
        writer.disable_bucket(reason, now=datetime.now(tz=UTC))
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"Failed to record the disabled flag in the state object: "
                f"{exc}. The bucket is NOT disabled; the condition is "
                f"re-evaluated on the next run."
            ),
        ))
        return

    observability.emit(observability.log_audit(
        action="bucket_disabled",
        source_bucket=bucket_name,
        details={
            "reason": reason,
            "state_key": state_store_module.state_object_key(bucket_name),
        },
    ))

    if ctx.on_bucket_disabled is not None:
        try:
            ctx.on_bucket_disabled(bucket_name, reason)
        except Exception as cb_exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"Bucket-disabled alert callback failed: {cb_exc}",
            ))


def _apply_recovery_plan(
    ctx: _BucketContext,
    writer: StateWriter,
    state: Any,
    recovery: RecoveryPlan,
    result: _BucketResult,
) -> str | None:
    """Apply a :class:`RecoveryPlan` produced by :func:`plan_recovery`.

    Performs the side effects the pure function cannot: threshold-breach
    disabling (log + state write + notification), the empty-watermark_low
    audit log, and the watermark rollback mutation.

    See ``src.core.job_recovery.plan_recovery`` for the decision logic.

    Returns the new checkpoint_watermark (after any rollback), or None if the
    bucket should be skipped (disabled due to threshold breach).
    """
    bucket_name = ctx.bucket_name

    # Threshold-breach disable.
    if recovery.disable_reason is not None:
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=recovery.disable_reason,
        ))
        result.errored = True
        _disable_bucket(ctx, writer, recovery.disable_reason)
        return None

    # Audit log for skipped empty watermark_lows.
    if recovery.skipped_empty_lows:
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"{recovery.skipped_empty_lows} failed submission record(s) had no "
                f"watermark_low; skipping their readmission rollback rather "
                f"than resetting the checkpoint to epoch. Objects covered by "
                f"those jobs are re-admitted on the next tagging event or by "
                f"a manual checkpoint reset."
            ),
        ))

    # Apply the watermark rollback when a usable target exists.
    if recovery.rollback_to is not None:
        state.last_processed_watermark = recovery.rollback_to
        state.processed_window = [
            r for r in state.processed_window if r.watermark <= recovery.rollback_to
        ]
        return recovery.rollback_to

    return state.last_processed_watermark


# ---------------------------------------------------------------------------
# Journal-read and eligibility phases — extracted from Steps d, d0, e
# ---------------------------------------------------------------------------


def _count_lookback_tail(
    ctx: _BucketContext,
    checkpoint_watermark: str,
    window_start: str,
    journal_read_row_cap: int,
) -> tuple[int, bool]:
    """Return ``(tail_rows, assumed)`` for the lookback tail.

    ``assumed`` is ``True`` when the count is a fallback rather than a measured
    figure, which obliges the caller to bound the tail anyway. See below.

    Two cases skip the query rather than issuing it and discarding the answer
    (Requirement 5.3):

    * The watermark is the epoch, which is every bucket's first run. There is
      nothing below it, so the tail is empty.
    * ``JournalLookbackSeconds`` is zero, so the tail range is empty by
      definition and ``window_start`` equals the watermark.

    On an Athena failure the tail is assumed to be at its allowance, which
    reserves the new-row budget so the run still progresses. That much is
    conservative on the progress axis. It is *not* conservative on the memory
    axis, which is why ``assumed`` is returned alongside it: an assumed count
    equal to the allowance would make the caller's ``tail_rows > tail_allowance``
    test false, so no floor would be raised and the read would span the tail's
    real size — the very quantity this failed query was asked for, and therefore
    unbounded — on top of the reserved new-row budget. The caller raises the
    floor regardless when ``assumed`` is set, so the read stays inside
    ``Journal_Read_Row_Cap`` at the cost of shortening a tail that might have
    fitted. Truncating a tail unnecessarily for one run is recoverable; reading
    an unbounded number of rows into memory is what the cap exists to prevent.
    """
    if not checkpoint_watermark or window_start == checkpoint_watermark:
        return 0, False

    try:
        return athena_journal_adapter.find_tail_row_count(
            athena_client=ctx.athena_client,
            bucket_name=ctx.bucket_name,
            window_start=window_start,
            watermark=checkpoint_watermark,
            athena_workgroup=ctx.athena_workgroup,
            output_location=ctx.athena_output_location,
        ), False
    except Exception as exc:  # noqa: BLE001 — best-effort, see docstring
        assumed = floor(journal_read_row_cap * TAIL_ROW_BUDGET_FRACTION)
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=ctx.bucket_name,
            cause=(
                f"Lookback-tail row count failed: {exc}. Falling back to "
                f"assuming the tail is at its allowance of {assumed} rows, which "
                f"reserves the new-row budget so this run still makes progress. "
                f"The tail is bounded to that allowance rather than read in full, "
                f"because its real size is exactly what this query failed to "
                f"establish."
            ),
        ))
        return assumed, True


def _raise_tail_floor(
    ctx: _BucketContext,
    checkpoint_watermark: str,
    window_start: str,
    tail_rows: int,
    tail_allowance: int,
    result: _BucketResult,
) -> str | None:
    """Return the read's lower bound when the tail exceeds its allowance.

    Truncates the tail from below, so the rows dropped are the oldest in the
    lookback window and the rows nearest the watermark — the likeliest genuine
    late arrivals — are kept.

    Sets ``result.tail_shortened`` and emits an error entry when the bound
    actually moves. An error rather than an audit entry, on the same reasoning
    the record-eviction path in ``state_store`` uses: an audit entry records a
    decision the Solution is entitled to make, and reducing late-arrival
    tolerance because a backlog forced it is a loss, not a policy.

    Returns the lower bound to use, or ``None`` when the run cannot establish a
    safe one and the caller must skip the bucket for this interval.

    ``None`` is returned on an Athena failure looking up the floor. Two directions
    were tried before this one and both were wrong:

    * Keep the nominal ``window_start``. This reads the tail's true size, which is
      unbounded at this point by construction — the caller only reaches here
      because the tail is known or assumed to exceed its allowance. It defeats the
      cap the ceilings in ``row_cap_validation`` are enforced against, and the
      out-of-memory failure it produces is self-sustaining: the run dies before
      the checkpoint advances, so the next run reads the same window.
    * Bound at the watermark, dropping the tail. Memory-safe, but it skips the
      **whole** lookback window rather than the surplus, so every genuine late
      arrival in it misses its only chance. ``is_eligible`` permanently rejects an
      operation at or below ``watermark - lookback``, and this run advances the
      watermark, so a record in the oldest part of the window — where truncation
      already concentrates — is gone for good. That is silent data loss traded for
      liveness, and one Athena error is not worth it.

    Skipping the bucket has neither cost. Nothing is read, so memory is bounded.
    The checkpoint does not advance, so nothing ages out of the lookback window
    and the next run re-scans it whole with a correct floor. The backlog is
    untouched and picked up entire on the next interval, so the only price is one
    interval's drain on a run that had already hit an Athena failure.

    It is also the posture this module already takes for a journal read it cannot
    trust: a fatal ``JournalReadError`` returns ``None`` from
    :func:`_read_journal_window` and leaves the checkpoint unchanged so the records
    are retried. A floor lookup that fails is the same class of event — the read
    window cannot be established — so it gets the same answer rather than a third
    posture invented for it.

    A persistently failing floor query therefore stalls the bucket. That is
    intended: it is an infrastructure fault, it raises ``BucketErrors`` every run,
    and papering over it by discarding late arrivals would hide it.
    """
    bucket_name = ctx.bucket_name

    if tail_allowance <= 0:
        # Unreachable: it needs a row cap whose tail share floors to zero, which
        # is only a cap of 1, and both the template's MinValue and the runtime
        # coercion enforce MIN_JOURNAL_READ_ROW_CAP. Kept because the alternative
        # ways out of this state all lose data or read unbounded — bounding at the
        # watermark drops the whole re-scan window, which is the very thing the
        # floor-failure path above refuses to do — so if a future change makes it
        # reachable, it must fail loudly rather than quietly pick one of them.
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"Lookback tail has no allowance: tail_allowance={tail_allowance} "
                f"for a tail of {tail_rows} rows. No read window for this bucket "
                f"is both bounded and lossless, so it is skipped this interval "
                f"with its checkpoint unchanged. This is a regression: "
                f"Journal_Read_Row_Cap is floored at "
                f"{MIN_JOURNAL_READ_ROW_CAP} precisely so this cannot happen."
            ),
        ))
        return None
    else:
        try:
            floor_bound = athena_journal_adapter.find_tail_floor(
                athena_client=ctx.athena_client,
                bucket_name=bucket_name,
                window_start=window_start,
                watermark=checkpoint_watermark,
                tail_allowance=tail_allowance,
                athena_workgroup=ctx.athena_workgroup,
                output_location=ctx.athena_output_location,
            )
        except Exception as exc:  # noqa: BLE001 — see docstring
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=(
                    f"Lookback-tail floor lookup failed: {exc}. Skipping this "
                    f"bucket for this interval. The lookback tail holds "
                    f"{tail_rows} rows against an allowance of {tail_allowance}, "
                    f"so reading it without a floor would be unbounded, and "
                    f"dropping it would skip the whole lookback window and lose "
                    f"any genuine late arrival in it. The checkpoint is left "
                    f"unchanged, so nothing ages out and the next interval "
                    f"re-scans the window whole."
                ),
            ))
            return None

    if floor_bound is None:
        # The tail turned out smaller than its allowance after all, so nothing
        # needs truncating.
        return window_start

    result.tail_shortened = True
    observability.emit(observability.log_error(
        component=_COMPONENT,
        bucket=bucket_name,
        cause=(
            f"Lookback tail shortened to fit the row budget: tail_rows="
            f"{tail_rows}, tail_allowance={tail_allowance}, "
            f"effective_since={floor_bound!r}, "
            f"journal_lookback_seconds={int(ctx.lookback.total_seconds())}. "
            f"Journal rows between the configured lookback window start and "
            f"the effective lower bound are not re-scanned this run, reducing "
            f"late-arrival tolerance for as long as the backlog lasts."
        ),
    ))
    return floor_bound


def _read_journal_window(
    ctx: _BucketContext,
    checkpoint_watermark: str,
    result: _BucketResult,
    writer: StateWriter,
) -> tuple[list, str | None, str | None] | None:
    """Steps d and d0: split the row budget and read the journal window.

    Sizes the lookback tail, divides ``Journal_Read_Row_Cap`` between it and the
    rows above the watermark, finds the row-cap boundary over the new rows,
    raises the read's lower bound when the tail will not fit its
    allowance, emits the journal_read_capped audit when capped, calls
    read_journal, reports journal errors, and returns early on fatal errors.

    Returns (ops, since_timestamp, journal_until) on success, or None when a
    fatal journal error, a failed boundary lookup, or a failed tail-floor lookup
    means this bucket should be skipped for the interval. The returned
    ``since_timestamp`` is the lower bound actually used, which is what the
    preflight count and the delete scan need so they cover the same range this
    read did. Sets result.capped and result.tail_shortened as side-effects.

    *writer* is used only for the journal-unavailable streak counter, which
    keeps the operator alert for an absent journal to one notification per
    incident rather than one per interval.
    """
    bucket_name = ctx.bucket_name
    athena_client = ctx.athena_client
    lookback = ctx.lookback
    journal_read_row_cap = ctx.journal_read_row_cap
    athena_workgroup = ctx.athena_workgroup
    athena_output_location = ctx.athena_output_location

    window_start = watermark_subtract(checkpoint_watermark, lookback)

    # Row-budget split (Step d0). The read spans the lookback tail,
    # (window_start, checkpoint_watermark], and the rows above the watermark,
    # which are the only rows that can advance the checkpoint. The cap governs
    # both together, so the tail's true row count has to be known before the
    # boundary over the new rows can be found.
    tail_rows, tail_rows_assumed = _count_lookback_tail(
        ctx, checkpoint_watermark, window_start, journal_read_row_cap,
    )
    tail_allowance, new_row_budget = split_row_budget(
        journal_read_row_cap, tail_rows,
    )

    # Boundary anchored at the watermark, not at window_start. This is what
    # makes forward progress structural: _build_boundary_query emits
    # `record_timestamp > timestamp '<since>'`, so a boundary derived from the
    # watermark is strictly above it by construction and the read window always
    # contains rows that can advance the checkpoint.
    journal_until: str | None = None
    try:
        journal_until = athena_journal_adapter.find_row_count_boundary(
            athena_client=athena_client,
            bucket_name=bucket_name,
            since_timestamp=checkpoint_watermark if checkpoint_watermark else None,
            row_cap=new_row_budget,
            athena_workgroup=athena_workgroup,
            output_location=athena_output_location,
        )
    except Exception as exc:  # noqa: BLE001 — see _raise_tail_floor's docstring
        # Same posture as a failed tail-floor lookup: the read window cannot be
        # established, so the bucket is skipped for this interval with its
        # checkpoint unchanged. Proceeding with journal_until at None reads every
        # row tagged since the watermark in one invocation, and read_journal
        # emits no LIMIT, so the only bound would be the predicate just lost.
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"Row-count boundary lookup failed: {exc}. Skipping this bucket "
                f"for this interval. Reading without the boundary would be "
                f"unbounded in memory, and the checkpoint is left unchanged, so "
                f"nothing ages out and the next interval reads the window whole."
            ),
        ))
        result.errored = True
        return None

    # Requirement 4 tripwire. Unreachable: a boundary anchored at the watermark
    # is strictly above it, which is the whole point of the anchor above. It
    # exists so a change that reintroduces a non-advancing window fails loudly
    # here instead of silently producing runs that read only already-processed
    # rows and never move the checkpoint.
    if journal_until is not None and journal_until <= checkpoint_watermark:
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=(
                f"Row-cap boundary did not advance: journal_until={journal_until!r} "
                f"is not above checkpoint_watermark={checkpoint_watermark!r} "
                f"(tail_rows={tail_rows}). A run reading this window can only "
                f"re-read already-processed rows, so the bucket is skipped this "
                f"interval rather than making no progress silently. This is a "
                f"regression: the boundary is anchored at the watermark and is "
                f"strictly above it by construction."
            ),
        ))
        result.errored = True
        return None

    # Raise the lower bound when the tail will not fit its allowance, so the
    # whole read stays within the row budget (Requirement 2.4).
    # An assumed count bounds the tail too, even though the assumed value equals
    # the allowance and so does not exceed it on its own: the real tail size is
    # unknown, and leaving the bound at window_start would read all of it.
    since_timestamp: str | None = window_start
    if tail_rows > tail_allowance or tail_rows_assumed:
        since_timestamp = _raise_tail_floor(
            ctx, checkpoint_watermark, window_start, tail_rows, tail_allowance,
            result,
        )
        if since_timestamp is None:
            # The floor could not be established, so there is no lower bound this
            # run can use that is both memory-safe and lossless. Skip the bucket
            # with the checkpoint untouched — see _raise_tail_floor. The error
            # naming the cause has already been emitted.
            result.errored = True
            return None

    if journal_until is not None:
        result.capped = True
        observability.emit(observability.log_audit(
            action="journal_read_capped",
            source_bucket=bucket_name,
            details={
                "row_cap": journal_read_row_cap,
                "until_timestamp": journal_until,
                "since_timestamp": since_timestamp,
                "tail_rows": tail_rows,
                "new_row_budget": new_row_budget,
                "tail_shortened": result.tail_shortened,
            },
        ))

    ops, journal_errors = athena_journal_adapter.read_journal(
        athena_client=athena_client,
        bucket_name=bucket_name,
        athena_workgroup=athena_workgroup,
        output_location=athena_output_location,
        since_timestamp=since_timestamp if since_timestamp else None,
        until_timestamp=journal_until,
    )

    # Report any journal errors (fatal or per-record)
    for jerr in journal_errors:
        entry = observability.log_error(
            component="Journal_Monitor",
            bucket=jerr.bucket,
            cause=jerr.cause,
        )
        observability.emit(entry)

    # If a fatal journal error occurred, leave checkpoint unchanged and skip
    fatal_errors = [e for e in journal_errors if e.is_fatal]
    if fatal_errors:
        result.errored = True

        # An absent journal is an unmet prerequisite rather than a failure
        # retrying resolves, so it is escalated to an operator instead of only
        # being logged. Emitted unconditionally, and separately from the
        # log_error above, so the condition is queryable by event name even
        # when no alert destination is configured.
        unavailable = next(
            (e for e in fatal_errors if e.is_journal_unavailable), None
        )
        if unavailable is not None:
            observability.emit(observability.log_audit(
                action="journal_unavailable",
                source_bucket=bucket_name,
                details={"cause": unavailable.cause},
            ))
            _escalate_journal_unavailable(
                ctx, writer, bucket_name, unavailable.cause,
            )
        return None

    return ops, since_timestamp, journal_until


def _select_eligible(
    ctx: _BucketContext,
    ops: list,
    state: Any,
    result: _BucketResult,
) -> tuple[list, Any, int]:
    """Step e: deduplicate, select eligible operations, compute candidate HWM.

    Reports malformed (skipped) records and assigns the raw_records / ops_read
    counters on result.

    Also applies the Archived_Object_Filter, which must run here rather than
    anywhere earlier: it depends on deduplication having already collapsed a
    lifecycle-transition record onto the tagging record it followed. See
    :mod:`src.core.archived_filter` for why that ordering is the mechanism
    rather than a detail.

    Returns (deduped_ops, candidate_hwm, raw_count).
    """
    bucket_name = ctx.bucket_name
    lookback = ctx.lookback

    raw_count = len(ops)
    (
        deduped_ops,
        skipped_records,
        candidate_hwm,
    ) = journal_dedup.select_eligible_operations(ops, state, lookback)

    # Report skipped (malformed) records
    for skipped in skipped_records:
        entry = observability.log_error(
            component="Journal_Monitor",
            bucket=bucket_name,
            cause=(
                f"Skipped record seq={skipped.sequence_number!r}: {skipped.reason}"
            ),
        )
        observability.emit(entry)

    # Archived_Object_Filter — drop objects S3 will not replicate because they
    # are in GLACIER or DEEP_ARCHIVE (Glacier Instant Retrieval replicates
    # normally and is not excluded). Applied after dedup by construction.
    #
    # candidate_hwm is deliberately left as computed over ALL eligible
    # operations, including the excluded ones, so the watermark advances past
    # an archived object rather than re-reading it every interval forever.
    # The consequence is that excluding an object is final for that tagging
    # event: restoring the object later does not by itself cause a
    # re-attempt, and the object must be tagged again. That matches what
    # already happened when the object was submitted and the task failed, so
    # it is not a change in reachability, only in cost and visibility.
    deduped_ops, archived_ops = filter_archived_operations(deduped_ops)

    if archived_ops:
        by_class = count_by_storage_class(archived_ops)
        observability.emit({
            "event": "archived_objects_excluded",
            "timestamp": observability._now_utc().isoformat(),
            "bucket": bucket_name,
            "excluded_count": len(archived_ops),
            "by_storage_class": by_class,
        })
        # Also an error-level entry, because this is not routine bookkeeping:
        # each excluded object is one an operator tagged expecting it to
        # replicate, and it will not, until it is restored and tagged again.
        observability.emit(observability.log_error(
            component="Journal_Monitor",
            bucket=bucket_name,
            cause=(
                f"Excluded {len(archived_ops)} tagged object(s) from "
                f"replication because S3 does not replicate archived storage "
                f"classes ({by_class}). Each object stays unreplicated until "
                f"it is restored, copied to a non-archived storage class, and "
                f"tagged again. No Batch Operations task was submitted for "
                f"them. See README.md, Objects That Are Not Replicated."
            ),
        ))

    result.raw_records = raw_count
    result.ops_read = len(deduped_ops)
    result.archived_excluded = len(archived_ops)

    return deduped_ops, candidate_hwm, raw_count


# ---------------------------------------------------------------------------
# Lease context manager (design.md D4, Requirements 5.1–5.4, 5.6)
# ---------------------------------------------------------------------------


class _LeaseHolder:
    """Mutable state yielded by ``_lease_scope``.

    ``submitted_refs`` is set by the code inside the ``with`` block only
    when a submission succeeds.  ``release_ok`` is set by ``__exit__``
    when the release completes without error.
    """

    __slots__ = ("submitted_refs", "release_ok")

    def __init__(self) -> None:
        self.submitted_refs = None
        self.release_ok: bool = False


@contextlib.contextmanager
def _lease_scope(
    writer: StateWriter,
    ctx: _BucketContext,
    candidate_hwm,
    lookback,
    result: _BucketResult,
):
    """Acquire the lease on entry, release it unconditionally on exit.

    Yields a ``_LeaseHolder`` whose ``submitted_refs`` attribute is initially
    ``None``.  Code inside the ``with`` block sets it to the candidate refs
    when (and only when) a submission succeeds, so the release call passes the
    correct value to ``StateWriter.release_lease``.

    On a successful release, ``release_ok`` is set to ``True`` so the caller
    can gate the persist and progressed steps on it (preserving the existing
    semantic where those only run when release succeeds).

    If release itself fails, the exception is caught and logged — it never
    replaces the outcome of the path that triggered the exit (Requirement 5.3).
    ``result.errored`` is set in that case, which is why ``result`` is passed
    in: a failed release leaves the lease persisted in the state object, and
    every operation at or below its watermark is then filtered out of the
    healing run and ages out of the lookback window — silent non-replication.
    Without a ``BucketErrors`` datum the alarm is blind to it, and the accepted
    lease-TTL risk (scan-aa27a832-remediation Requirement 11.1) rests on this
    being visible.
    """
    lease = Lease(
        lease_id=str(uuid.uuid4()),
        candidate_max_watermark=candidate_hwm,
        acquired_at=datetime.now(tz=UTC),
        status=LeaseStatus.IN_FLIGHT,
    )
    writer.acquire_lease(lease)
    holder = _LeaseHolder()
    try:
        yield holder
    finally:
        try:
            writer.release_lease(
                submitted_refs=holder.submitted_refs,
                lookback=lookback,
                # The watermark advances over every eligible operation, not
                # only those that reached the manifest (Requirement 2.5, D3):
                # without this a non-matching record newer than the newest
                # submitted one would stall the cursor and be re-read forever.
                candidate_max_watermark=candidate_hwm,
            )
        except Exception as exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=ctx.bucket_name,
                cause=f"Failed to release lease (checkpoint not advanced): {exc}",
            ))
            result.errored = True
        else:
            holder.release_ok = True


# ---------------------------------------------------------------------------
# Manifest-building phase — extracted from Step f
# ---------------------------------------------------------------------------


def _filter_deleted_entries(
    ctx: _BucketContext,
    manifest_result: Any,
    since_timestamp: str | None,
) -> list | None:
    """Read permanent deletes, filter entries, and return kept entries.

    Returns the filtered entry list, or None when all candidates are excluded.
    """
    bucket_name = ctx.bucket_name
    athena_client = ctx.athena_client
    athena_workgroup = ctx.athena_workgroup
    athena_output_location = ctx.athena_output_location

    try:
        perm_deleted = read_permanent_deletes(
            athena_client=athena_client,
            bucket_name=bucket_name,
            since_window_start=since_timestamp if since_timestamp else None,
            athena_workgroup=athena_workgroup,
            output_location=athena_output_location,
        )
    except Exception as exc:  # noqa: BLE001 — non-fatal, use empty set
        logger.warning(
            "%s | %s | read_permanent_deletes failed (using empty set): %s",
            _COMPONENT, bucket_name, exc,
        )
        perm_deleted = set()

    matched_set: set[MatchedObject] = {
        MatchedObject(
            source_bucket=entry.source_bucket,
            object_key=entry.object_key,
            replication_config_id=bucket_name,
            matched_rule_ids=frozenset(),
            version_id=entry.version_id,
        )
        for entry in manifest_result.entries
    }
    kept_set, excluded_count = filter_deleted_versions(matched_set, perm_deleted)

    if excluded_count > 0:
        observability.emit({
            "event": "deleted_versions_excluded",
            "timestamp": observability._now_utc().isoformat(),
            "bucket": bucket_name,
            "excluded_count": excluded_count,
        })

    if not kept_set:
        logger.debug(
            "All candidates excluded by Deleted_Version_Filter for "
            "bucket %r", bucket_name,
        )
        return None

    kept_keys = {(obj.source_bucket, obj.object_key) for obj in kept_set}
    return [
        entry for entry in manifest_result.entries
        if (entry.source_bucket, entry.object_key) in kept_keys
    ]


def _build_manifest(
    ctx: _BucketContext,
    gen: ManifestGenerator,
    rules: list,
    since_timestamp: str | None,
    journal_until: str | None,
    writer: StateWriter,
    tracking: _CompletionTracking,
    ts_label: str,
) -> tuple[WrittenManifest, set[tuple[str, str, str | None]]] | None:
    """Preflight count, manifest generation, deleted-version filter, and write.

    Returns ``(written, kept_triples)`` on success, or None on each of the four
    skip paths (preflight failure, no matches, all-excluded, manifest write
    failure).  ``kept_triples`` holds the
    ``(source_bucket, object_key, version_id)`` triple of every entry that
    survived the Deleted_Version_Filter and reached the written manifest; it is
    the input to ``build_submitted_refs`` (Requirement 2.2).
    """
    bucket_name = ctx.bucket_name
    s3_client = ctx.s3_client
    athena_client = ctx.athena_client
    state_bucket = ctx.state_bucket
    athena_workgroup = ctx.athena_workgroup
    athena_output_location = ctx.athena_output_location
    kms_key_arn = ctx.kms_key_arn

    try:
        pf_count = preflight_count(
            athena_client=athena_client,
            bucket_name=bucket_name,
            rules=rules,
            since_timestamp=since_timestamp if since_timestamp else None,
            athena_workgroup=athena_workgroup,
            output_location=athena_output_location,
            until_timestamp=journal_until,
        )
    except Exception as exc:  # noqa: BLE001
        entry = observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=f"Preflight_Count failed: {exc}",
        )
        observability.emit(entry)
        return None

    tracking.record_scan(writer, bucket_name, pf_count)

    observability.emit({
        "event": "manifest_strategy_selected",
        "timestamp": observability._now_utc().isoformat(),
        "bucket": bucket_name,
        "preflight_count": pf_count,
        "manifest_format": ManifestFormat.INVENTORY_REPORT.value,
        "generation_mode": "In_Memory_Generation",
    })

    manifest_result = gen.finalize(bucket_name)

    if not manifest_result.has_matches:
        logger.debug(
            "No matches for bucket %r — skipping job creation", bucket_name,
        )
        return None

    kept_entries = _filter_deleted_entries(ctx, manifest_result, since_timestamp)
    if kept_entries is None:
        return None

    csv_bytes = serialize(kept_entries).encode("utf-8")
    data_file_key = f"manifests/{bucket_name}/{ts_label}/data/data.csv"
    try:
        written = write_in_memory_inventory_manifest(
            s3_client=s3_client,
            scratch_bucket=state_bucket,
            config_id=bucket_name,
            source_bucket=bucket_name,
            csv_bytes=csv_bytes,
            data_file_key=data_file_key,
            kms_key_arn=kms_key_arn or None,
        )
        written.object_count = len(kept_entries)  # type: ignore[misc]
    except InventoryManifestWriteError as exc:
        entry = observability.log_error(
            component="Manifest_Generator",
            bucket=bucket_name,
            cause=str(exc),
        )
        observability.emit(entry)
        return None

    kept_triples = {
        (entry.source_bucket, entry.object_key, entry.version_id)
        for entry in kept_entries
    }
    return written, kept_triples


# ---------------------------------------------------------------------------
# Submission and persistence phases — extracted from Step f
# ---------------------------------------------------------------------------


@dataclass
class _SubmissionOutcome:
    """Result of _submit_job: whether a submission succeeded, the record to
    persist, and whether the bucket was disabled."""

    succeeded: bool
    last_submission: SubmissionRecord | None
    bucket_disabled: bool


def _escalate_journal_unavailable(
    ctx: _BucketContext,
    writer: StateWriter,
    bucket_name: str,
    cause: str,
) -> None:
    """Escalate an absent S3 Metadata journal to an operator, rate-limited.

    A missing journal fails identically on every subsequent interval, so
    without rate limiting the alert would repeat for as long as the
    prerequisite went unmet: every 15 minutes by default, indefinitely.
    :meth:`StateWriter.journal_unavailable_alert_due` bounds that to one
    notification per :data:`JOURNAL_UNAVAILABLE_REALERT_INTERVAL`, which both
    keeps an unmet prerequisite visible and stops it flooding an inbox.

    The interval is recorded only after delivery has been attempted and did not
    raise, matching the report-missing path in
    :mod:`src.lambda_handler`. Recording it first would spend the interval on an
    alert a failed SNS or CloudWatch Logs call never delivered, leaving a
    stalled bucket unannounced for a whole interval while the condition
    persisted every run. The write is safe in that position because this
    function is reached only from :func:`_read_journal_window` on a path that
    abandons the bucket, so no later write depends on the ETag it advances.

    Unlike :func:`_escalate_submission_failure`, this never disables the
    bucket. A submission failure of that class is a defect in the Solution that
    needs a new deployment, so continuing to retry is pointless; a missing
    journal is fixable in the operator's own account, and the bucket starts
    working on the next interval once it is. Disabling would add a manual
    config edit to a recovery path that otherwise needs none.
    """
    if ctx.on_journal_unavailable is None:
        return

    now = datetime.now(tz=UTC)

    try:
        should_alert = writer.journal_unavailable_alert_due(
            bucket_name, now=now,
        )
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=f"Failed to read journal-unavailable alert record: {exc}",
        ))
        # A duplicate notification is a smaller failure than a lost one.
        should_alert = True

    if not should_alert:
        return

    try:
        ctx.on_journal_unavailable(bucket_name, cause)
    except Exception as cb_exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=f"Journal-unavailable alert callback failed: {cb_exc}",
        ))
        # Suppression stays unset, so the next interval alerts again rather
        # than going quiet for an interval over an alert nobody received.
        return

    try:
        writer.record_journal_unavailable_alert(bucket_name, now=now)
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=f"Failed to record journal-unavailable alert: {exc}",
        ))


def _escalate_submission_failure(
    ctx: _BucketContext,
    writer: StateWriter,
    bucket_name: str,
    error_reason: str,
    result: _BucketResult,  # noqa: ARG001 — passed for call-site symmetry with other escalation helpers
) -> bool:
    """Handle PERMANENT_CLIENT submission-failure streak escalation.

    Increments the failure streak, fires the first-occurrence alert, and
    disables the bucket at the configured threshold.

    The increment stays *before* the alert here, unlike
    :func:`_escalate_journal_unavailable`, because the streak is not a
    suppression token: it is also the counter the circuit breaker disables on.
    Withholding it when a publish fails would keep re-alerting but would also
    stop the streak ever reaching ``max_batch_job_failures``, so a permanent
    client defect would retry forever and never disable the bucket. A failed
    first-occurrence publish therefore costs the early warning only; the
    threshold alert still fires, on a streak this function keeps advancing.

    Returns True if the bucket was disabled at threshold.
    """
    try:
        streak = writer.increment_submission_failure_streak(bucket_name)
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=f"Failed to increment submission failure streak: {exc}",
        ))
        streak = 1  # assume first occurrence for alert purposes

    # Alert on the first occurrence only (streak == 1).
    if streak == 1 and ctx.on_submission_failure is not None:
        try:
            ctx.on_submission_failure(bucket_name, error_reason)
        except Exception as cb_exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"Submission failure alert callback failed: {cb_exc}",
            ))

    # Disable at threshold (Requirement 4.1, 4.2, 4.5).
    if streak < ctx.max_batch_job_failures:
        return False

    disable_reason_submission = (
        f"S3 Batch Operations job submission for bucket "
        f"{bucket_name!r} was rejected by the AWS API before it "
        f"was sent ({streak} consecutive interval(s)). The cause "
        f"is a request the Solution builds that botocore's own "
        f"parameter validation rejects — this is a code defect, "
        f"not a condition in your account. Re-enabling this bucket "
        f"without a Solution code fix will reproduce the failure."
    )
    observability.emit(observability.log_error(
        component=_COMPONENT,
        bucket=bucket_name,
        cause=disable_reason_submission,
    ))
    _disable_bucket(ctx, writer, disable_reason_submission)
    return True


def _submit_job(
    ctx: _BucketContext,
    writer: StateWriter,
    result: _BucketResult,
    written: Any,
    bucket_consecutive_failures: int,
    checkpoint_watermark: str,
    candidate_hwm: str | None,
) -> _SubmissionOutcome:
    """Submit one Batch_Replication_Job and handle all three outcome branches.

    Returns a _SubmissionOutcome so the caller can decide whether to release
    the lease and persist the submission record.
    """
    bucket_name = ctx.bucket_name
    s3control_client = ctx.s3control_client
    account_id = ctx.account_id
    state_bucket = ctx.state_bucket

    # Derive completion_report_prefix unconditionally — every job writes a
    # completion report regardless of whether completion tracking is configured
    # (Requirement 1.1).
    completion_report_prefix = _completion_report_prefix(
        bucket_name, written.s3_location.key
    )

    submission = batch_operations_adapter.submit_batch_job(
        s3control_client=s3control_client,
        account_id=account_id,
        manifest_location=written.s3_location,
        manifest_etag=written.etag,
        batch_operations_role_arn=ctx.batch_operations_role_arn,
        config_id=bucket_name,
        object_count=written.object_count,
        source_bucket=bucket_name,
        has_version_ids=written.all_versioned,
        manifest_format=ManifestFormat.INVENTORY_REPORT.value,
        completion_report_prefix=completion_report_prefix,
        state_bucket=state_bucket,
    )

    if submission.was_submitted:
        sub_entry = observability.log_submission(
            job_id=submission.job_id,
            source_bucket=bucket_name,
        )
        observability.emit(sub_entry)
        result.submitted += 1
        last_submission = SubmissionRecord(
            replication_config_id=bucket_name,
            source_bucket=bucket_name,
            job_id=submission.job_id,
            manifest_key=written.s3_location.key,
            submitted_at=datetime.now(tz=UTC),
            status=SubmissionStatus.SUBMITTED,
            watermark_low=checkpoint_watermark,
            watermark_high=candidate_hwm or "",
            consecutive_failures=bucket_consecutive_failures,
        )
        # A successful submission clears any submission-failure streak so
        # that a recurrence after a fix is reported again (Requirement 3.4).
        try:
            writer.clear_submission_failure_streak(bucket_name)
        except Exception as exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"Failed to clear submission failure streak (non-fatal): {exc}",
            ))
        return _SubmissionOutcome(
            succeeded=True,
            last_submission=last_submission,
            bucket_disabled=False,
        )

    elif submission.failed:
        failure_class_label = (
            submission.failure_class.value if submission.failure_class else "UNKNOWN"
        )
        entry = observability.log_error(
            component="Batch_Job_Manager",
            bucket=bucket_name,
            cause=(
                f"Job submission failed "
                f"({submission.status.value}, class={failure_class_label}): "
                f"{submission.error_reason}"
            ),
        )
        observability.emit(entry)
        result.errored = True

        # Submission-failure streak and escalation.
        # Only PERMANENT_CLIENT failures increment the streak and fire the
        # alert — a service-side error must not disable a bucket (Req 4.4).
        bucket_disabled = (
            submission.failure_class is FailureClass.PERMANENT_CLIENT
            and _escalate_submission_failure(
                ctx, writer, bucket_name, submission.error_reason or "", result,
            )
        )
        return _SubmissionOutcome(
            succeeded=False,
            last_submission=None,
            bucket_disabled=bucket_disabled,
        )

    else:
        # Zero-object manifest: no job to submit
        logger.debug(
            "Job skipped for bucket %r (object_count=0)", bucket_name,
        )
        return _SubmissionOutcome(
            succeeded=False,
            last_submission=None,
            bucket_disabled=False,
        )


def _persist_submission(
    ctx: _BucketContext,
    writer: StateWriter,
    last_submission: SubmissionRecord | None,
    result: _BucketResult,
    *,
    terminal_job_ids: Collection[str] = (),
    completion_tracking_enabled: bool = True,
) -> None:
    """Persist the submission record into the per-bucket state object.

    The write adds this job's record without disturbing another job's, prunes
    the records of jobs that have settled, and enforces the record ceiling — all
    in one conditional write. *terminal_job_ids* and
    *completion_tracking_enabled* are what the store needs to tell a settled
    record from a terminal one whose report has not been read; see
    ``StateStore.record_submission``.

    Best-effort: the job is already submitted and the checkpoint already
    advanced, so a persistence failure here is non-fatal — it never replaces the
    outcome of the run. It costs the audit convenience of the in-state record
    (the job id is also captured in the submission and audit log entries) and
    defers pruning to the next successful submission.

    Non-fatal is not invisible, which is why ``result`` is passed in:
    ``result.errored`` is set so the failure publishes a ``BucketErrors`` datum.
    Because the checkpoint advanced before this write was attempted, the lost
    record leaves a billed job that nothing tracks, over a range the watermark
    has already passed, and there is no rollback available. The accepted
    persist-before-advance ordering risk (scan-aa27a832-remediation Requirement
    11.3) rests on that being visible.
    """
    if last_submission is None:
        return
    try:
        writer.record_submission(
            last_submission,
            terminal_job_ids=terminal_job_ids,
            completion_tracking_enabled=completion_tracking_enabled,
            max_concurrent_jobs=ctx.max_concurrent_jobs,
        )
    except Exception as exc:  # noqa: BLE001
        entry = observability.log_error(
            component=_COMPONENT,
            bucket=ctx.bucket_name,
            cause=(
                "Failed to persist submission record "
                f"(job already submitted, checkpoint advanced): {exc}"
            ),
        )
        observability.emit(entry)
        result.errored = True


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_interval(
    config_source: dict,
    runtime_config: dict | None = None,
) -> RunOutcome:
    """Execute one Processing_Interval.

    Loads and validates config, then for each Monitored_Bucket derives rules,
    reads and deduplicates journal operations, matches and accumulates matched
    objects, finalises manifests, submits at most one batch job per
    replication configuration, advances the checkpoint only on successful
    submission, releases the lease, and emits a single summary log entry.

    Parameters
    ----------
    config_source:
        A ``dict`` (already parsed from JSON/YAML) passed to
        ``config_loader.load_config`` for validation.
    runtime_config:
        Optional dict of AWS-specific settings:

        * ``state_bucket`` — S3 bucket for state objects and manifest CSV files.
        * ``athena_workgroup`` — Athena workgroup to run queries in (default: ``"primary"``).
        * ``athena_output_location`` — S3 URI for Athena query results.
        * ``account_id`` — AWS account ID that owns the source buckets and jobs.
        * ``batch_operations_role_arn`` — ARN of the stack-created S3 Batch
          Operations job role, passed as every job's ``RoleArn``.
        * ``region`` — AWS region used when creating boto3 clients.
        * ``kms_key_arn`` — optional KMS key ARN. When provided, state objects
          are encrypted with SSE-KMS using this key. Athena query result
          encryption is governed by the deployed ``AthenaWorkGroup``'s own
          ``EncryptionConfiguration`` (SSE-KMS with this same key when set at
          deploy time, SSE-S3 otherwise) rather than by this runtime value —
          the workgroup enforces its configuration, so a per-query override
          here would be silently ignored (security-scan-remediation
          Decisions 1, 2). The S3 Batch Operations manifest is always written
          with SSE-S3 because Batch Operations cannot read a KMS-encrypted
          CSV manifest.
        * ``journal_lookback_seconds`` — optional non-negative number of seconds
          to re-scan below the ``record_timestamp`` watermark each run, to catch
          journal records delivered late (eventual consistency).  Defaults to
          one hour.  Re-scanned records already submitted are suppressed by the
          processed-operation window, so a larger lookback never causes
          redundant replication — only more rows scanned.

    Returns
    -------
    RunOutcome
        Aggregated per-bucket metrics plus ``any_capped_and_progressed`` — the
        signal the Self_Reinvocation decision (``should_reinvoke``) needs
        (Requirements 4.1, 4.2, 4.3).

    Raises
    ------
    config_loader.ConfigError
        When the configuration is invalid or missing required fields.  This is
        a fatal error and no S3 resources are modified before the exception
        propagates.
    """
    if runtime_config is None:
        runtime_config = {}

    # -----------------------------------------------------------------------
    # 1. Load and validate configuration — fatal on error (Req 13.6)
    # -----------------------------------------------------------------------
    try:
        app_config = config_loader.load_config(config_source)
    except config_loader.ConfigError as exc:
        entry = observability.log_error(_COMPONENT, "", str(exc))
        observability.emit(entry)
        raise

    # -----------------------------------------------------------------------
    # 2. Extract runtime AWS settings
    # -----------------------------------------------------------------------
    state_bucket: str = runtime_config.get("state_bucket", "")
    athena_workgroup: str = runtime_config.get("athena_workgroup", "primary")
    athena_output_location: str = runtime_config.get("athena_output_location", "")
    account_id: str = runtime_config.get("account_id", "")
    # The stack-created Batch Operations job role, from the
    # BATCH_OPERATIONS_ROLE_ARN environment variable. Deployment-derived, in
    # the same class as state_bucket and account_id.
    batch_operations_role_arn: str = runtime_config.get(
        "batch_operations_role_arn", ""
    )
    kms_key_arn: str = runtime_config.get("kms_key_arn", "") or ""

    # Validate kms_key_arn format when provided (fail fast rather than wait
    # for a cryptic InvalidKeyId or AccessDenied during mid-run put_object).
    if kms_key_arn:
        _validate_kms_key_arn(kms_key_arn)

    # Completion-tracking feature gate (Requirement 4.8): the SNS topic ARN
    # a Completion_Report would be published to. Absent/empty disables
    # report-derived per-object tracking and email for this deployment,
    # mirroring the existing MetricsNamespace no-op-when-unset pattern. The
    # BOPS completion report itself is always requested and diagnosed
    # regardless of this setting. The stack wires this to a provisioned SNS
    # topic ARN through the COMPLETION_REPORT_TOPIC_ARN environment variable.
    completion_report_topic_arn: str = (
        runtime_config.get("completion_report_topic_arn", "") or ""
    ).strip()

    lookback = _resolve_lookback(runtime_config)

    # Notification callback invoked after a bucket has been disabled and the
    # flag persisted to its state object. The lambda handler wires this to the
    # recovery-instructions alert; library callers may omit it.
    on_bucket_disabled = runtime_config.get("on_bucket_disabled")

    # Callback invoked on the first occurrence of a permanent client-side
    # submission failure. The lambda handler wires this to publish to
    # BatchJobFailureLogGroup / BatchJobFailureTopic, mirroring the
    # bucket-disabled alert shape (submission-failure-visibility Req 3.1).
    on_submission_failure = runtime_config.get("on_submission_failure")

    # Callback invoked on the first interval in which a bucket's S3 Metadata
    # journal is found to be absent. The lambda handler wires this to the same
    # BatchJobFailureLogGroup / BatchJobFailureTopic destinations as the two
    # callbacks above; library callers may omit it.
    on_journal_unavailable = runtime_config.get("on_journal_unavailable")

    # Maximum consecutive S3 Batch Operations job failures before the bucket
    # is disabled to prevent runaway per-job costs on a low-churn deployment.
    # Default 4 matches the CloudFormation parameter default.
    max_batch_job_failures: int = max(1, int(runtime_config.get("max_batch_job_failures", 4)))

    # Ceiling on Batch Operations jobs outstanding at once per bucket. Clamped at
    # 1 rather than 0: 0 would defer every bucket forever, which is a way to stop
    # the Solution entirely and not something a tuning knob should be able to do
    # by accident. The CloudFormation parameter enforces the same floor.
    max_concurrent_jobs: int = max(1, int(
        runtime_config.get("max_concurrent_jobs_per_bucket", MAX_CONCURRENT_JOBS_DEFAULT)
    ))

    # -----------------------------------------------------------------------
    # Journal_Read_Row_Cap — the single scale knob (Requirement 2.1)
    # -----------------------------------------------------------------------
    # Floored at MIN_JOURNAL_READ_ROW_CAP rather than 1: a cap of 1 leaves the
    # lookback tail an allowance of floor(1 * 0.8) = 0, which is the one budget
    # split with no safe read window — see _raise_tail_floor. The template
    # enforces the same minimum, so this only catches a hand-edited config object.
    journal_read_row_cap: int = max(MIN_JOURNAL_READ_ROW_CAP, int(
        runtime_config.get(
            "journal_read_row_cap", JOURNAL_READ_ROW_CAP_DEFAULT
        )
    ))

    # -----------------------------------------------------------------------
    # 3. Startup no-destination-client guard — fatal before any S3 access
    #    (Req 12.2, 13.1)
    # -----------------------------------------------------------------------
    factory = ClientFactory()
    # Raises DestinationClientError if the factory has been modified to accept
    # destination-side parameters — fail fast before any S3 resource is modified.
    factory.check_no_destination_client()

    store = state_store_module.StateStore(kms_key_arn=kms_key_arn or None)

    # -----------------------------------------------------------------------
    # 4. Per-bucket loop: derive rules → journal → match → accumulate → submit
    # -----------------------------------------------------------------------
    total_ops_read = 0
    total_raw_records = 0
    total_matched = 0
    total_submitted = 0
    bucket_results: list[BucketMetrics] = []  # per-bucket metrics for CloudWatch
    # What the publish phase needs from this run, per bucket. A bucket skipped as
    # disabled gets no entry, so the publish phase falls back to zero and False
    # for it rather than reporting a stale count.
    bucket_run_state: dict[str, _BucketRunState] = {}
    disabled_buckets = 0  # run-level count for CloudWatch (auto-disable visibility)
    any_capped_and_progressed = False  # RunOutcome signal for Self_Reinvocation

    for bucket in app_config.buckets:
        # The disable flag lives in the bucket's state object, so reading it
        # needs a client. Checking here rather than inside _process_bucket
        # keeps the guarantee that a disabled bucket costs nothing: no
        # replication configuration is read, no journal is queried, and it
        # contributes no BucketMetrics, which is what makes "no BucketErrors
        # datum for a bucket" a meaningful alarm condition.
        skip = _check_bucket_disabled(bucket, factory, store, state_bucket)
        if skip is not None:
            if skip.counts_as_disabled:
                disabled_buckets += 1
            if skip.metrics is not None:
                bucket_results.append(skip.metrics)
            continue

        # _process_bucket documents that any skip or error is logged and the
        # function returns with partial counters, without raising. This handler
        # enforces that contract at the boundary instead of trusting it: without
        # it, one unexpected exception — a malformed watermark_low escaping
        # deserialize_submission_record is the known trigger — aborts every
        # remaining bucket in the run, so a fault in one bucket's state object
        # silently stops replication for all the others.
        try:
            result = _process_bucket(
                bucket=bucket,
                store=store,
                factory=factory,
                state_bucket=state_bucket,
                athena_workgroup=athena_workgroup,
                athena_output_location=athena_output_location,
                account_id=account_id,
                batch_operations_role_arn=batch_operations_role_arn,
                kms_key_arn=kms_key_arn,
                lookback=lookback,
                on_bucket_disabled=on_bucket_disabled,
                on_submission_failure=on_submission_failure,
                on_journal_unavailable=on_journal_unavailable,
                max_batch_job_failures=max_batch_job_failures,
                completion_report_topic_arn=completion_report_topic_arn,
                journal_read_row_cap=journal_read_row_cap,
                max_concurrent_jobs=max_concurrent_jobs,
            )
        except Exception as exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket.name,
                cause=(
                    f"Processing raised unexpectedly: {exc}. Skipping the "
                    f"bucket this run; the remaining buckets are unaffected."
                ),
            ))
            # An errored BucketMetrics entry, matching the shape
            # _check_bucket_disabled uses for its read-failure path, so
            # BucketErrors is not silently zero for a run that lost a bucket.
            bucket_results.append(
                BucketMetrics(
                    source_bucket=bucket.name,
                    ops_read=0,
                    matched=0,
                    submitted=0,
                    errored=True,
                )
            )
            # outstanding_jobs stays None — unknown, not zero. Processing blew up
            # at an unknown point, so whether the DescribeJob loop ran is not
            # known, and a report claiming nothing remains in tracking would be
            # a false all-clear.
            bucket_run_state[bucket.name] = _BucketRunState()
            continue

        bucket_run_state[bucket.name] = _BucketRunState(
            outstanding_jobs=result.outstanding_jobs,
            submission_deferred=result.submission_deferred,
        )
        total_ops_read += result.ops_read
        total_raw_records += result.raw_records
        total_matched += result.matched
        total_submitted += result.submitted
        bucket_results.append(
            BucketMetrics(
                source_bucket=bucket.name,
                ops_read=result.ops_read,
                matched=result.matched,
                submitted=result.submitted,
                errored=result.errored,
                archived_excluded=result.archived_excluded,
                submission_deferred=result.submission_deferred,
                tail_shortened=result.tail_shortened,
            )
        )
        if result.capped and result.progressed:
            any_capped_and_progressed = True

    # -----------------------------------------------------------------------
    # 5. Emit single summary log entry (Req 11.1, 11.2, 11.5)
    # -----------------------------------------------------------------------
    duplicate_records_discarded = max(0, total_raw_records - total_ops_read)
    summary = observability.log_summary(
        ops_read=total_ops_read,
        matched_objects=total_matched,
        jobs_submitted=total_submitted,
        duplicate_records_discarded=duplicate_records_discarded,
    )
    observability.emit(summary)

    # -----------------------------------------------------------------------
    # 6. Publish CloudWatch metrics (optional; no-op when namespace absent)
    #    Runs after all checkpoint-advancing work and the summary log so that
    #    a CloudWatch failure cannot affect the run outcome (Req 5.1, 5.3).
    # -----------------------------------------------------------------------
    # duplicate_records_discarded is deliberately not carried here: it stays a
    # log-only field. As a metric it conflated malformed records, genuine
    # at-least-once duplicates, and records excluded as already processed
    # (including every JournalLookbackSeconds re-read), so no threshold on it
    # implied an action.
    run_result = RunResult(
        buckets=bucket_results,
        disabled_buckets=disabled_buckets,
    )
    publisher = MetricsPublisher(
        namespace=runtime_config.get("metrics_namespace"),
        dimensions=runtime_config.get("metrics_dimensions"),
    )
    try:
        publisher.publish(run_result)
    except Exception as exc:  # noqa: BLE001
        entry = observability.log_error(
            component="Metrics_Publisher",
            bucket="",
            cause=f"Failed to publish metrics: {exc}",
        )
        observability.emit(entry)

    # -----------------------------------------------------------------------
    # 7. Isolated completion-tracking interval (design.md Decision 9.2) — runs
    #    strictly after every bucket's checkpoint/lease work and the metrics
    #    publish above, in its own try/except, so a completion-tracking
    #    failure can never affect checkpoint advancement, lease release, or
    #    Batch_Replication_Job submission for this run (Requirement 6.2, 6.3).
    #    Entirely a no-op when completion_report_topic_arn is unset
    #    (Requirement 4.8) — mirrors the existing MetricsNamespace
    #    no-op-when-unset pattern used above; guarded here at the call site
    #    (in addition to the function's own internal gating) so that when the
    #    feature is disabled, no destination-presence client is constructed
    #    and no per-bucket state read is attempted at all.
    # -----------------------------------------------------------------------
    if completion_report_topic_arn:
        try:
            _run_completion_tracking_interval(
                buckets=app_config.buckets,
                factory=factory,
                store=store,
                state_bucket=state_bucket,
                completion_report_topic_arn=completion_report_topic_arn,
                run_state=bucket_run_state,
            )
        except Exception as exc:  # noqa: BLE001
            entry = observability.log_error(
                component="Completion_Tracker",
                bucket="",
                cause=f"Completion-tracking interval failed: {exc}",
            )
            observability.emit(entry)

    return RunOutcome(
        any_capped_and_progressed=any_capped_and_progressed,
        buckets=bucket_results,
    )


def _resolve_lookback(runtime_config: dict) -> timedelta:
    """Resolve the journal lookback window from runtime config.

    Reads ``journal_lookback_seconds`` (a non-negative number of seconds) and
    falls back to :data:`DEFAULT_JOURNAL_LOOKBACK` when absent.

    Raises
    ------
    config_loader.ConfigError
        If the value is not a non-negative number, so the run fails fast rather
        than producing a confusing error mid-run.
    """
    raw = runtime_config.get("journal_lookback_seconds")
    if raw is None:
        return DEFAULT_JOURNAL_LOOKBACK
    try:
        seconds = float(raw)
    except (TypeError, ValueError) as exc:
        raise config_loader.ConfigError(
            f"Invalid journal_lookback_seconds {raw!r}: must be a number."
        ) from exc
    if seconds < 0:
        raise config_loader.ConfigError(
            f"Invalid journal_lookback_seconds {raw!r}: must be non-negative."
        )
    return timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Per-bucket processing
# ---------------------------------------------------------------------------


def _match_and_accumulate(
    ctx: _BucketContext,
    deduped_ops: list,
    rules: list,
    result: _BucketResult,
    raw_count: int,
    journal_read_row_cap: int,
) -> ManifestGenerator:
    """Match each deduped operation against rules and accumulate into a generator.

    Also emits the row-cap overshoot audit when applicable.

    Returns the populated ManifestGenerator.
    """
    bucket_name = ctx.bucket_name
    gen = ManifestGenerator()

    for op in deduped_ops:
        matched_set, match_errors = rule_matcher.match(op, rules)

        for merr in match_errors:
            entry = observability.log_error(
                component="Rule_Matcher",
                bucket=merr.source_bucket,
                cause=merr.reason,
            )
            observability.emit(entry)

        gen.accumulate(matched_set)
        result.matched += len(matched_set)

    # Row-cap overshoot audit: find_row_count_boundary reads INCLUSIVELY so
    # every row sharing the boundary timestamp is admitted — a tie at the cap
    # boundary can push actual rows above journal_read_row_cap. This is
    # deliberate (no row silently falls below the advancing watermark), and
    # IN_MEMORY_MEMORY_CEILING reserves headroom, but the overshoot is logged
    # so it is visible rather than assumed (~617 ms/page pagination cost at
    # scale makes this the primary throughput lever).
    if result.capped and raw_count > journal_read_row_cap:
        observability.emit(observability.log_audit(
            action="row_cap_overshoot",
            source_bucket=bucket_name,
            details={
                "row_cap": journal_read_row_cap,
                "rows_read": raw_count,
                "matched": result.matched,
                "overshoot_rows": raw_count - journal_read_row_cap,
            },
        ))

    return gen


def _leased_manifest_and_submit(
    ctx: _BucketContext,
    writer: StateWriter,
    result: _BucketResult,
    deduped_ops: list,
    rules: list,
    raw_count: int,
    journal_read_row_cap: int,
    since_timestamp: str | None,
    journal_until: str | None,
    tracking: _CompletionTracking,
    bucket_consecutive_failures: int,
    checkpoint_watermark: str,
    candidate_hwm: str | None,
    holder: _LeaseHolder,
) -> _SubmissionOutcome | None:
    """Run the match-accumulate, manifest-build, and submit phases under a lease.

    Returns the _SubmissionOutcome on success/failure, or None when the bucket
    should be skipped (idle scan, no matches, all excluded, or disabled).
    """
    bucket_name = ctx.bucket_name
    gen = _match_and_accumulate(
        ctx, deduped_ops, rules, result, raw_count, journal_read_row_cap,
    )

    if not gen.has_accumulated_entries(bucket_name) and candidate_hwm is None:
        # Record idle scan so quiescence can be satisfied on quiet buckets —
        # without it, completion_scan_state freezes at the last active run and
        # no Completion_Report is ever published.
        tracking.record_idle_scan(writer, bucket_name)
        return None

    # Persist per-object timestamps (tagged_at, last_modified) and routing
    # (matched rule IDs, destination buckets) so the completion report email
    # can include them even though TrackedObjects are created on a later
    # invocation (when the BOPS report arrives).
    obj_timestamps = gen.get_timestamps(bucket_name)
    obj_routing = gen.get_routing(bucket_name)
    if obj_timestamps or obj_routing:
        try:
            writer.store_completion_timestamps(obj_timestamps, routing=obj_routing)
        except Exception as exc:  # noqa: BLE001
            # Non-fatal: both are nice-to-have for the email; do not block job
            # submission if the write fails.
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"Failed to store completion report metadata: {exc}",
            ))

    ts_label = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")

    manifest_out = _build_manifest(
        ctx=ctx,
        gen=gen,
        rules=rules,
        since_timestamp=since_timestamp,
        journal_until=journal_until,
        writer=writer,
        tracking=tracking,
        ts_label=ts_label,
    )
    if manifest_out is None:
        return None
    written, kept_triples = manifest_out

    sub_outcome = _submit_job(
        ctx=ctx,
        writer=writer,
        result=result,
        written=written,
        bucket_consecutive_failures=bucket_consecutive_failures,
        checkpoint_watermark=checkpoint_watermark,
        candidate_hwm=candidate_hwm,
    )
    if sub_outcome.bucket_disabled:
        return None

    if sub_outcome.succeeded:
        # Only operations whose object actually reached the written manifest are
        # recorded as processed (Requirement 2.1); the watermark still advances
        # over all eligible operations via candidate_hwm (Requirement 2.5).
        #
        # Invariant: this list is never empty on the success path, and
        # advance_checkpoint depends on that. It reads an empty submitted_refs
        # as "nothing was submitted" and leaves the watermark unchanged, which
        # here would mean a job that was created and billed followed by a
        # cursor that never advances past it — the same window re-submitted
        # every interval. The invariant holds because kept_triples is derived
        # from the manifest entries, which are derived from these same ops via
        # rule_matcher and ManifestGenerator, so every kept triple has at least
        # one op that produced it; and a job is only submitted for a non-empty
        # manifest. It is stated here because it is a coupling between three
        # functions rather than a local property: narrowing what reaches the
        # manifest without narrowing candidate_hwm alongside it would break it.
        holder.submitted_refs = journal_dedup.build_submitted_refs(
            deduped_ops, kept_triples,
        )

    return sub_outcome


def _prepare_state_and_recovery(
    ctx: _BucketContext,
    store: state_store_module.StateStore,
    s3_client,
    state_bucket: str,
    bucket_name: str,
    *,
    writer: None,  # noqa: ARG001 — keyword sentinel; writer is created inside from checkpoint state
    tracking_arn: str,
    account_id: str,
    result: _BucketResult,
) -> tuple[
    StateWriter, str, _CompletionTracking, int, Any, _JobCheckResult
] | None:
    """Read checkpoint, set up writer/tracking, run job-check and recovery.

    Returns (writer, checkpoint_watermark, tracking, consecutive_failures, state,
    job_check) or None if the checkpoint read fails. The caller reads
    ``job_check.outstanding`` to decide whether to defer submission, and
    ``job_check.terminal_job_ids`` to tell the state store which records have
    settled.
    """
    try:
        state, _initial_etag = store.get_checkpoint(
            s3_client, state_bucket, bucket_name
        )
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT, bucket=bucket_name,
            cause=f"Failed to read checkpoint: {exc}",
        ))
        result.errored = True
        return None

    state_writer = StateWriter(store, s3_client, state_bucket, bucket_name, _initial_etag)
    checkpoint_watermark = state.last_processed_watermark

    tracking: _CompletionTracking = (
        _CompletionHooks(state_bucket, account_id)
        if tracking_arn
        else _NullCompletionHooks()
    )

    try:
        prev_submissions = store.get_submission_records(
            s3_client, state_bucket, bucket_name
        )
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT, bucket=bucket_name,
            cause=f"Failed to read prior submission records (skipping recovery): {exc}",
        ))
        prev_submissions = {}

    job_check = _describe_prior_jobs(
        ctx, state_writer, store, prev_submissions, tracking,
    )

    recovery = plan_recovery(
        records=prev_submissions,
        outcomes=job_check.outcomes,
        bucket_name=bucket_name,
        threshold=ctx.max_batch_job_failures,
    )

    if job_check.any_check_failed:
        new_watermark = _apply_recovery_plan(
            ctx, state_writer, state, recovery, result
        )
        if new_watermark is None:
            # Threshold breach: the bucket has been disabled, which is durable and
            # is the whole of the work this scoring authorized — there is no
            # rollback on this path, and the bucket submits nothing again until an
            # operator re-enables it. Commit the flag so a re-enabled bucket is not
            # re-disabled by the same historical failures.
            _commit_recovery_scored(state_writer, job_check.newly_scored_job_ids)
            return None
        checkpoint_watermark = new_watermark

    return (
        state_writer,
        checkpoint_watermark,
        tracking,
        recovery.consecutive_failures,
        state,
        job_check,
    )


def _process_bucket(
    bucket,
    store: state_store_module.StateStore,
    factory: ClientFactory,
    state_bucket: str,
    athena_workgroup: str,
    athena_output_location: str,
    account_id: str,
    batch_operations_role_arn: str,
    kms_key_arn: str = "",
    lookback: timedelta = DEFAULT_JOURNAL_LOOKBACK,
    on_bucket_disabled=None,
    on_submission_failure=None,
    on_journal_unavailable=None,
    max_batch_job_failures: int = 4,
    completion_report_topic_arn: str = "",
    journal_read_row_cap: int = JOURNAL_READ_ROW_CAP_DEFAULT,
    max_concurrent_jobs: int = MAX_CONCURRENT_JOBS_DEFAULT,
) -> _BucketResult:
    """Process one Monitored_Bucket for a single interval.

    Performs per-bucket fault isolation: any skip or error is logged and the
    function returns with partial counters, without raising.

    At most one job is submitted per bucket per run, before and after the
    concurrency bound: the limit caps how many jobs may be *outstanding*, not the
    submission rate, so ``BatchJobsSubmitted`` stays 0 or 1.
    """
    result = _BucketResult()
    bucket_name = bucket.name

    clients = _create_clients(bucket, factory, result)
    if clients is None:
        return result
    s3_client, athena_client, s3control_client = clients

    ctx = _BucketContext(
        bucket=bucket,
        bucket_name=bucket_name,
        s3_client=s3_client,
        athena_client=athena_client,
        s3control_client=s3control_client,
        state_bucket=state_bucket,
        athena_workgroup=athena_workgroup,
        athena_output_location=athena_output_location,
        account_id=account_id,
        batch_operations_role_arn=batch_operations_role_arn,
        kms_key_arn=kms_key_arn,
        lookback=lookback,
        journal_read_row_cap=journal_read_row_cap,
        max_batch_job_failures=max_batch_job_failures,
        max_concurrent_jobs=max_concurrent_jobs,
        on_bucket_disabled=on_bucket_disabled,
        on_submission_failure=on_submission_failure,
        on_journal_unavailable=on_journal_unavailable,
    )

    rules = _resolve_rules(ctx)
    if rules is None:
        return result

    prep = _prepare_state_and_recovery(
        ctx, store, s3_client, state_bucket, bucket_name,
        writer=None, tracking_arn=completion_report_topic_arn,
        account_id=account_id, result=result,
    )
    if prep is None:
        return result
    (
        writer,
        checkpoint_watermark,
        tracking,
        bucket_consecutive_failures,
        state,
        job_check,
    ) = prep

    outstanding_jobs = job_check.outstanding
    result.outstanding_jobs = len(outstanding_jobs)

    # Defer this bucket's work once its outstanding job count reaches the limit.
    #
    # The bound exists because not bounding replaces one unbounded behavior with
    # another. A bandwidth-bound bucket submitting every 15 minutes accumulates
    # jobs at roughly four an hour, against an account-level Batch Operations job
    # quota this Solution does not model, and each job carries a per-job charge.
    #
    # Bounded rather than serialized: one job already spans every one of the
    # bucket's tag-scoped rules and reaches terminal only when all its tasks do,
    # so a rule targeting small objects already waits on one targeting large
    # objects inside a single job. Serializing extends that head-of-line blocking
    # across batches without limit, and holds the watermark meanwhile, so the
    # journal read window grows until a resumed run hits JournalReadRowCap and
    # drains one capped job at a time. A job's duration is set by replication
    # throughput, not task count: 50 GiB across 5,400 objects took just over 5
    # minutes cross-Region, which puts a multi-terabyte job into hours and a
    # multi-hundred-terabyte one into days, against a CheckFrequencyMinutes that
    # defaults to 15.
    #
    # Deferring costs nothing but time. The return happens before the journal is
    # read, so no Athena query is billed, and because nothing is submitted the
    # watermark does not advance and no operation enters `processed_window`:
    # every pending tagging event stays eligible and is picked up whole once a
    # job finishes. This is the same state path an ordinary submission failure
    # already takes, and it is per bucket — other buckets in this run are
    # unaffected.
    if len(outstanding_jobs) >= max_concurrent_jobs:
        result.submission_deferred = True
        # The oldest outstanding job, not an arbitrary one: it is the one an
        # operator would investigate.
        oldest = _oldest_outstanding(outstanding_jobs)
        observability.emit(observability.log_audit(
            action="submission_deferred_job_in_flight",
            source_bucket=bucket_name,
            details={
                "outstanding_count": len(outstanding_jobs),
                "limit": max_concurrent_jobs,
                "job_id": oldest.job_id if oldest else "",
                "job_status": oldest.status if oldest else "",
                "job_age_seconds": (
                    oldest.elapsed_seconds(datetime.now(tz=UTC)) if oldest else None
                ),
            },
        ))
        # A deferral held up entirely by jobs whose status could not be read is
        # not the ordinary "a job outlasted an interval" case, and an audit entry
        # is the wrong weight for it: nothing here is working as intended. It
        # resolves on its own once the describes succeed, or at
        # _UNDESCRIBABLE_JOB_MAX_AGE if they never do, but an operator should not
        # have to notice a stretch of SubmissionDeferred datapoints to find out.
        if all(job.status == _UNKNOWN_JOB_STATUS for job in outstanding_jobs):
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=(
                    f"Submission deferred, and the status of all "
                    f"{len(outstanding_jobs)} outstanding job(s) is unknown because "
                    f"every DescribeJob call failed. This bucket submits nothing "
                    f"while that holds. Check that the execution role still holds "
                    f"s3:DescribeJob."
                ),
            ))
        return result

    journal_result = _read_journal_window(
        ctx, checkpoint_watermark, result, writer)
    if journal_result is None:
        return result
    ops, since_timestamp, journal_until = journal_result

    deduped_ops, candidate_hwm, raw_count = _select_eligible(
        ctx, ops, state, result)

    # Acquire lease after dedup — when candidate_hwm is None there is nothing
    # to lease, so use a no-op context manager.
    lease_cm = (
        _lease_scope(writer, ctx, candidate_hwm, lookback, result)
        if candidate_hwm is not None
        else contextlib.nullcontext(_LeaseHolder())
    )
    try:
        with lease_cm as holder:
            sub_outcome = _leased_manifest_and_submit(
                ctx=ctx,
                writer=writer,
                result=result,
                deduped_ops=deduped_ops,
                rules=rules,
                raw_count=raw_count,
                journal_read_row_cap=journal_read_row_cap,
                since_timestamp=since_timestamp,
                journal_until=journal_until,
                tracking=tracking,
                bucket_consecutive_failures=bucket_consecutive_failures,
                checkpoint_watermark=checkpoint_watermark,
                candidate_hwm=candidate_hwm,
                holder=holder,
            )
            if sub_outcome is None:
                return result

    except state_store_module.ConditionalWriteError as exc:
        observability.emit(observability.log_error(
            component=_COMPONENT, bucket=bucket_name,
            cause=f"Lease acquisition failed (stale ETag — concurrent run?): {exc}",
        ))
        result.errored = True
        return result
    except Exception as exc:  # noqa: BLE001
        observability.emit(observability.log_error(
            component=_COMPONENT, bucket=bucket_name,
            cause=f"Lease acquisition failed: {exc}",
        ))
        result.errored = True
        return result

    if holder.release_ok:
        if holder.submitted_refs is not None:
            result.progressed = True
        _persist_submission(
            ctx, writer, sub_outcome.last_submission, result,
            terminal_job_ids=job_check.terminal_job_ids,
            completion_tracking_enabled=bool(completion_report_topic_arn),
        )

    # The readmission is consumed here and nowhere earlier: a job has been
    # submitted for the window the rollback widened, so the rolled-back range is
    # covered by billed work whether or not the record persisted above. Every
    # earlier return — the concurrency deferral, a journal read failure, a
    # preflight or manifest write failure, a submission failure, a lease failure —
    # leaves the flag uncommitted, so the next run scores the job again and
    # readmits the same range. Requirements 5.1, 5.2.
    if sub_outcome.succeeded:
        _commit_recovery_scored(writer, job_check.newly_scored_job_ids)

    # A job submitted by this run is outstanding too, and the completion report is
    # built after this returns, so the count a subscriber sees has to include it.
    # Reporting the pre-submission count could let a report say nothing remains in
    # tracking while the job this very run started was still replicating.
    #
    # Reaching here means the DescribeJob loop ran, so the count is an int; the
    # guard keeps that an assertion rather than an assumption.
    if result.outstanding_jobs is not None:
        result.outstanding_jobs += result.submitted

    return result


# ---------------------------------------------------------------------------
# Isolated completion-tracking publish phase.
# ---------------------------------------------------------------------------


def _run_completion_tracking_interval(
    buckets: list[MonitoredBucket],
    factory: ClientFactory,
    store: state_store_module.StateStore,
    state_bucket: str,
    completion_report_topic_arn: str = "",
    run_state: dict[str, _BucketRunState] | None = None,
) -> None:
    """Publish quiescent completion items per bucket.

    The report-resolution path runs during bucket processing. This isolated
    phase remains after checkpoint and metrics work so newly resolved items may
    publish in the same invocation. Per-bucket failures are isolated and leave
    other buckets unaffected.

    *run_state* carries the two values this phase cannot derive from the state
    object — the bucket's outstanding job count and whether its submission was
    deferred — so the report can state whether replication work is still in
    progress. A bucket with no entry reports zero and False, which is what a
    bucket skipped before processing should report.

    Items written by 1.0.1 that 1.1.0 cannot resolve are normalized to
    ``UNKNOWN`` in memory here and drain through the ordinary publish-then-delete
    path. There is no separate migration write; see
    ``completion_tracker.resolve_legacy_item`` and design.md Decision 5.
    """
    if not completion_report_topic_arn:
        return

    sns_client_by_region: dict[str, Any] = {}

    def _sns_client(region: str):
        client = sns_client_by_region.get(region)
        if client is None:
            client = factory.create_sns_client(region=region)
            sns_client_by_region[region] = client
        return client

    for bucket in buckets:
        source_bucket = bucket.name
        try:
            s3_client = factory.create_s3_client(region=bucket.region)
            all_items = store.get_all_completion_items(
                s3_client, state_bucket, source_bucket
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=f"Failed to read completion items for publish: {exc}",
            ))
            continue

        if not all_items:
            continue

        try:
            scan_state_by_config = store.get_scan_state(
                s3_client, state_bucket, source_bucket
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2 isolation
            # Conservative fallback: treat as if no scan has ever run for
            # this bucket's configs (quiescence_check already treats a
            # missing config-id entry as not-quiescent), rather than
            # aborting the whole bucket's publish evaluation this pass.
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=f"Failed to read scan state for publish (treating as empty): {exc}",
            ))
            scan_state_by_config = {}

        publishable: list[tuple[str, TrackedObject]] = []
        for item_key, stored_item in all_items.items():
            try:
                # A state object written by 1.0.1 can hold items 1.1.0 has no
                # way to resolve: lifecycle PENDING, or a RESOLVED outcome of
                # PENDING/GONE/EXPIRED. Left alone they would sit here forever,
                # never publishable and never pruned, so the bucket's
                # completion_items map would only ever grow and the objects they
                # describe would never be reported at all.
                #
                # Normalizing in memory lets the existing publish-then-delete
                # path drain them: they report as UNKNOWN and are deleted with
                # everything else in the batch (design.md Decision 5).
                item = completion_tracker.resolve_legacy_item(stored_item)
                if completion_tracker.should_publish(item, scan_state_by_config):
                    publishable.append((item_key, item))
            except Exception as exc:  # noqa: BLE001 — Requirement 6.2, Property 13
                # This entry is the only signal that an item is stuck. It is not
                # publishable, so it is never deleted, and the report carries no
                # count of items left behind for it to show up in. The object key
                # is deliberately absent — keys are never logged — so the count of
                # these entries is what an operator has to go on.
                observability.emit(observability.log_error(
                    component="Completion_Tracker",
                    bucket=source_bucket,
                    cause=(
                        f"should_publish evaluation failed for an item: {exc}. "
                        f"The item stays in this bucket's completion items and is "
                        f"not reported; it will be retried on the next run."
                    ),
                ))
                continue

        if not publishable:
            continue

        # One report per batch: SNS rejects a body over 256 KiB, and because
        # items are deleted only after a successful publish, a single
        # oversized report would fail identically every run and pin its items
        # in the state object permanently (Requirement 4.9).
        key_by_id = {id(item): item_key for item_key, item in publishable}
        batches = completion_tracker.chunk_items_for_report(
            [item for _, item in publishable]
        )

        # The report deliberately carries no count of items left behind. Such a
        # count is always zero in any report that gets sent: quiescence is keyed
        # per bucket, so every item here is tested against the same ScanState, and
        # the `if not publishable` above means a report exists only when at least
        # one item passed. A run that matched anything records a non-zero match
        # count and so publishes nothing; a run that matched nothing records a
        # zero-match scan later than every job creation time, so everything passes.
        # There is no ordering that leaves some items quiescent and others not.
        #
        # The one exception is an item whose should_publish raised above, which is
        # reported by that error rather than by a number here.
        #
        # The job-level values below are duplicated across chunks rather than
        # apportioned: the chunks are one logical report split only to fit SNS's
        # message limit and carry no ordering, so a per-chunk countdown would imply
        # a sequence that does not exist.
        bucket_state = (run_state or {}).get(source_bucket, _BucketRunState())

        region = next((b.region for b in buckets if b.name == source_bucket), "")
        item_keys: list[str] = []
        for batch in batches:
            report = completion_tracker.build_completion_report(
                source_bucket,
                batch,

                outstanding_jobs=bucket_state.outstanding_jobs,
                submission_deferred=bucket_state.submission_deferred,
            )
            result = sns_report_adapter.publish_completion_report(
                _sns_client(region),
                completion_report_topic_arn,
                report,
                subject=completion_tracker.format_completion_report_subject(report),
            )
            if not result.success:
                # Leave this batch's items in place for the next interval;
                # batches already published are still deleted below, so a
                # partial failure never republishes what has been delivered.
                observability.emit(observability.log_error(
                    component="Completion_Tracker",
                    bucket=source_bucket,
                    cause=f"Failed to publish Completion_Report: {result.error_reason}",
                ))
                continue
            item_keys.extend(key_by_id[id(item)] for item in batch)

        if not item_keys:
            continue
        try:
            _, delete_etag = store.get_checkpoint(
                s3_client, state_bucket, source_bucket
            )
            store.delete_completion_items(
                s3_client,
                state_bucket,
                source_bucket,
                item_keys,
                delete_etag,
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=(
                    "Completion_Report published but failed to delete "
                    f"covered items (risk of duplicate report next interval): {exc}"
                ),
            ))
            continue

        observability.emit(observability.log_audit(
            action="completion_report_published",
            source_bucket=source_bucket,
            details={"item_count": len(publishable)},
        ))
