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

When InlineHashCeiling is exceeded for a bucket, the orchestrator calls
``runtime_config['on_bucket_disable'](bucket_name, reason)`` if provided,
then skips that bucket and continues processing the remaining buckets.
The lambda handler supplies this callback and uses it to write the
``disabled``/``disabled_reason``/``disabled_at`` fields back to
``solution-config.json``.

Requirements: 4.3, 6.1, 7.3, 8.1, 8.2, 8.3, 9.1, 9.3, 9.4, 11.1
"""
from __future__ import annotations

import concurrent.futures
import contextlib
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Any
from collections.abc import Callable

from src.adapters import athena_journal_adapter
from src.adapters import batch_operations_adapter
from src.adapters import bops_report_reader
from src.adapters import bucket_policy_adapter
from src.adapters import replication_config_adapter
from src.adapters import source_status_adapter
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
from src.core import completion_serializer
from src.core import completion_tracker
from src.core import config_loader
from src.core import journal_dedup
from src.core import observability
from src.core import rule_matcher
from src.core.delete_filter import filter_deleted_versions
from src.core.manifest_generator import ManifestGenerator, serialize
from src.core.manifest_strategy import (
    JOURNAL_READ_ROW_CAP_DEFAULT,
    ManifestFormat,
)
from src.core.models import (
    BucketMetrics,
    FailureClass,
    Lease,
    LeaseStatus,
    MatchedObject,
    MonitoredBucket,
    RunOutcome,
    RunResult,
    SubmissionRecord,
    SubmissionStatus,
    TrackedObject,
)
from src.core.job_recovery import JobOutcome, RecoveryPlan, plan_recovery
from src.core.watermark import subtract as watermark_subtract

logger = logging.getLogger(__name__)

_COMPONENT = "Orchestrator"

# Default journal lookback window.  Each run re-scans the journal from
# (watermark - lookback) so records that S3 Metadata delivered late (the
# journal is eventually consistent) are still picked up.  The bounded
# processed-operation window suppresses re-submission of records already
# included in a job, so the lookback never causes redundant replication.
DEFAULT_JOURNAL_LOOKBACK = timedelta(hours=1)

# ---------------------------------------------------------------------------
# Completion-tracking interval defaults (design.md Decisions 3, 5)
# ---------------------------------------------------------------------------

# Default cap on the total number of Source_Status_Check HeadObject calls
# issued per _run_completion_tracking_interval invocation.
COMPLETION_CHECK_BATCH_SIZE_DEFAULT = 2000

# How long a Tracked_Object may stay PENDING before it is abandoned. This is a
# backstop, not a replication deadline: it must comfortably exceed both the
# time real replication takes and several Processing_Intervals (which may
# themselves be up to 24 hours), so it never abandons work that is genuinely
# still in flight. Seven days satisfies both while still bounding the state
# object's growth.
COMPLETION_ITEM_TTL_DEFAULT = timedelta(days=7)

# Maximum wall-clock seconds to wait for a single completion-tracking
# HeadObject check (mirrors batch_operations_adapter.py's _TIMEOUT_SECONDS).
_COMPLETION_CHECK_TIMEOUT_SECONDS: float = 60.0

# Default number of concurrent worker threads for _check_batch (design.md
# Decision 3's check_batch pseudocode).
_COMPLETION_CHECK_MAX_WORKERS = 20

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

    S3 Batch Operations writes the actual report object under a
    service-generated subpath of this prefix (e.g.
    ``job-<job_id>/results/<manifest-hash>.csv``) — ``bops_report_reader``
    lists everything under this prefix rather than reading one specific key.
    """
    sanitized_manifest_key = manifest_key.replace("/", "_")
    return f"completion-reports/{replication_config_id}/{sanitized_manifest_key}/"


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
    ) -> None:
        """Completion-merge hook — read report, merge configs, clear alerted."""
        if job_status not in ("Complete", "Failed", "Cancelled"):
            return
        try:
            if not store.completion_job_exists(
                s3_client, self._state_bucket, bucket_name, rec.job_id
            ):
                report_prefix = _completion_report_prefix(config_id, rec.manifest_key)
                entries = bops_report_reader.read_bops_completion_report(
                    s3_client,
                    self._state_bucket,
                    report_prefix,
                )
                writer.merge_completion_configs(
                    entries=entries,
                    replication_config_id=bucket_name,
                    job_id=rec.job_id,
                    manifest_generated_at=job_response["Job"]["CreationTime"],
                )
                alerted = store.get_alerted_configs(
                    s3_client, self._state_bucket, bucket_name
                )
                if bucket_name in alerted:
                    writer.clear_alerted_config(
                        replication_config_id=bucket_name,
                    )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.1 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket_name,
                cause=(
                    f"Failed to read BOPS_Completion_Report or merge "
                    f"Config_Contexts for job {rec.job_id!r} (config "
                    f"{config_id!r}): {exc}"
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

    def prime_bucket_policy(
        self, s3_client, bucket_name: str, replication_role_arn: str,
    ) -> None:
        """Pre-submission bucket-policy priming — isolated, best-effort."""
        try:
            bucket_policy_adapter.ensure_completion_report_bucket_policy(
                s3_client,
                self._state_bucket,
                bucket_name,
                replication_role_arn,
                self._account_id,
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 9.7 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=self._state_bucket,
                cause=(
                    "Failed to ensure Completion_Report_Bucket_Policy_"
                    f"Statement for bucket {bucket_name!r}: {exc}"
                ),
            ))

    def report_prefix(self, bucket_name: str, manifest_key: str) -> str | None:
        """Derive the completion report prefix for a submission."""
        return _completion_report_prefix(bucket_name, manifest_key)


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

    def prime_bucket_policy(self, *_args, **_kwargs) -> None:  # noqa: ANN002
        return None

    def report_prefix(self, *_args, **_kwargs) -> None:  # noqa: ANN002
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
    kms_key_arn: str
    lookback: timedelta
    journal_read_row_cap: int
    max_batch_job_failures: int
    on_bucket_disable: Callable | None
    on_submission_failure: Callable | None


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

    def record_submission(self, record: SubmissionRecord) -> None:
        """Persist a submission record, updating the held ETag."""
        self._etag = self._store.record_submission(
            self._s3_client, self._state_bucket, record, self._etag,
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

    def merge_completion_configs(
        self, entries, replication_config_id: str, job_id: str,
        manifest_generated_at: datetime,
        timestamps: dict | None = None,
    ) -> None:
        """Merge completion configs, updating the held ETag."""
        self._etag = self._store.merge_completion_configs(
            self._s3_client, self._state_bucket, self._source_bucket,
            entries=entries,
            replication_config_id=replication_config_id,
            job_id=job_id,
            manifest_generated_at=manifest_generated_at,
            current_etag=self._etag,
            timestamps=timestamps,
        )

    def store_completion_timestamps(
        self, timestamps: dict,
    ) -> None:
        """Persist per-object timestamps for the completion report email."""
        self._etag = self._store.store_completion_timestamps(
            self._s3_client, self._state_bucket, self._source_bucket,
            timestamps=timestamps,
            current_etag=self._etag,
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


# ---------------------------------------------------------------------------
# Internal result type — carries per-bucket counters back to the caller
# ---------------------------------------------------------------------------


@dataclass
class _BucketResult:
    """Aggregated counters from processing one Monitored_Bucket."""

    ops_read: int = 0       # distinct logical operations forwarded to matching (after dedup)
    raw_records: int = 0    # raw journal records read before dedup
    matched: int = 0        # total Matched_Object entries accumulated
    submitted: int = 0      # successful Batch_Replication_Job submissions
    errored: bool = False   # True when the bucket was skipped due to a processing error
    capped: bool = False    # True when this run was a Capped_Run (journal_until is not None)
    progressed: bool = False  # True when a job was submitted AND the checkpoint advanced


# ---------------------------------------------------------------------------
# Extracted phases — called from _process_bucket
# ---------------------------------------------------------------------------


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
    result: _BucketResult,
) -> list | None:
    """Step a: derive replication rules and validate the role ARN.

    Returns the list of rules on success, or None if the bucket should be
    skipped (no rules found or role ARN invalid).  When skipping due to an
    invalid role ARN, *result* is marked errored.
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

    replication_role_arn = rules[0].replication_role_arn

    if not completion_tracker.validate_replication_role_arn(
        replication_role_arn, ctx.account_id
    ):
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=ctx.bucket_name,
            cause=(
                f"Replication role ARN {replication_role_arn!r} on bucket "
                f"{ctx.bucket_name!r} is not a well-formed IAM role ARN in "
                f"account {ctx.account_id!r}. Skipping this bucket: the value "
                f"would otherwise be passed to S3 Batch Operations as the "
                f"job role."
            ),
        ))
        result.errored = True
        return None

    return rules


# ---------------------------------------------------------------------------
# Failed-job recovery phase — extracted from Step b2
# ---------------------------------------------------------------------------


@dataclass
class _JobCheckResult:
    """Outcome of the DescribeJob loop for one bucket's prior submissions."""

    failed_lows: list[str]
    any_check_ran: bool
    any_check_failed: bool
    outcomes: list[JobOutcome]


def _describe_prior_jobs(
    ctx: _BucketContext,
    writer: StateWriter,
    store: state_store_module.StateStore,
    prev_submissions: dict[str, SubmissionRecord],
    tracking: _CompletionTracking,
) -> _JobCheckResult:
    """Wrap the DescribeJob loop: check each prior submission's job status.

    For each prior submission record with a job_id, calls DescribeJob to get
    the terminal status.  Runs the completion-merge hook (positioned before
    the Failed/Cancelled circuit-breaker logic so an exception there cannot
    reach it).  Collects watermark_lows of failed jobs and emits readmission
    audit logs.

    The consolidation arithmetic is performed by ``plan_recovery`` in
    ``src.core.job_recovery`` — this function collects the raw outcomes and
    the caller passes them to that pure decision function.

    Returns a _JobCheckResult with the loop's collected state.
    """
    bucket_name = ctx.bucket_name
    s3_client = ctx.s3_client
    s3control_client = ctx.s3control_client
    account_id = ctx.account_id

    failed_lows: list[str] = []
    any_check_ran = False
    any_check_failed = False
    outcomes: list[JobOutcome] = []

    for config_id, rec in prev_submissions.items():
        if not rec.job_id:
            continue
        try:
            resp = s3control_client.describe_job(
                AccountId=account_id, JobId=rec.job_id
            )
            job_status = resp["Job"]["Status"]
        except Exception as exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"DescribeJob {rec.job_id!r} failed (best-effort): {exc}",
            ))
            continue

        any_check_ran = True

        outcomes.append(JobOutcome(
            config_id=config_id,
            job_id=rec.job_id,
            status=job_status,
            watermark_low=rec.watermark_low,
            watermark_high=rec.watermark_high,
            consecutive_failures=rec.consecutive_failures,
        ))

        # Completion-tracking creation hook — positioned before the
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
        )

        if job_status in ("Failed", "Cancelled"):
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
    )


def _apply_recovery_plan(
    ctx: _BucketContext,
    state: Any,
    recovery: RecoveryPlan,
    result: _BucketResult,
) -> str | None:
    """Apply a :class:`RecoveryPlan` produced by :func:`plan_recovery`.

    Performs the side effects the pure function cannot: threshold-breach
    disabling (log + on_bucket_disable callback), the empty-watermark_low audit
    log, and the watermark rollback mutation.

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
        if ctx.on_bucket_disable is not None:
            try:
                ctx.on_bucket_disable(bucket_name, recovery.disable_reason)
            except Exception as cb_exc:  # noqa: BLE001
                observability.emit(observability.log_error(
                    component=_COMPONENT,
                    bucket=bucket_name,
                    cause=f"Failed to write disabled flag to config: {cb_exc}",
                ))
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


def _read_journal_window(
    ctx: _BucketContext,
    checkpoint_watermark: str,
    result: _BucketResult,
) -> tuple[list, str | None, str | None] | None:
    """Steps d and d0: apply the row-count cap and read the journal window.

    Performs find_row_count_boundary (best-effort), emits the journal_read_capped
    audit when capped, calls read_journal, reports journal errors, and returns
    early on fatal errors.

    Returns (ops, since_timestamp, journal_until) on success, or None when a
    fatal journal error means this bucket should be skipped.  Sets result.capped
    as a side-effect when the row-count boundary fires.
    """
    bucket_name = ctx.bucket_name
    athena_client = ctx.athena_client
    lookback = ctx.lookback
    journal_read_row_cap = ctx.journal_read_row_cap
    athena_workgroup = ctx.athena_workgroup
    athena_output_location = ctx.athena_output_location

    since_timestamp = watermark_subtract(checkpoint_watermark, lookback)

    # Row-count cap boundary (Step d0): find the record_timestamp that would
    # cap this run to at most journal_read_row_cap rows.
    journal_until: str | None = None
    try:
        journal_until = athena_journal_adapter.find_row_count_boundary(
            athena_client=athena_client,
            bucket_name=bucket_name,
            since_timestamp=since_timestamp if since_timestamp else None,
            row_cap=journal_read_row_cap,
            athena_workgroup=athena_workgroup,
            output_location=athena_output_location,
        )
    except Exception as exc:  # noqa: BLE001
        # Best-effort: if the boundary check itself fails, proceed
        # uncapped rather than blocking the whole run on a check that
        # exists purely to prevent an unusual, rare condition.
        observability.emit(observability.log_error(
            component=_COMPONENT,
            bucket=bucket_name,
            cause=f"Row-count boundary check failed (proceeding uncapped): {exc}",
        ))

    if journal_until is not None:
        result.capped = True
        observability.emit(observability.log_audit(
            action="journal_read_capped",
            source_bucket=bucket_name,
            details={
                "row_cap": journal_read_row_cap,
                "until_timestamp": journal_until,
                "since_timestamp": since_timestamp,
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

    result.raw_records = raw_count
    result.ops_read = len(deduped_ops)

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
def _lease_scope(writer: StateWriter, ctx: _BucketContext, candidate_hwm, lookback):
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
    if ctx.on_bucket_disable is not None:
        try:
            ctx.on_bucket_disable(bucket_name, disable_reason_submission)
        except Exception as cb_exc:  # noqa: BLE001
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"Failed to disable bucket after submission streak: {cb_exc}",
            ))
    return True


def _submit_job(
    ctx: _BucketContext,
    writer: StateWriter,
    result: _BucketResult,
    written: Any,
    replication_role_arn: str,
    tracking: _CompletionTracking,
    bucket_consecutive_failures: int,
    checkpoint_watermark: str,
    candidate_hwm: str | None,
) -> _SubmissionOutcome:
    """Submit one Batch_Replication_Job and handle all three outcome branches.

    Returns a _SubmissionOutcome so the caller can decide whether to release
    the lease and persist the submission record.
    """
    bucket_name = ctx.bucket_name
    s3_client = ctx.s3_client
    s3control_client = ctx.s3control_client
    account_id = ctx.account_id
    state_bucket = ctx.state_bucket

    # Derive completion_report_prefix via the tracking collaborator.
    completion_report_prefix = tracking.report_prefix(
        bucket_name, written.s3_location.key
    )

    # Pre-submission bucket-policy priming — isolated, best-effort.
    tracking.prime_bucket_policy(s3_client, bucket_name, replication_role_arn)

    submission = batch_operations_adapter.submit_batch_job(
        s3control_client=s3control_client,
        account_id=account_id,
        manifest_location=written.s3_location,
        manifest_etag=written.etag,
        replication_role_arn=replication_role_arn,
        config_id=bucket_name,
        object_count=written.object_count,
        source_bucket=bucket_name,
        has_version_ids=written.all_versioned,
        manifest_format=ManifestFormat.INVENTORY_REPORT.value,
        completion_report_prefix=completion_report_prefix,
        state_bucket=state_bucket if completion_report_prefix else None,
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
) -> None:
    """Persist the submission record into the per-bucket state object.

    Best-effort: the job is already submitted and the checkpoint already
    advanced, so a persistence failure here is non-fatal and only loses the
    audit convenience of the in-state record (the job id is also captured in
    the submission and audit log entries).
    """
    if last_submission is None:
        return
    try:
        writer.record_submission(last_submission)
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
    kms_key_arn: str = runtime_config.get("kms_key_arn", "") or ""

    # Validate kms_key_arn format when provided (fail fast rather than wait
    # for a cryptic InvalidKeyId or AccessDenied during mid-run put_object).
    if kms_key_arn:
        _validate_kms_key_arn(kms_key_arn)

    # Completion-tracking feature gate (Requirement 4.8): the SNS topic ARN
    # a Completion_Report would be published to. Absent/empty means the
    # entire completion-tracking feature is a no-op for this deployment —
    # no Completion_Record is created, no Destination_Presence_Check or
    # Source_Status_Check is issued — mirroring the existing
    # MetricsNamespace no-op-when-unset pattern. Task 22.1 wires this to a
    # stack-provisioned SNS topic ARN via the COMPLETION_REPORT_TOPIC_ARN
    # env var; for now this key is simply read from runtime_config like any
    # other optional setting.
    completion_report_topic_arn: str = (
        runtime_config.get("completion_report_topic_arn", "") or ""
    ).strip()

    lookback = _resolve_lookback(runtime_config)

    # Callback invoked when InlineHashCeiling is exceeded for a bucket.
    # The lambda handler provides this to write disabled=True back to the
    # solution-config.json; library callers may omit it.
    on_bucket_disable = runtime_config.get("on_bucket_disable")

    # Callback invoked on the first occurrence of a permanent client-side
    # submission failure. The lambda handler wires this to publish to
    # BatchJobFailureLogGroup / BatchJobFailureTopic, mirroring the
    # bucket-disabled alert shape (submission-failure-visibility Req 3.1).
    on_submission_failure = runtime_config.get("on_submission_failure")

    # Maximum consecutive S3 Batch Operations job failures before the bucket
    # is disabled to prevent runaway per-job costs on a low-churn deployment.
    # Default 4 matches the CloudFormation parameter default.
    max_batch_job_failures: int = max(1, int(runtime_config.get("max_batch_job_failures", 4)))

    # -----------------------------------------------------------------------
    # Journal_Read_Row_Cap — the single scale knob (Requirement 2.1)
    # -----------------------------------------------------------------------
    journal_read_row_cap: int = max(1, int(
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
    disabled_buckets = 0  # run-level count for CloudWatch (auto-disable visibility)
    any_capped_and_progressed = False  # RunOutcome signal for Self_Reinvocation

    for bucket in app_config.buckets:
        if bucket.disabled:
            disabled_buckets += 1
            entry = observability.log_error(
                component=_COMPONENT,
                bucket=bucket.name,
                cause=(
                    f"Bucket is disabled in solution-config "
                    f"(disabled_at={bucket.disabled_at!r}, "
                    f"reason={bucket.disabled_reason!r}). "
                    f"Re-enable by setting disabled=false in the config."
                ),
            )
            observability.emit(entry)
            continue

        result = _process_bucket(
            bucket=bucket,
            store=store,
            factory=factory,
            state_bucket=state_bucket,
            athena_workgroup=athena_workgroup,
            athena_output_location=athena_output_location,
            account_id=account_id,
            kms_key_arn=kms_key_arn,
            lookback=lookback,
            on_bucket_disable=on_bucket_disable,
            on_submission_failure=on_submission_failure,
            max_batch_job_failures=max_batch_job_failures,
            completion_report_topic_arn=completion_report_topic_arn,
            journal_read_row_cap=journal_read_row_cap,
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
                check_batch_size=int(
                    runtime_config.get(
                        "completion_check_batch_size",
                        COMPLETION_CHECK_BATCH_SIZE_DEFAULT,
                    )
                ),
                completion_report_topic_arn=completion_report_topic_arn,
                completion_item_ttl=timedelta(
                    hours=float(
                        runtime_config.get(
                            "completion_item_ttl_hours",
                            COMPLETION_ITEM_TTL_DEFAULT.total_seconds() / 3600,
                        )
                    )
                ),
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
    replication_role_arn: str,
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

    # Persist per-object timestamps (tagged_at, last_modified) so the
    # completion report email can include them even though TrackedObjects
    # are created on a later invocation (when the BOPS report arrives).
    obj_timestamps = gen.get_timestamps(bucket_name)
    if obj_timestamps:
        try:
            writer.store_completion_timestamps(obj_timestamps)
        except Exception as exc:  # noqa: BLE001
            # Non-fatal: timestamps are nice-to-have for the email; do not
            # block job submission if the write fails.
            observability.emit(observability.log_error(
                component=_COMPONENT,
                bucket=bucket_name,
                cause=f"Failed to store completion timestamps: {exc}",
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
        replication_role_arn=replication_role_arn,
        tracking=tracking,
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
) -> tuple[StateWriter, str, _CompletionTracking, int, Any] | None:
    """Read checkpoint, set up writer/tracking, run job-check and recovery.

    Returns (writer, checkpoint_watermark, tracking, consecutive_failures, state)
    or None if the checkpoint read fails.
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
        new_watermark = _apply_recovery_plan(ctx, state, recovery, result)
        if new_watermark is None:
            return None
        checkpoint_watermark = new_watermark

    return state_writer, checkpoint_watermark, tracking, recovery.consecutive_failures, state


def _process_bucket(
    bucket,
    store: state_store_module.StateStore,
    factory: ClientFactory,
    state_bucket: str,
    athena_workgroup: str,
    athena_output_location: str,
    account_id: str,
    kms_key_arn: str = "",
    lookback: timedelta = DEFAULT_JOURNAL_LOOKBACK,
    on_bucket_disable=None,
    on_submission_failure=None,
    max_batch_job_failures: int = 4,
    completion_report_topic_arn: str = "",
    journal_read_row_cap: int = JOURNAL_READ_ROW_CAP_DEFAULT,
) -> _BucketResult:
    """Process one Monitored_Bucket for a single interval.

    Performs per-bucket fault isolation: any skip or error is logged and the
    function returns with partial counters, without raising.
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
        kms_key_arn=kms_key_arn,
        lookback=lookback,
        journal_read_row_cap=journal_read_row_cap,
        max_batch_job_failures=max_batch_job_failures,
        on_bucket_disable=on_bucket_disable,
        on_submission_failure=on_submission_failure,
    )

    rules = _resolve_rules(ctx, result)
    if rules is None:
        return result

    replication_role_arn = rules[0].replication_role_arn

    prep = _prepare_state_and_recovery(
        ctx, store, s3_client, state_bucket, bucket_name,
        writer=None, tracking_arn=completion_report_topic_arn,
        account_id=account_id, result=result,
    )
    if prep is None:
        return result
    writer, checkpoint_watermark, tracking, bucket_consecutive_failures, state = prep
    journal_result = _read_journal_window(ctx, checkpoint_watermark, result)
    if journal_result is None:
        return result
    ops, since_timestamp, journal_until = journal_result

    deduped_ops, candidate_hwm, raw_count = _select_eligible(
        ctx, ops, state, result)

    # Acquire lease after dedup — when candidate_hwm is None there is nothing
    # to lease, so use a no-op context manager.
    lease_cm = (
        _lease_scope(writer, ctx, candidate_hwm, lookback)
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
                replication_role_arn=replication_role_arn,
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
        _persist_submission(ctx, writer, sub_outcome.last_submission)

    return result


# ---------------------------------------------------------------------------
# Isolated completion-tracking interval (design.md Decisions 3, 6, 7, 8) —
# source-only. There is no destination-account or destination-region client,
# no CheckKind, and no age gate anywhere in this section (design.md's
# "Removes destination access entirely" note).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _TrackingCandidate:
    """One check-eligible ``TrackedObject`` selected for a Source_Status_Check
    in a single ``_run_completion_tracking_interval`` invocation, tagged with
    which bucket it belongs to.

    Thin orchestrator-level wrapper around
    ``completion_tracker.CheckCandidate`` (``item_key`` + ``obj``): that
    dataclass has no notion of *which bucket* the item belongs to —
    ``select_check_candidates`` operates on one bucket's ``items`` dict at a
    time. This wrapper adds ``source_bucket`` so that, after collecting
    candidates across ALL buckets into one global list for the cross-bucket
    sort/cap step, the caller can still tell which bucket's ``s3_client``/
    ``store`` calls to use when issuing the check and persisting the
    resolution.
    """

    source_bucket: str
    item_key: str
    obj: TrackedObject

    @property
    def oldest_manifest_generated_at(self) -> datetime:
        """The oldest ``ConfigContext.manifest_generated_at`` among this
        candidate's routing configs — the deterministic cross-bucket
        ordering key (design.md Decision 1)."""
        return min(ctx.manifest_generated_at for ctx in self.obj.configs.values())


def _check_batch(
    checks: list[tuple[_TrackingCandidate, Callable[[], Any]]],
    max_workers: int = _COMPLETION_CHECK_MAX_WORKERS,
) -> list[tuple[_TrackingCandidate, Any]]:
    """Run up to ``CompletionCheckBatchSize`` Source_Status_Checks concurrently.

    Mirrors ``batch_operations_adapter.py::_call_with_timeout``'s
    thread-backed timeout pattern, generalized to a bounded pool of
    ``max_workers`` concurrent workers rather than a single in-line call.
    Each check is a zero-argument callable returning a
    ``source_status_adapter.SourceStatusResult`` (which itself never raises
    — every failure mode is represented as a ``CHECK_FAILED`` result); a
    check that unexpectedly raises, or that does not complete within
    ``_COMPLETION_CHECK_TIMEOUT_SECONDS``, is treated as ``None`` rather than
    propagating — a single failing check must never abort the rest of the
    batch (Requirement 6.2, Property 13).

    Parameters
    ----------
    checks:
        A list of ``(candidate, check_fn)`` pairs. ``check_fn`` is a
        zero-argument closure over the already-constructed source-side S3
        client and the specific object/version to check.
    max_workers:
        Maximum number of concurrent worker threads.

    Returns
    -------
    list[tuple[_TrackingCandidate, Any]]
        One ``(candidate, result)`` pair per input, in the same order as
        ``checks``. Never raises for an individual check failure.
    """
    if not checks:
        return []

    results: list[Any] = [None] * len(checks)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_index = {
            pool.submit(check_fn): index for index, (_, check_fn) in enumerate(checks)
        }
        for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results[index] = future.result(timeout=_COMPLETION_CHECK_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 — a single failing check must not
                # abort the batch (Requirement 3.6, 6.2, Property 13); treat
                # any exception (including a concurrent.futures.TimeoutError)
                # as an unexpected check failure.
                results[index] = None

    return [(checks[i][0], results[i]) for i in range(len(checks))]


def _run_completion_tracking_interval(
    buckets: list[MonitoredBucket],
    factory: ClientFactory,
    store: state_store_module.StateStore,
    state_bucket: str,
    check_batch_size: int = COMPLETION_CHECK_BATCH_SIZE_DEFAULT,
    completion_report_topic_arn: str = "",
    completion_item_ttl: timedelta = COMPLETION_ITEM_TTL_DEFAULT,
) -> None:
    """Check-and-reconcile, then publish, phases of the isolated
    completion-tracking interval — source-only (design.md Decisions 3, 6, 7).

    **Check-and-reconcile phase.** For each bucket's check-eligible
    ``TrackedObject``s (``store.get_check_eligible_items`` — already
    filtered to ``state == PENDING`` AND every ``ConfigContext``
    ``bops_confirmed``), selects candidates via
    ``completion_tracker.select_check_candidates(items)``. Across ALL
    buckets, orders candidates by the oldest routing job's
    ``manifest_generated_at``, then ``item_key`` as a deterministic
    tie-break, caps the combined total at ``check_batch_size``, issues one
    ``source_status_adapter.check_source_replication_status`` per candidate
    concurrently via ``_check_batch``, reconciles each result via
    ``completion_tracker.reconcile_source_status_check``, and persists all
    resolutions per bucket via ``store.apply_completion_resolutions``. There
    is no destination-presence client, no ``CheckKind``, and no age gate —
    every gated candidate goes straight to a Source_Status_Check.

    Per-candidate and per-bucket failures are isolated (Requirement 6.2,
    Property 13): an exception raised while checking or reconciling one
    candidate, or while persisting one bucket's resolutions, is logged via
    ``observability.log_error`` (identifying only ``job_id``/
    ``replication_config_id`` — never the object key, Requirement 7.3) and
    does not prevent other candidates or other buckets from being
    processed. A Source_Status_Check that reports a header-absent result
    resolves the Tracked_Object to ``UNKNOWN`` and also emits an error
    entry (Requirement 3.5); a successful resolution to ``COMPLETE``,
    ``PENDING``, or ``FAILED`` emits no log entry at all (Requirement 7.1).

    **Publish phase**, run after the check-and-reconcile phase above
    completes for every bucket: for each bucket in ``buckets``, reads that
    bucket's full, UNFILTERED set of ``TrackedObject``s via
    ``store.get_all_completion_items`` (deliberately NOT the check-eligible
    set from the phase above — an item that already resolved in a prior
    interval is no longer check-eligible, yet still needs its
    ``should_publish``/quiescence re-evaluated every interval), collects
    every item for which ``completion_tracker.should_publish`` is true
    (using that bucket's ``store.get_scan_state``), and — if that collected
    set is non-empty — builds ONE ``completion_tracker.build_completion_report``
    for the whole batch and publishes it once via
    ``sns_report_adapter.publish_completion_report``. On a successful
    publish, deletes every covered item via ``store.delete_completion_items``
    (guarded by a freshly-read ETag for that bucket's state object) and
    emits ``observability.log_audit(action="completion_report_published", ...)``.
    On a failed publish, emits ``observability.log_error`` identifying the
    bucket and leaves every item in the batch untouched for retry at the
    next interval (Requirement 4.5, 4.6). The whole publish phase — no
    ``get_scan_state`` call, no ``should_publish`` evaluation, no SNS client
    construction, no publish call, for any bucket — is skipped entirely
    when ``completion_report_topic_arn`` is unset (Requirement 4.8),
    mirroring the check-and-reconcile phase's own gating precedent at the
    ``run_interval`` call site. Per-item and per-bucket failures in this
    phase are isolated the same way as the check-and-reconcile phase
    (Requirement 6.2): a failure reading one bucket's items, reading its
    scan state, or evaluating one item's ``should_publish``, is logged and
    that bucket/item is skipped rather than aborting the whole pass.

    Parameters
    ----------
    buckets:
        The list of ``MonitoredBucket``s to poll (mirrors ``app_config.buckets``
        from ``run_interval``).
    factory:
        The shared ``ClientFactory`` used to construct each bucket's
        source-side S3 client (for ``source_status_adapter`` and for
        ``store.get_check_eligible_items``/``apply_completion_resolutions``),
        and, when ``completion_report_topic_arn`` is set, one SNS client
        per source region (via ``factory.create_sns_client``) for the
        publish phase. No destination-account or destination-region client
        is ever constructed anywhere in this function.
    store:
        The shared ``StateStore``.
    state_bucket:
        The scratch/state bucket name.
    check_batch_size:
        The maximum number of Source_Status_Check ``HeadObject`` calls
        issued in this invocation (default 2000).
    completion_report_topic_arn:
        ARN of the stack-provisioned ``CompletionReportTopic``. Empty/unset
        (the default) disables the entire publish phase (Requirement 4.8) —
        mirrors the existing ``MetricsNamespace``/``completion_report_topic_arn``
        no-op-when-unset pattern used elsewhere in this module.

    Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 4.1, 4.4, 4.5, 4.6, 5.4, 6.2,
    7.1, 7.2, 7.3
    """
    now = datetime.now(tz=UTC)

    # Cache one source-side s3_client per source_bucket — reuses the same
    # ClientFactory cache _process_bucket already relies on.
    source_client_by_bucket: dict[str, Any] = {}

    def _source_client(source_bucket: str):
        client = source_client_by_bucket.get(source_bucket)
        if client is None:
            region = next(
                (b.region for b in buckets if b.name == source_bucket), ""
            )
            client = factory.create_s3_client(region=region)
            source_client_by_bucket[source_bucket] = client
        return client

    # ------------------------------------------------------------------
    # 1. Collect check-eligible TrackedObjects (and their check candidates)
    #    across all buckets.
    # ------------------------------------------------------------------
    all_candidates: list[_TrackingCandidate] = []

    for bucket in buckets:
        try:
            eligible_items = store.get_check_eligible_items(
                _source_client(bucket.name), state_bucket, bucket.name
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket.name,
                cause=f"Failed to read check-eligible items: {exc}",
            ))
            continue

        if not eligible_items:
            continue

        try:
            candidates = completion_tracker.select_check_candidates(
                eligible_items
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2 isolation
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=bucket.name,
                cause=f"select_check_candidates failed: {exc}",
            ))
            continue

        for candidate in candidates:
            all_candidates.append(
                _TrackingCandidate(
                    source_bucket=bucket.name,
                    item_key=candidate.item_key,
                    obj=candidate.obj,
                )
            )

    # ------------------------------------------------------------------
    # 2. Order oldest-routing-job-first across ALL buckets, then
    #    lexicographic item_key as a deterministic tie-break; cap at
    #    check_batch_size.
    #
    # No early return when all_candidates is empty — the publish phase
    # (step 5) must still run even when the check-and-reconcile phase has
    # nothing to check this pass.
    # ------------------------------------------------------------------
    def _sort_key(candidate: _TrackingCandidate) -> tuple[datetime, str]:
        return (candidate.oldest_manifest_generated_at, candidate.item_key)

    all_candidates.sort(key=_sort_key)
    selected = all_candidates[:check_batch_size]

    # ------------------------------------------------------------------
    # 3. Build the (candidate, check_fn) pairs and run them concurrently.
    # ------------------------------------------------------------------
    checks: list[tuple[_TrackingCandidate, Callable[[], Any]]] = []
    expired: list[_TrackingCandidate] = []

    for candidate in selected:
        # Expire before spending a HeadObject on it: an object past the TTL is
        # abandoned regardless of what the check would return, so the call
        # would be wasted (Requirement 3.8).
        if completion_tracker.is_expired(candidate.obj, now, completion_item_ttl):
            expired.append(candidate)
            continue

        source_bucket = candidate.source_bucket
        object_key = candidate.obj.object_key
        version_id = candidate.obj.version_id

        def _source_check_fn(
            object_key=object_key,
            version_id=version_id,
            source_bucket=source_bucket,
        ):
            s3_client = _source_client(source_bucket)
            return source_status_adapter.check_source_replication_status(
                s3_client, source_bucket, object_key, version_id
            )

        checks.append((candidate, _source_check_fn))

    check_results = _check_batch(checks)

    # ------------------------------------------------------------------
    # 4. Reconcile each result, grouping resolutions by source_bucket.
    #    Logging: a header-absent or check-failed result emits exactly one
    #    error entry naming only job_id/replication_config_id (never the
    #    object key or item_key, Requirement 7.3); a clean COMPLETE/PENDING/
    #    FAILED resolution emits nothing (Requirement 7.1).
    # ------------------------------------------------------------------
    resolutions_by_bucket: dict[str, dict[str, TrackedObject]] = {}

    for candidate in expired:
        resolutions_by_bucket.setdefault(candidate.source_bucket, {})[
            candidate.item_key
        ] = completion_tracker.expire_tracked_object(candidate.obj, now)
        observability.emit(observability.log_audit(
            action="completion_item_expired",
            source_bucket=candidate.source_bucket,
            details={
                "job_ids": [ctx.job_id for ctx in candidate.obj.configs.values()],
                "age_seconds": int(
                    completion_tracker.tracked_object_age(candidate.obj, now)
                    .total_seconds()
                ),
                "ttl_seconds": int(completion_item_ttl.total_seconds()),
            },
        ))

    for candidate, raw_result in check_results:
        source_bucket = candidate.source_bucket
        item_key = candidate.item_key
        job_ids = [ctx.job_id for ctx in candidate.obj.configs.values()]
        config_ids = list(candidate.obj.configs.keys())

        if raw_result is None:
            # The check itself raised unexpectedly or timed out — leave the
            # item untouched (still PENDING) for retry next interval.
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=(
                    "Source_Status_Check raised an unexpected error "
                    f"(job_ids={job_ids!r}, replication_config_ids={config_ids!r})"
                ),
            ))
            continue

        try:
            reconciled = completion_tracker.reconcile_source_status_check(
                candidate.obj, raw_result, now
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2, Property 13
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=(
                    "Reconciliation failed "
                    f"(job_ids={job_ids!r}, replication_config_ids={config_ids!r}): {exc}"
                ),
            ))
            continue

        if raw_result.kind is source_status_adapter.SourceStatusCheckKind.CHECK_FAILED:
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=(
                    "Source_Status_Check failed "
                    f"(job_ids={job_ids!r}, replication_config_ids={config_ids!r}): "
                    f"{raw_result.error_reason}"
                ),
            ))
        elif raw_result.kind is source_status_adapter.SourceStatusCheckKind.HEADER_ABSENT:
            # Requirement 3.5 — resolved to UNKNOWN; record an error
            # indication naming job_id/replication_config_id, never the key.
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=(
                    "Source_Status_Check header absent "
                    f"(job_ids={job_ids!r}, replication_config_ids={config_ids!r})"
                ),
            ))

        resolutions_by_bucket.setdefault(source_bucket, {})[item_key] = reconciled

    # ------------------------------------------------------------------
    # 5. Persist all resolutions per bucket via apply_completion_resolutions.
    #    Grouped by source_bucket (one state object per bucket) so a single
    #    conditional write covers every item's resolution for that bucket.
    # ------------------------------------------------------------------
    for source_bucket, bucket_resolutions in resolutions_by_bucket.items():

        def _mutate_fn(
            payload: dict[str, Any], bucket_resolutions=bucket_resolutions
        ) -> dict[str, Any]:
            existing_items = completion_serializer.deserialize_completion_items(payload)
            for item_key, reconciled_obj in bucket_resolutions.items():
                if item_key not in existing_items:
                    # Item was deleted (e.g. already published) between
                    # selection and persistence — nothing to update.
                    continue
                existing_items[item_key] = reconciled_obj
            payload["completion_items"] = completion_serializer.serialize_completion_items(
                existing_items
            )
            return payload

        try:
            s3_client = _source_client(source_bucket)
            store.apply_completion_resolutions(
                s3_client, state_bucket, source_bucket, _mutate_fn
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 6.2, Property 13
            observability.emit(observability.log_error(
                component="Completion_Tracker",
                bucket=source_bucket,
                cause=f"Failed to persist completion resolutions: {exc}",
            ))
            continue

    # ------------------------------------------------------------------
    # 6. Publish phase — one Completion_Report per source_bucket, covering
    #    every TrackedObject whose should_publish holds.
    #
    #    Entirely a no-op when completion_report_topic_arn is unset
    #    (Requirement 4.8): no TrackedObject is read for should_publish, no
    #    ScanState is read, and no SNS client is ever constructed.
    # ------------------------------------------------------------------
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
            all_items = store.get_all_completion_items(
                _source_client(source_bucket), state_bucket, source_bucket
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
                _source_client(source_bucket), state_bucket, source_bucket
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
        for item_key, item in all_items.items():
            try:
                if completion_tracker.should_publish(item, scan_state_by_config):
                    publishable.append((item_key, item))
            except Exception as exc:  # noqa: BLE001 — Requirement 6.2, Property 13
                observability.emit(observability.log_error(
                    component="Completion_Tracker",
                    bucket=source_bucket,
                    cause=f"should_publish evaluation failed for an item: {exc}",
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

        region = next((b.region for b in buckets if b.name == source_bucket), "")
        item_keys: list[str] = []
        for batch in batches:
            report = completion_tracker.build_completion_report(source_bucket, batch)
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
                _source_client(source_bucket), state_bucket, source_bucket
            )
            store.delete_completion_items(
                _source_client(source_bucket),
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
