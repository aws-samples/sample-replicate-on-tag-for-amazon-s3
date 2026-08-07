"""Core data models for the tag-based S3 replication backfill Solution.

All models are pure Python dataclasses — no AWS dependencies.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from urllib.parse import quote, unquote

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

# journal position identifier; treated as an opaque string
SequenceNumber = str

# ---------------------------------------------------------------------------
# AppConfig / MonitoredBucket
# ---------------------------------------------------------------------------


@dataclass
class MonitoredBucket:
    """A source S3 bucket the customer wants the Solution to watch."""

    name: str    # must satisfy S3 bucket naming rules; required (1.2, 2.2)
    region: str  # non-empty, valid AWS region; required (1.2, 13.3, 13.8)
    # No tag_filter and no destination permitted (1.5)
    # The following three fields are written by the Solution when a bucket
    # must be disabled (e.g. InlineHashCeiling exceeded).  Operators can
    # also set disabled=True manually to pause a bucket without removing it.
    disabled: bool = False
    disabled_reason: str = ""
    disabled_at: str = ""   # ISO 8601 timestamp set when disabled=True is written


@dataclass
class AppConfig:
    """Validated configuration for a single Solution run."""

    buckets: list[MonitoredBucket]  # 1..1 000 entries (1.1)


# ---------------------------------------------------------------------------
# DerivedReplicationRule
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DestinationRef:
    """Opaque destination reference carried from the bucket's replication configuration.

    Never used to make destination-side calls (12.2, 13.1).
    """

    bucket_arn: str  # destination bucket ARN as recorded in the configuration


@dataclass
class DerivedReplicationRule:
    """A tag-scoped replication rule derived from a source bucket's Replication_Configuration.

    Never customer-supplied (1.5, 3.2).
    """

    source_bucket: str
    replication_config_id: str         # identifies the bucket replication config; job-grouping key
    rule_id: str
    tag_filter: dict[str, str]         # non-empty set of required tag key-value pairs (3.2)
    destination: DestinationRef        # implied by config; never accessed independently (12.2)
    key_prefix: str | None = None   # optional (3.2)


# ---------------------------------------------------------------------------
# TaggingOperation (Journal record)
# ---------------------------------------------------------------------------

# Rendered in place of a missing ``operation_version``.  The ``\x01`` prefix
# cannot occur in a real S3 version ID, and the sentinel is distinct from ``""``
# so a null version stays separable from an empty version token.
_NULL_VERSION = "\x01null-version"


@dataclass
class TaggingOperation:
    """A record from the S3 Metadata journal representing a tagging event.

    Requirements 4.2, 5.1.
    """

    source_bucket: str
    object_key: str
    resulting_tag_set: dict[str, str]   # complete tag set after the operation (5.1)
    sequence_number: SequenceNumber     # journal position; used for checkpoint + ordering (4.3)
    operation: str                       # e.g. "PutObjectTagging"
    event_time: datetime
    operation_version: str | None = None  # journal-provided per-object version token
    last_modified: datetime | None = None  # object last-modified from journal
    # Object storage class as of this journal record, from the journal's
    # storage_class column. One of STANDARD, REDUCED_REDUNDANCY, STANDARD_IA,
    # ONEZONE_IA, INTELLIGENT_TIERING, GLACIER, DEEP_ARCHIVE, GLACIER_IR, or
    # None when the journal did not populate the column.
    #
    # Deliberately absent from logical_operation_id below, and that omission
    # is load-bearing rather than incidental. A lifecycle transition into an
    # archived storage class writes its own UPDATE_METADATA record, and
    # because a transition changes neither the key, the version, nor the
    # tags, that record carries an identity identical to the earlier tagging
    # record it follows. Keeping storage_class out of the identity is what
    # lets the two collapse in journal_dedup, where the transition record
    # wins on sequence_number and so becomes the record the archived-object
    # filter sees. Including it here would split them into two identities,
    # the pre-transition record would survive alongside the transition, and
    # an object known to be archived would still reach a manifest.
    storage_class: str | None = None

    @property
    def logical_operation_id(self) -> str:
        """Stable dedup identity for this logical tagging operation.

        Computed as ``source_bucket + object_key + version + digest``, joined by
        ``\\x00``, where ``version`` is ``operation_version`` or the
        ``_NULL_VERSION`` sentinel, and ``digest`` is a truncated SHA-256 of the
        canonical JSON rendering of ``resulting_tag_set``.

        The tag set enters the identity unconditionally, so a second tagging
        event on the same object version is a distinct operation.  The journal's
        ``version_id`` column is an S3 object version, not a per-mutation token:
        ``PutObjectTagging`` creates no new version, so without the tag digest
        every tagging event on one version would collapse to one identity.

        The digest is hashed rather than embedded because ``processed_window``
        entries are serialized into a single S3 state object and tag values are
        operator-controlled; 16 hex characters is fixed-width.  Canonical JSON
        keeps the identity independent of tag ordering.

        Two records sharing the same ``logical_operation_id`` are duplicate
        deliveries of the same event (at-least-once journal delivery).
        """
        version = self.operation_version if self.operation_version is not None else _NULL_VERSION
        tag_repr = json.dumps(self.resulting_tag_set, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(tag_repr.encode("utf-8")).hexdigest()[:16]
        return "\x00".join([self.source_bucket, self.object_key, version, digest])


# ---------------------------------------------------------------------------
# MatchedObject
# ---------------------------------------------------------------------------


@dataclass(eq=False)
class MatchedObject:
    """An object whose resulting tag set and key satisfy at least one derived rule.

    Identity (for set membership and dedup) is
    ``(source_bucket, object_key, replication_config_id)`` (5.5, 6.3).
    ``version_id`` is NOT part of identity — it is threaded through to the
    manifest so S3 Batch Operations can replicate the correct object version.
    """

    source_bucket: str
    object_key: str
    replication_config_id: str
    matched_rule_ids: frozenset[str]  # rules that were satisfied (5.5)
    version_id: str | None = None  # version ID from journal; not part of identity
    tagged_at: datetime | None = None  # tag event timestamp from journal
    last_modified: datetime | None = None  # object last-modified from journal
    # Destination bucket ARNs of the rules in ``matched_rule_ids``, taken from
    # the replication configuration. Reported to the operator so a
    # Completion_Report names where each object was bound for; never used to
    # make a destination-side call (12.2, 13.1).
    destination_bucket_arns: frozenset[str] = frozenset()

    # Two MatchedObject instances are *the same object* if they share the
    # identity triple, regardless of which rule IDs were matched or version_id.
    # This is the dedup key used by the Manifest_Generator (6.3).

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MatchedObject):
            return NotImplemented
        return (
            self.source_bucket == other.source_bucket
            and self.object_key == other.object_key
            and self.replication_config_id == other.replication_config_id
        )

    def __hash__(self) -> int:
        return hash((self.source_bucket, self.object_key, self.replication_config_id))


# ---------------------------------------------------------------------------
# ManifestEntry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ManifestEntry:
    """A single row in an S3 Batch Operations manifest.

    Serialized as ``"source_bucket,object_key,version_id"``, always three
    fields (6.4); a ``None`` version_id is emitted as the literal ``null``.

    The ``object_key`` is URL-encoded (percent-encoding, ``/`` preserved) on
    serialization. S3 Batch Operations requires manifest object keys to be
    URL-encoded, and encoding also neutralizes commas and newlines in keys —
    both of which are legal in S3 object keys and would otherwise corrupt the
    CSV or allow row injection by an actor who controls an object key.

    ``error_code`` is not part of the outbound manifest row — it exists
    solely so this same type can also represent one row of an S3 Batch
    Operations BOPS_Completion_Report (``bops_report_reader``), which
    carries a per-task ``ErrorCode`` column the outbound manifest has no
    equivalent for. It defaults to ``None`` and is ignored by
    :meth:`to_csv_row` and :meth:`from_versioned_csv_row`, so it never
    affects the outbound-manifest encode/decode contract.

    ``task_status``, ``http_status_code``, and ``result_message`` are the
    report's three remaining columns, retained for the same reason. Unlike
    ``error_code`` they are declared ``compare=False``, so they take no part
    in equality or hashing: they are diagnostic detail about one task
    attempt, not part of an entry's identity, and instances of this type are
    placed in sets and used as dict keys on the manifest path. Letting a
    differing ``ResultMessage`` make two otherwise identical entries unequal
    would silently change that path's dedup semantics.

    ``result_message`` is free-form service text. It is emitted into log
    entries but never parsed or matched against, since AWS documents no
    stable format for it.
    """

    source_bucket: str
    object_key: str
    version_id: str | None = None  # included in manifest when present
    error_code: str | None = None  # BOPS_Completion_Report ErrorCode column, when parsed from one; unused for outbound manifests
    task_status: str | None = field(default=None, compare=False)       # report TaskStatus column, e.g. "succeeded" / "failed"
    http_status_code: str | None = field(default=None, compare=False)  # report HTTPStatusCode column, as the verbatim string
    result_message: str | None = field(default=None, compare=False)    # report ResultMessage column; free-form service text

    def to_csv_row(self) -> str:
        """Return the CSV row as required by S3 Batch Operations (6.4, 13.6).

        Always emits a 3-column row ``source-bucket,object-key,version-id``.
        When ``version_id`` is ``None``, the literal string ``null`` is emitted
        as the third field (e.g. ``source-bucket,object-key,null``). Two
        independent, both live-verified, failure modes motivate emitting the
        literal string ``null`` rather than an empty field:

        1. Omitting the third field entirely for a null-version entry in a
           manifest that declares 3 fields produces a row with fewer
           columns than every other row, which S3 Batch Operations rejects
           outright with
           ``InvalidManifestContent: Unexpected number of task fields``
           (this is the in-memory-generation-path counterpart of the same
           defect fixed in ``unload_generator.py``'s UNLOAD SQL
           projection).
        2. Even with the field present but empty (``source-bucket,key,``),
           S3 Batch Operations parses the manifest successfully but fails
           that individual task with
           ``SrcObjectNotFound: Object versionID is invalid`` — an empty
           string is not recognized as "the null version". AWS's own
           documentation for S3 Inventory report manifests confirms the
           correct representation is the literal string ``null``
           ("Converting empty version ID strings in Amazon S3 Inventory
           reports to null strings":
           ``CASE WHEN version_id = '' THEN 'null' ELSE version_id END``).

        The object key is percent-encoded (``/`` preserved) so that commas,
        newlines, and other special characters in the key cannot break the
        CSV row structure or allow row injection. S3 Batch Operations
        URL-decodes the key when it reads the manifest.
        """
        key = quote(self.object_key, safe="/")
        if self.version_id is not None:
            return f"{self.source_bucket},{key},{self.version_id}"
        return f"{self.source_bucket},{key},null"

    @classmethod
    def from_versioned_csv_row(cls, row: str) -> ManifestEntry:
        """Parse a 3-column CSV row (bucket, key, version_id).

        Uses rpartition to split off the version_id and partition to split the
        bucket from the (URL-encoded) key, then URL-decodes the key. Because
        encoded keys never contain a literal comma, the split is unambiguous.
        Both an empty version_id field and the literal string ``null``
        (the form :meth:`to_csv_row` now emits for a null-version object,
        matching the convention S3 Batch Operations itself requires — see
        :meth:`to_csv_row`'s docstring) are returned as ``version_id=None``,
        so round-tripping either representation is lossless.
        """
        remainder, _, version_id_raw = row.rpartition(",")
        bucket, _, key = remainder.partition(",")
        version_id = None if version_id_raw in ("", "null") else version_id_raw
        return cls(
            source_bucket=bucket,
            object_key=unquote(key),
            version_id=version_id,
        )


@dataclass
class S3Location:
    """The scratch-bucket location where a manifest object is stored."""

    bucket: str
    key: str


# ---------------------------------------------------------------------------
# CheckpointState / Lease
# ---------------------------------------------------------------------------


class LeaseStatus(Enum):
    """Status values for an in-flight lease (9.4)."""

    IN_FLIGHT = "IN_FLIGHT"


@dataclass(frozen=True)
class ProcessedRef:
    """A logical operation whose object reached the written manifest of a
    successfully submitted job, retained while its watermark is still within the
    lookback window.

    The Journal_Monitor re-scans a lookback window of the journal each run to
    catch records that arrived late (S3 Metadata is eventually consistent).
    Without a memory of what was already replicated, re-scanning would re-submit
    those objects every interval.  ``ProcessedRef`` is that memory: an op whose
    ``logical_operation_id`` appears here is suppressed on re-scan, so the
    lookback never causes redundant replication (9.x).  An operation that
    matched no rule, or that the Deleted_Version_Filter excluded, never reaches
    a manifest and so is never recorded here.  Entries whose
    ``watermark`` falls below ``current_watermark - lookback`` are pruned, so
    the set stays bounded by the lookback window.
    """

    logical_operation_id: str
    watermark: str  # canonical watermark of the op's record_timestamp


@dataclass
class Lease:
    """An in-flight concurrency lease embedded in the per-bucket CheckpointState.

    Cleared on successful confirmation or failure (9.4).
    """

    lease_id: str
    candidate_max_watermark: str  # canonical record_timestamp high-water mark being submitted this run
    acquired_at: datetime
    status: LeaseStatus = field(default=LeaseStatus.IN_FLIGHT)


@dataclass
class CheckpointState:
    """Per-Monitored_Bucket durable state serialized into a single S3 state object.

    The object's ETag is the concurrency token for all conditional writes (4.3, 9.4).

    The checkpoint is a ``record_timestamp`` watermark (canonical UTC string),
    not a ``sequence_number``: S3 documents ``sequence_number`` ordering only
    per (bucket, key), whereas ``record_timestamp`` is globally comparable and
    therefore safe to use as a single cross-key cursor (4.3, 9.1).
    """

    source_bucket: str
    last_processed_watermark: str  # canonical record_timestamp; advanced only on successful submission (4.3, 9.1)
    lease: Lease | None = None  # embedded in the same state object (9.4)
    processed_window: list[ProcessedRef] = field(default_factory=list)  # late-arrival dedup memory (9.x)


# ---------------------------------------------------------------------------
# SubmissionRecord
# ---------------------------------------------------------------------------


class FailureClass(Enum):
    """Classification of a submission failure's root cause.

    Carried on ``SubmissionResult.failure_class`` when the status is
    ``CREATE_FAILED``.  The orchestrator uses this to decide escalation:
    only ``PERMANENT_CLIENT`` counts toward the disable threshold and
    triggers an immediate alert, because it represents a code defect in
    the Solution that will never self-heal.

    Requirements: submission-failure-visibility 2.1, 2.2
    """

    PERMANENT_CLIENT = "PERMANENT_CLIENT"   # botocore rejected the request (ParamValidationError)
    SERVICE = "SERVICE"                     # AWS returned an error response (ClientError)
    TIMEOUT = "TIMEOUT"                     # call exceeded the timeout budget
    UNKNOWN = "UNKNOWN"                     # anything else


class SubmissionStatus(Enum):
    """Terminal and in-progress statuses for a Batch_Replication_Job submission."""

    SUBMITTED = "SUBMITTED"
    CREATE_FAILED = "CREATE_FAILED"
    SUBMIT_FAILED = "SUBMIT_FAILED"
    SKIPPED = "SKIPPED"  # manifest absent or empty — no job created (7.5)


@dataclass
class SubmissionRecord:
    """A record of a Batch_Replication_Job submission attempt (7.4, 7.6, 7.7, 10.x).

    ``watermark_low`` and ``watermark_high`` are set at submission time so that
    a subsequent run can roll the checkpoint back to ``watermark_low`` when the
    job is found to have Failed or Cancelled, re-admitting the affected operations
    without persisting a new state write.  They default to empty strings for
    backward compatibility with records that pre-date these fields.

    Keying note (design.md Decision D3): a bucket now has a single
    ``SubmissionRecord`` (one Batch Operations job per bucket per interval).
    ``StateStore.record_submission`` stores it in the persisted
    ``submission_records`` dict keyed by the per-bucket sentinel
    (``source_bucket``) rather than by ``replication_config_id``. This field
    keeps its name and value unchanged — it still identifies the rule/config
    whose derivation produced the union manifest's role — only the dict *key*
    used to store the record changed. No other shape change.

    Requirements: 4.1, 4.5, 2.2, 2.4
    """

    replication_config_id: str
    source_bucket: str
    job_id: str
    manifest_key: str
    submitted_at: datetime
    status: SubmissionStatus
    watermark_low: str = ""   # bucket watermark before this run — resume point on failure
    watermark_high: str = ""  # candidate_hwm for this run — latest watermark attempted
    consecutive_failures: int = 0  # consecutive Failed/Cancelled jobs for this config; reset to 0 on Complete
    report_diagnosed: bool = False  # per-job; resets when next submission replaces the record


# ---------------------------------------------------------------------------
# CloudWatch Metrics models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BucketMetrics:
    """Per-bucket processing counters produced by one Processing_Interval.

    Carried from the orchestrator to the Metrics_Publisher after the
    per-bucket loop completes (Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 7.3).

    Attributes
    ----------
    source_bucket:
        Name of the Monitored_Bucket these counts apply to.
    ops_read:
        Count of distinct journal operations forwarded to matching
        (raw records minus duplicates).
    matched:
        Count of Matched_Object entries accumulated.
    submitted:
        Count of successful Batch_Replication_Job submissions (0 or 1).
    errored:
        True when the bucket was skipped due to a genuine processing error
        (client creation failure, checkpoint read failure, fatal journal
        error, or lease acquisition failure).  False for a normal skip
        (no tag-scoped rules) or a clean run.
    archived_excluded:
        Count of tagged objects dropped by the Archived_Object_Filter because
        they are in an archived storage class S3 will not replicate.  Defaults
        to zero so a caller constructing this without the field still gets
        valid metrics.
    """

    source_bucket: str
    ops_read: int
    matched: int
    submitted: int
    errored: bool
    archived_excluded: int = 0


@dataclass(frozen=True)
class RunResult:
    """Aggregated outcome of one Processing_Interval passed to the Metrics_Publisher.

    Requirements: 3.1, 7.3

    Attributes
    ----------
    buckets:
        One BucketMetrics entry per Monitored_Bucket processed in the run.
    disabled_buckets:
        Run-level count of Monitored_Buckets skipped because their
        ``disabled`` flag is set in Solution_Config. A disabled bucket
        produces no ``BucketMetrics`` entry at all (it is skipped before the
        per-bucket counters exist), so without this count an auto-disabled
        bucket — replication silently stopped, awaiting a manual re-enable —
        would be visible only in logs and in the one-shot notification email.
    """

    buckets: list[BucketMetrics]
    disabled_buckets: int = 0


@dataclass(frozen=True)
class RunOutcome:
    """Aggregated capped/progressed signal from one Processing_Interval,
    returned by ``orchestrator.run_interval`` and consumed by the
    Self_Reinvocation decision (``should_reinvoke``).

    Extends the per-bucket information ``run_interval`` already gathers
    (``BucketMetrics``) with the one additional signal the reinvocation
    decision needs: whether *any* bucket in this run both hit the
    Journal_Read_Row_Cap (a Capped_Run) and progressed (a
    Batch_Replication_Job was submitted and the checkpoint advanced).

    Requirements: 4.1, 4.2, 4.3

    Attributes
    ----------
    any_capped_and_progressed:
        True iff at least one Monitored_Bucket processed in this run was
        both a Capped_Run and progressed. False when no bucket capped, or
        when every capped bucket failed to progress (generation,
        submission, or checkpoint advance failed) — so a failing run can
        never trigger Self_Reinvocation.
    buckets:
        One BucketMetrics entry per Monitored_Bucket processed in the run
        (identical to ``RunResult.buckets``).
    """

    any_capped_and_progressed: bool
    buckets: list[BucketMetrics]


# ---------------------------------------------------------------------------
# CompletionState / ConfigContext / TrackedObject
# ---------------------------------------------------------------------------
#
# Per-object aggregate (Tracked_Object) model — source-status-only,
# BOPS-completion-report-gated. Because x-amz-replication-status is a single
# aggregate value per source object version (COMPLETED only once every
# destination is done, PENDING if any is outstanding, FAILED if any failed),
# a Tracked_Object carries ONE outcome, not one per destination. A
# ConfigContext per replication_config_id records only which
# Batch_Replication_Job covered the object for that rule and whether that
# job's BOPS_Completion_Report has confirmed it — it carries no outcome of
# its own. See design.md Decision 2.


class CompletionState(Enum):
    """Per-Tracked_Object lifecycle value."""

    PENDING = "PENDING"
    RESOLVED = "RESOLVED"


@dataclass
class ConfigContext:
    """Per-(matching replication_config_id) context for a Tracked_Object:
    which BOPS job covered this object for that config, that job's
    manifest-generation time (for the Quiescence_Check), and whether that
    config's BOPS job has been confirmed terminal AND its completion report
    lists this object version (the gating signal for the
    Source_Status_Check — see design.md Decision 3)."""

    replication_config_id: str
    job_id: str                         # the Batch_Replication_Job that covered this config
    manifest_generated_at: datetime     # Job.CreationTime — used by the Quiescence_Check
    bops_confirmed: bool = False


@dataclass
class TrackedObject:
    """The completion-tracking unit.

    Identity is (source_bucket, object_key, version_id). Carries a single
    aggregate `state` and `replication_outcome` — never one per destination
    — because the Source_Replication_Status_Header reflects the source
    object version as a whole across every destination.
    """

    source_bucket: str
    object_key: str
    version_id: str | None           # None == the null-version marker
    configs: dict[str, ConfigContext]   # keyed by replication_config_id
    state: CompletionState = CompletionState.PENDING
    resolved_at: datetime | None = None
    resolution_method: str | None = None   # "source_status_header" | None while PENDING
    # "COMPLETE" (header read COMPLETED) | verbatim x-amz-replication-status
    # value "PENDING" | "FAILED" | "UNKNOWN" (header absent) | None while
    # PENDING.
    #
    # NOTE: the string "PENDING" is a legal replication_outcome value (a
    # source object whose header itself reads "PENDING") and is unrelated to
    # CompletionState.PENDING. Check-candidate selection filters exclusively
    # on `state`, never on `replication_outcome` — see Property 5.
    replication_outcome: str | None = None
    tagged_at: datetime | None = None       # tag event timestamp from journal
    last_modified: datetime | None = None   # object last-modified from journal
    # Replication rule IDs that matched this object, and the destination
    # buckets those rules target. Reported so an operator can see which rules
    # fired and where the object was bound for.
    #
    # These describe *intent*, not per-destination outcome: the source
    # object's x-amz-replication-status header is a single aggregate across
    # every destination (COMPLETED only when all succeed, FAILED when one or
    # more fail), so `replication_outcome` cannot be attributed to an
    # individual entry in `destinations`.
    matched_rules: frozenset[str] = frozenset()
    destinations: frozenset[str] = frozenset()


@dataclass
class ScanState:
    """Latest tag-matching scan result recorded for one replication_config_id.

    Used exclusively by the Quiescence_Check (design Decision 6) to decide
    whether a Completion_Report may be published: a job's report is deferred
    until a scan recorded strictly after that job's ``manifest_generated_at``
    finds zero new Matched_Object entries for the same ``replication_config_id``.
    """

    last_scan_at: datetime
    last_scan_match_count: int
