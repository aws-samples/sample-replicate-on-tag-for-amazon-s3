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
# Null-version markers
# ---------------------------------------------------------------------------
# The three spellings of "the null version" that can appear in the VersionId
# position of a manifest row or a Batch Operations completion report row. All
# of them mean ``version_id=None`` to this Solution.
#
# The completion report's VersionId column is a round-trip of whatever the
# manifest supplied, which is why more than one spelling reaches us:
#
#   ``null``  A correctly serialized null version, the form
#             :meth:`ManifestEntry.to_csv_row` emits and the form AWS's own
#             S3 Inventory guidance prescribes. Observed echoed back verbatim
#             in the report for job b2f9f42d-dba4-4a77-945e-074cef95450e
#             (2026-08-27), where both null-version tasks succeeded and the
#             source objects reached ``ReplicationStatus: COMPLETED``.
#   ``\N``    What the report writes when the manifest supplied nothing to
#             echo, i.e. an empty VersionId field. That manifest row always
#             fails with ``SrcObjectNotFound: Object versionID is invalid``,
#             so this spelling only appears on a failed row. Observed in job
#             0f65a1b7-9b4c-4124-a1ad-06ea77d7224f (2026-07-21), before
#             ``to_csv_row`` was changed to emit the literal ``null``.
#   ``""``    An unversioned-bucket row.
#
# Matching is exact and case-sensitive on purpose: S3 version IDs are
# case-sensitive, so a real version whose value happens to read ``NULL`` is a
# different object from the null version.
#
# Shared by :meth:`ManifestEntry.from_versioned_csv_row` and the completion
# report reader so the inbound and outbound halves cannot drift apart.
NULL_VERSION_TOKENS = ("", "null", "\\N")


def normalize_version_id(raw: str | None) -> str | None:
    """Return ``None`` for any null-version spelling, else *raw* unchanged."""
    if raw is None or raw in NULL_VERSION_TOKENS:
        return None
    return raw

# ---------------------------------------------------------------------------
# AppConfig / MonitoredBucket
# ---------------------------------------------------------------------------


@dataclass
class MonitoredBucket:
    """A source S3 bucket the customer wants the Solution to watch."""

    name: str    # must satisfy S3 bucket naming rules; required (1.2, 2.2)
    region: str  # non-empty, valid AWS region; required (1.2, 13.3, 13.8)
    # No tag_filter and no destination permitted (1.5)
    # Whether a bucket is disabled is NOT carried here: it is per-bucket
    # runtime state, held in that bucket's state object and read via
    # StateStore.get_disable_state (see BucketDisableState for why).


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
        Every spelling in :data:`NULL_VERSION_TOKENS` is returned as
        ``version_id=None``, so round-tripping any of them is lossless. That
        includes the literal string ``null``, the form :meth:`to_csv_row` emits
        for a null-version object, matching the convention S3 Batch Operations
        itself requires — see :meth:`to_csv_row`'s docstring.
        """
        remainder, _, version_id_raw = row.rpartition(",")
        bucket, _, key = remainder.partition(",")
        version_id = normalize_version_id(version_id_raw)
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


@dataclass(frozen=True)
class BucketDisableState:
    """Whether a Monitored_Bucket is disabled, read from its state object.

    Persisted as sibling top-level keys in ``state/<source_bucket>.json``
    alongside ``CheckpointState`` rather than as fields *of* it, and in the
    state object rather than in Solution_Config. Three reasons, each of which
    rules out an alternative that looks simpler:

    * **Not in Solution_Config.** The config custom resource rewrites that
      object wholesale from template parameters on every stack create/update
      and has no notion of this flag, so a disable stored there is cleared by
      an unrelated deploy — re-enabling a bucket whose jobs keep failing
      without anyone deciding to. The state object is only ever seeded with
      ``If-None-Match: *``, so a disable recorded here survives every deploy.
    * **Not a ``CheckpointState`` field.**
      :func:`~src.core.checkpoint_logic.advance_checkpoint` builds a fresh
      ``CheckpointState`` from an explicit field list on every lease release,
      so a field added there would reset to its default at the end of every
      interval. As sibling keys, ``put_checkpoint``'s overlay preserves them
      untouched, the same way it preserves ``submission_records``.
    * **Read without ``deserialize``.** A disabled bucket must not become an
      enabled one because its watermark was corrupted, and ``deserialize``
      raises on an implausible ``last_processed_watermark``.
      :meth:`~src.adapters.state_store.StateStore.get_disable_state` therefore
      reads the raw payload, as ``get_submission_records`` does.

    An absent ``disabled`` key and an explicit ``false`` both mean enabled, so
    the documented recovery step (setting ``"disabled": false``) and deleting
    the key outright are equivalent.
    """

    disabled: bool = False
    reason: str = ""
    at: str = ""  # ISO 8601 timestamp recorded when the bucket was disabled


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
    report_diagnosed: bool = False  # per-job; its report's task errors have been logged
    # Per-job: this job's terminal outcome has already been folded into the
    # bucket's recovery arithmetic, so it must not be folded in again.
    #
    # Needed because a record now outlives the run in which its job reached
    # terminal. Requirement 3.2 keeps a terminal record whose completion report
    # has not been read, so that check_report_handler can alert on it. Without
    # this flag such a record is re-scored on every subsequent run: the watermark
    # rolls back to the same watermark_low, the same journal range is resubmitted
    # at a fresh per-job charge, and consecutive_failures climbs until the circuit
    # breaker disables the bucket with a reason claiming N consecutive failures for
    # one job that failed once. Under 1.0.1's single overwritten record this could
    # not happen, because the record was replaced before the next run read it.
    #
    # Distinct from report_diagnosed on purpose: a job whose report is unreadable
    # is scored (its status is known) but not diagnosed (its task errors are not),
    # which is exactly the case that makes re-scoring reachable.
    recovery_scored: bool = False


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
    submission_deferred:
        True when the bucket was skipped because its previous Batch Operations
        job had not finished. Not an error, and distinct from ``errored``: the
        run did what it should, and the tagging it did not submit stays eligible
        for the next run. Surfaced as a metric so an operator can see a bucket
        whose replication throughput is the limiting factor rather than
        wondering why no job was submitted.
    tail_shortened:
        True when the run raised its journal-read lower bound above
        ``last_processed_watermark - JournalLookbackSeconds`` because the
        lookback tail would not fit the row budget. Not an error: the run read
        every row it could afford and made progress. Surfaced as a metric
        because it is the one condition under which the Solution re-scans less
        of the lookback window than configured, so late-arrival tolerance is
        reduced for as long as the backlog lasts.
    """

    source_bucket: str
    ops_read: int
    matched: int
    submitted: int
    errored: bool
    archived_excluded: int = 0
    submission_deferred: bool = False
    tail_shortened: bool = False


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
        ``disabled`` flag is set in their state object. A disabled bucket
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
# Per-object aggregate (Tracked_Object) model. A ready S3 Batch Operations
# completion report provides one aggregate task outcome for an object, rather
# than an outcome per destination. A ConfigContext records the
# Batch_Replication_Job that covered the object; it carries no outcome of its
# own. See the report-derived-completion design, Decision 2.


class CompletionState(Enum):
    """Per-Tracked_Object lifecycle value.

    ``PENDING`` is retained so a state object written by 1.0.1 stays
    deserializable. Nothing in 1.1.0 produces it, and nothing can advance it:
    the source-object check that used to resolve a ``PENDING`` item is gone. It
    also remains the dataclass default for :class:`TrackedObject`, which
    ``merge_completion_report`` constructs transiently before setting
    ``RESOLVED``.

    An item deserialized in this state is normalized to ``RESOLVED``/``UNKNOWN``
    in memory by ``completion_tracker.resolve_legacy_item`` when the publish
    phase evaluates it, so it drains rather than accumulating.
    """

    PENDING = "PENDING"
    RESOLVED = "RESOLVED"


@dataclass
class ConfigContext:
    """Per-replication-config context for a tracked object.

    ``bops_confirmed`` is retained only in serialized state so a 1.1.0 state
    object remains readable after a rollback to 1.0.1. Report-derived contexts
    write it as ``True``. Completion behavior does not inspect the field.
    """

    replication_config_id: str
    job_id: str                         # the Batch_Replication_Job that covered this config
    manifest_generated_at: datetime     # Job.CreationTime — used by the Quiescence_Check
    bops_confirmed: bool = True


@dataclass
class TrackedObject:
    """The completion-tracking unit.

    Identity is (source_bucket, object_key, version_id). Carries one aggregate
    ``state`` and ``replication_outcome`` from the S3 Batch Operations
    completion-report row for the task. The row does not identify a specific
    destination when the replication configuration has more than one.
    """

    source_bucket: str
    object_key: str
    version_id: str | None           # None == the null-version marker
    configs: dict[str, ConfigContext]   # keyed by replication_config_id
    state: CompletionState = CompletionState.PENDING
    resolved_at: datetime | None = None
    resolution_method: str | None = None
    # Newly resolved report rows use only "COMPLETE", "FAILED", or "UNKNOWN".
    # The 1.0.1 values "PENDING", "GONE", and "EXPIRED" remain deserializable so
    # a state object written by 1.0.1 stays readable; the publish phase
    # normalizes them to "UNKNOWN" in memory via
    # completion_tracker.resolve_legacy_item.
    replication_outcome: str | None = None
    tagged_at: datetime | None = None       # tag event timestamp from journal
    last_modified: datetime | None = None   # object last-modified from journal
    # Replication rule IDs that matched this object, and the destination
    # buckets those rules target. Reported so an operator can see which rules
    # fired and where the object was bound for.
    #
    # These describe *intent*, not per-destination outcome: the Batch
    # Operations completion-report row is the aggregate result for a task and
    # cannot identify which destination failed when multiple destinations are
    # configured.
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
