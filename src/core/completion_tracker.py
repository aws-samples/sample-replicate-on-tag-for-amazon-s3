"""Pure completion-tracking resolution logic (Completion_Tracker).

No AWS calls, no I/O — mirrors the pure-core style of ``checkpoint_logic.py``
and ``journal_dedup.py``. See
``.kiro/specs/source-status-completion-tracking/design.md`` for the full
design (Decisions 2, 3, 5, 6, 7, 9).

This module implements the source-side-only, BOPS-report-gated, per-object
(``TrackedObject``) completion-tracking design. There is no ``CheckKind``,
no age gate, no ``Source_Status_Threshold``, no ``confirm_presence`` closure,
and no destination call anywhere in this module.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from src.adapters.source_status_adapter import SourceStatusCheckKind, SourceStatusResult
from src.core.models import CompletionState, ConfigContext, ScanState, TrackedObject

if TYPE_CHECKING:
    from src.core.models import ManifestEntry


# ---------------------------------------------------------------------------
# Creation — Config_Context mapping (Decision 2, 5)
# ---------------------------------------------------------------------------


def create_pending_tracked_object_updates(
    entries: list[ManifestEntry],
    replication_config_id: str,
    job_id: str,
    manifest_generated_at: datetime,
) -> dict[tuple[str, str | None], ConfigContext]:
    """Build the per-entry new ``ConfigContext`` updates for a terminal job's
    BOPS_Completion_Report.

    For each ``ManifestEntry`` in ``entries`` (the parsed rows of a terminal
    job's BOPS_Completion_Report, not the manifest itself), produces one new
    ``ConfigContext(replication_config_id, job_id, manifest_generated_at,
    bops_confirmed=True)``, keyed by item identity ``(object_key,
    version_id)`` — ``version_id`` passes through verbatim, including
    ``None`` for the null-version marker (unversioned bucket); it is never
    coerced to an empty string or other placeholder (Requirement 2.3).

    This function does NOT create or merge the ``TrackedObject`` itself —
    only the state store can see whether a ``TrackedObject`` for a given
    identity already exists (created earlier by a sibling rule's job, or to
    be extended later by another rule's job — Requirement 2.6); that merge
    is the state store's responsibility (``StateStore.merge_completion_configs``).

    This function takes no status parameter and performs no I/O — it is
    usable for any terminal ``DescribeJob`` status (``Complete``, ``Failed``,
    or ``Cancelled``), per Requirement 2.5; the caller (the orchestrator's
    per-config ``DescribeJob`` loop) decides *when* to invoke it, and is also
    responsible for the ``job_id not in completion_processed_job_ids``
    idempotency gate (design.md Decision 2) — this function does not check
    or know about that gate either.

    This function produces ``ConfigContext``s only — there is no per-config
    outcome or state, since resolution is object-level.

    Requirements: 2.1, 2.2, 2.3, 2.5, 2.6, 2.7
    """
    updates: dict[tuple[str, str | None], ConfigContext] = {}
    for entry in entries:
        updates[(entry.object_key, entry.version_id)] = ConfigContext(
            replication_config_id=replication_config_id,
            job_id=job_id,
            manifest_generated_at=manifest_generated_at,
            bops_confirmed=True,
        )
    return updates


# ---------------------------------------------------------------------------
# Resolution — reconcile_source_status_check (Decision 3)
# ---------------------------------------------------------------------------


def _resolve(obj: TrackedObject, resolution_method: str, outcome: str, now: datetime) -> TrackedObject:
    """Return a new ``TrackedObject`` transitioned to ``RESOLVED``.

    Carries ``source_bucket``, ``object_key``, ``version_id``, and
    ``configs`` through verbatim from ``obj`` — those never change on
    resolution.
    """
    return TrackedObject(
        source_bucket=obj.source_bucket,
        object_key=obj.object_key,
        version_id=obj.version_id,
        configs=obj.configs,
        state=CompletionState.RESOLVED,
        resolved_at=now,
        resolution_method=resolution_method,
        replication_outcome=outcome,
    )


def _unchanged(obj: TrackedObject) -> TrackedObject:
    """Return a new ``TrackedObject`` with every field identical to ``obj``
    — still whatever state it was in — to be retried at the next
    Completion_Poll_Interval.

    Mirrors ``checkpoint_logic.py::advance_checkpoint``'s convention of
    returning a new object rather than mutating the input in place.
    """
    return TrackedObject(
        source_bucket=obj.source_bucket,
        object_key=obj.object_key,
        version_id=obj.version_id,
        configs=obj.configs,
        state=obj.state,
        resolved_at=obj.resolved_at,
        resolution_method=obj.resolution_method,
        replication_outcome=obj.replication_outcome,
    )


def reconcile_source_status_check(
    obj: TrackedObject,
    result: SourceStatusResult,
    now: datetime,
) -> TrackedObject:
    """Return the reconciled ``TrackedObject`` after a Source_Status_Check.

    Operates on a single ``TrackedObject`` (object-level state). There is no
    ``confirm_presence`` closure parameter and no
    ``destination_access_configured`` branch — a ``COMPLETED`` header
    resolves DIRECTLY to ``COMPLETE``, and no destination call is ever made
    from this function (design.md Decision 3).

    Branches on ``result.kind``:

    * ``CHECK_FAILED`` — the ``HeadObject`` call itself failed transiently.
      Returns the object unchanged, still in its prior state (``PENDING``),
      to be retried at the next Completion_Poll_Interval (Requirement 3.6).
    * ``HEADER_ABSENT`` — the call succeeded but the source object version
      carried no ``x-amz-replication-status`` header. Returns a new object
      transitioned to ``RESOLVED`` with
      ``resolution_method="source_status_header"``,
      ``replication_outcome="UNKNOWN"`` (Requirement 3.5). The accompanying
      key-free error indication (naming only ``job_id``/
      ``replication_config_id``) is the orchestrator's responsibility, not
      this pure function's.
    * ``HEADER_VALUE`` with ``result.value == "COMPLETED"`` — resolves
      DIRECTLY to ``RESOLVED``/``COMPLETE`` (Requirement 3.2). No
      destination call is made.
    * ``HEADER_VALUE`` with ``result.value in ("PENDING", "FAILED")`` — the
      header value is immediately terminal and recorded verbatim (Requirement
      3.4).

    Requirements: 3.2, 3.4, 3.5, 3.6
    """
    if result.kind is SourceStatusCheckKind.CHECK_FAILED:
        return _unchanged(obj)

    if result.kind is SourceStatusCheckKind.OBJECT_GONE:
        # Terminal: the object version no longer exists, so no later check can
        # resolve it. Resolving to GONE releases the Tracked_Object through the
        # normal publish-then-delete path instead of retrying forever
        # (Requirement 3.7).
        return _resolve(obj, "object_gone", "GONE", now)

    if result.kind is SourceStatusCheckKind.HEADER_ABSENT:
        return _resolve(obj, "source_status_header", "UNKNOWN", now)

    # HEADER_VALUE.
    header_value = result.value
    if header_value == "COMPLETED":
        return _resolve(obj, "source_status_header", "COMPLETE", now)

    # "PENDING" or "FAILED" — verbatim.
    return _resolve(obj, "source_status_header", header_value, now)


# ---------------------------------------------------------------------------
# Tracked_Object expiry — the backstop for anything that never resolves
# ---------------------------------------------------------------------------


def tracked_object_age(obj: TrackedObject, now: datetime) -> timedelta:
    """Age of *obj* measured from the most recent job that covered it.

    Uses ``max(manifest_generated_at)`` across ``configs`` — the newest job —
    so an object re-covered by a later job gets a fresh window rather than
    inheriting the age of the first job that happened to include it.

    Returns a zero timedelta for an object with no configs, which cannot
    become a check candidate anyway (``select_check_candidates`` requires
    every config to be ``bops_confirmed``, and ``all(())`` of an empty dict
    admits it — so returning zero keeps such an object un-expirable rather
    than instantly expired).
    """
    if not obj.configs:
        return timedelta(0)
    newest = max(ctx.manifest_generated_at for ctx in obj.configs.values())
    return now - newest


def is_expired(obj: TrackedObject, now: datetime, ttl: timedelta) -> bool:
    """True iff *obj* has been tracked for longer than *ttl* without resolving.

    The backstop for a Tracked_Object that can never resolve on its own. The
    known case is a source object whose ``HeadObject`` fails permanently
    while presenting as transient (see ``source_status_adapter``'s note on
    403 masking), but the guarantee this provides is general: no
    Tracked_Object occupies the state object indefinitely, whatever the
    cause.

    A non-positive *ttl* disables expiry entirely.

    Requirements: 3.8
    """
    if ttl <= timedelta(0):
        return False
    return tracked_object_age(obj, now) > ttl


def expire_tracked_object(obj: TrackedObject, now: datetime) -> TrackedObject:
    """Resolve *obj* as ``EXPIRED`` so it leaves the state object (Req 3.8).

    Deliberately reuses the normal resolution path rather than deleting the
    object directly: the item is published in a Completion_Report and only
    then deleted, so an operator sees which objects were abandoned and why
    instead of them vanishing silently.
    """
    return _resolve(obj, "expired", "EXPIRED", now)


# ---------------------------------------------------------------------------
# Selection — select_check_candidates (Decision 3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckCandidate:
    """One ``TrackedObject`` selected for a Source_Status_Check this interval.

    Returned by ``select_check_candidates``. Carries exactly what the
    caller (``_run_completion_tracking_interval``) needs to look up which
    ``TrackedObject`` this refers to and issue the check for it.
    """

    item_key: str
    obj: TrackedObject


def select_check_candidates(
    items: dict[str, TrackedObject],
) -> list[CheckCandidate]:
    """Flatten every check-eligible ``TrackedObject`` in ``items`` into candidates.

    A ``TrackedObject`` is selected iff ``state == CompletionState.PENDING``
    AND every ``ConfigContext`` in ``configs`` has ``bops_confirmed ==
    True`` — a ``TrackedObject`` with any unconfirmed ``ConfigContext`` is
    skipped this interval, since the aggregate
    ``x-amz-replication-status`` header is not read until every routing job
    has been confirmed to have processed the object (Requirement 3.1).

    There is no ``CheckKind`` and no age-gate routing — every gated
    candidate is a Source_Status_Check candidate.

    ``RESOLVED`` objects are never included — enforced structurally by
    filtering exclusively on ``state``, never on ``replication_outcome``
    (the literal string ``"PENDING"`` is a legal ``replication_outcome``
    value and must never be confused with ``CompletionState.PENDING`` — see
    Property 5) (Requirement 3.3).

    Requirements: 3.1, 3.3
    """
    candidates: list[CheckCandidate] = []
    for item_key, obj in items.items():
        if obj.state != CompletionState.PENDING:
            continue
        if not all(ctx.bops_confirmed for ctx in obj.configs.values()):
            continue
        candidates.append(CheckCandidate(item_key=item_key, obj=obj))
    return candidates


# ---------------------------------------------------------------------------
# Quiescence and publish gating (Decision 6)
# ---------------------------------------------------------------------------


def quiescence_check(
    manifest_generated_at: datetime,
    scan_state: ScanState | None,
) -> bool:
    """True iff a scan recorded AFTER ``manifest_generated_at`` found zero new
    matches (design.md Decision 6; keyed per-bucket per
    single-batch-job-per-bucket design.md D5).

    ``scan_state`` is already the single pre-selected value the caller
    (``should_publish``) passes in for that specific config. Under
    single-batch-job-per-bucket's design.md D5, the caller's per-object
    ``configs`` dict has exactly one entry, keyed by the per-bucket
    sentinel, so in practice this function is called at most once per
    object per interval, against the one ``ScanState`` recorded per bucket
    by ``StateStore.record_scan_result``.

    False when no such later scan has run yet (Requirement 5.1's "cycle
    immediately following manifest generation" has not occurred), or when
    the latest recorded scan (regardless of how much later) found >= 1 match
    (Requirement 5.2 — keeps deferring until a later evaluation finds zero).

    Requirements: 5.1, 5.2, 5.3
    """
    if scan_state is None or scan_state.last_scan_at <= manifest_generated_at:
        return False
    return scan_state.last_scan_match_count == 0


def should_publish(
    obj: TrackedObject,
    scan_state_by_config: dict[str, ScanState | None],
) -> bool:
    """True iff ``obj`` is ``RESOLVED`` AND every one of its routing configs
    is independently quiescent (design.md Decision 6).

    Resolution is a single object-level state (not per destination), so
    the conjunction is over ``configs`` for quiescence only.

    Per single-batch-job-per-bucket design.md D4/D5: since one Batch
    Operations job is submitted per bucket rather than per rule,
    ``merge_completion_configs`` creates exactly one ``ConfigContext`` per
    object, keyed by the per-bucket sentinel (the bucket's own name). This
    loop is therefore a *single* quiescence check against the per-bucket
    ``ScanState`` — not a true multi-config conjunction. The loop is left
    generic (iterating whatever ``configs`` holds) rather than special-cased
    to one entry.

    Edge case — an ``obj`` with an empty ``configs`` dict: the ``for`` loop
    trivially falls through and this function returns ``True`` (vacuous
    conjunction over an empty set) provided ``obj.state == RESOLVED``. This
    should not normally occur, since a ``TrackedObject`` is only ever
    created alongside at least one ``ConfigContext``.

    Args:
        obj: The ``TrackedObject`` to evaluate.
        scan_state_by_config: Mapping of ``replication_config_id ->
            Optional[ScanState]`` — e.g. the result of
            ``StateStore.get_scan_state``. Under current-design code this
            dict holds exactly one entry, keyed by the same per-bucket
            sentinel used as the key in ``obj.configs``, so the lookup
            below resolves to that single ``ScanState`` written by
            ``StateStore.record_scan_result`` for the bucket. A config_id
            absent from this mapping entirely is treated identically to an
            explicit ``None`` value (not quiescent).

    Requirements: 4.1, 4.4, 5.1, 5.2, 5.3, 5.4, 3.3
    """
    if obj.state != CompletionState.RESOLVED:
        return False
    for config_id, ctx in obj.configs.items():
        scan_state = scan_state_by_config.get(config_id)
        if not quiescence_check(ctx.manifest_generated_at, scan_state):
            return False
    return True


# ---------------------------------------------------------------------------
# Reporting — build_completion_report (Decision 7)
# ---------------------------------------------------------------------------


def build_completion_report(source_bucket: str, items: list[TrackedObject]) -> dict:
    """Build the JSON-serializable Completion_Report payload for a
    per-source_bucket batch (design.md Decision 7).

    Each item carries ONE aggregate ``outcome`` and ``destinations`` is a
    plain list of routing ``replication_config_id``s (context only — there
    is no per-destination outcome).

    * ``summary`` is a one-line, human-readable headline (e.g. "Completion
      Report for my-bucket: 3 object(s) processed. Outcomes: COMPLETE: 2,
      FAILED: 1.") — inserted as the FIRST key so it appears first when the
      dict is pretty-printed, letting an operator reading the raw
      notification (e.g. the SNS-to-email body) see the headline result
      without parsing JSON, while the message body remains strictly valid
      JSON for every other SNS subscriber protocol (SQS, Lambda, HTTPS).
    * ``source_bucket`` is copied verbatim from the parameter, not read off
      any individual item.
    * ``item_count`` is ``len(items)`` — counts Tracked_Objects.
    * ``outcome_counts`` counts ONE per Tracked_Object (the single aggregate
      outcome), not per destination. Only outcomes that actually occur
      appear as keys.
    * ``items`` has one entry per ``TrackedObject`` in ``items``, in the
      same order, with ``object_key``, ``version_id`` (``None`` passed
      through as JSON ``null``), ``outcome`` (the single aggregate
      ``replication_outcome``), and ``destinations`` (a plain list of the
      routing ``replication_config_id``s from ``obj.configs``, in
      ``configs`` dict iteration order — insertion order, Python 3.7+
      semantics).

    Under the one-job-per-bucket design (single-batch-job-per-bucket
    design.md Decision D4), ``obj.configs`` holds exactly one entry —
    keyed by the per-bucket sentinel (the bucket's own name) — for every
    ``TrackedObject``, since ``merge_completion_configs`` merges a single
    ``ConfigContext`` per object per bucket. Consequently ``destinations``
    for such an item is a single-element list, ``[bucket_name]``; it is
    context only (there is no per-destination outcome breakdown). This
    function's logic is generic over however many entries ``obj.configs``
    holds. The aggregate ``outcome`` field and ``outcome_counts`` are
    derived solely from ``obj.replication_outcome``, a single aggregate
    value per object that is not keyed by ``configs``.

    Requirements: 4.2, 4.3, 3.2
    """
    outcome_counts: dict[str, int] = {}
    report_items = []
    for obj in items:
        outcome_counts[obj.replication_outcome] = outcome_counts.get(obj.replication_outcome, 0) + 1
        report_items.append(_report_entry(obj))

    # "summary" is inserted FIRST (dict insertion order — Python 3.7+
    # semantics — is preserved through json.dumps) so a human reading the
    # raw, pretty-printed JSON message body (e.g. in an email) sees the
    # headline result as the first visible line, without needing a
    # separate non-JSON preamble that would break a non-email subscriber
    # (SQS, Lambda, HTTPS) expecting the message body to be valid JSON.
    return {
        "summary": _format_completion_report_summary(
            source_bucket, len(items), outcome_counts
        ),
        "source_bucket": source_bucket,
        "item_count": len(items),
        "outcome_counts": outcome_counts,
        "items": report_items,
    }


# ---------------------------------------------------------------------------
# Completion_Report chunking (SNS 256 KiB message limit)
# ---------------------------------------------------------------------------

# SNS rejects a Publish whose message body exceeds 256 KiB. The budget below
# is the usable allowance for report *items*; the remainder covers the
# envelope (``summary``, ``source_bucket``, ``item_count``,
# ``outcome_counts``) plus pretty-printing overhead. A rejected publish is
# not merely a lost notification: covered items are deleted only after a
# successful publish, so an oversized report would fail identically on every
# subsequent run and pin those items in the state object forever.
_SNS_MAX_MESSAGE_BYTES = 262_144
_REPORT_ITEM_BUDGET_BYTES = 240_000

# SNS rejects a Subject longer than 100 characters.
_SNS_MAX_SUBJECT_CHARS = 100

# Serialized length of an empty ``items`` wrapper, used to price one entry at
# the exact nesting depth (and therefore the exact indentation) it occupies in
# the real report body. Measuring an entry on its own at top level
# under-counts it by the added indentation of every line.
_EMPTY_ITEMS_WRAPPER_BYTES = len(json.dumps({"items": []}, indent=2))


def _report_entry(obj: TrackedObject) -> dict:
    """Build the one report entry for *obj*.

    Shared by :func:`build_completion_report` and :func:`chunk_items_for_report`
    so the size the chunker prices can never drift from the payload actually
    published.
    """
    entry = {
        "object_key": obj.object_key,
        "version_id": obj.version_id,
        "outcome": obj.replication_outcome,
        "destinations": list(obj.configs.keys()),
    }
    if obj.tagged_at is not None:
        entry["tagged_at"] = obj.tagged_at.isoformat()
    if obj.last_modified is not None:
        entry["last_modified"] = obj.last_modified.isoformat()
    return entry


def _entry_bytes(obj: TrackedObject) -> int:
    """Serialized cost of *obj*'s entry within a report body, including its
    trailing separator."""
    nested = len(json.dumps({"items": [_report_entry(obj)]}, indent=2))
    return nested - _EMPTY_ITEMS_WRAPPER_BYTES + 1


def chunk_items_for_report(
    items: list[TrackedObject],
    max_item_bytes: int = _REPORT_ITEM_BUDGET_BYTES,
) -> list[list[TrackedObject]]:
    """Split *items* into batches each small enough for one SNS publish.

    Sizing is measured, not assumed: each item is priced at the exact
    serialized length its entry occupies inside the report body, so a batch of
    long S3 keys chunks more aggressively than a batch of short ones.

    An item whose own entry exceeds *max_item_bytes* is still placed in a
    batch by itself rather than dropped or looped on — an S3 key is capped at
    1,024 bytes, so this cannot occur in practice, but the function stays
    total for any input.

    Returns an empty list for empty input, so the caller publishes nothing.

    Requirements: 4.5, 4.9
    """
    batches: list[list[TrackedObject]] = []
    current: list[TrackedObject] = []
    current_bytes = 0

    for obj in items:
        entry_bytes = _entry_bytes(obj)
        if current and current_bytes + entry_bytes > max_item_bytes:
            batches.append(current)
            current = []
            current_bytes = 0
        current.append(obj)
        current_bytes += entry_bytes

    if current:
        batches.append(current)
    return batches


# Plain-English rendering of each Replication_Outcome, ordered most to least
# severe. The order drives the summary sentence: a reader scanning an email
# should meet the outcomes needing attention first, regardless of which is
# numerically largest. ``None`` is an outcome an object can legitimately carry
# (see build_completion_report) and reads as UNKNOWN.
#
# Each entry is ``(outcome, plural phrase, singular phrase)``. Both forms are
# carried because the count precedes the phrase, so "1 were deleted" and
# "1 are still replicating" would otherwise be emitted. Phrases are also
# worded to read correctly when the clause absorbs the total ("1,057 objects
# were deleted before replication could be confirmed").
_OUTCOME_PHRASES: list[tuple[str | None, str, str]] = [
    ("FAILED",
     "failed to replicate",
     "failed to replicate"),
    ("EXPIRED",
     "were abandoned after the tracking window expired",
     "was abandoned after the tracking window expired"),
    ("UNKNOWN",
     "reported no replication status",
     "reported no replication status"),
    (None,
     "reported no replication status",
     "reported no replication status"),
    ("PENDING",
     "are still replicating",
     "is still replicating"),
    ("GONE",
     "were deleted before replication could be confirmed",
     "was deleted before replication could be confirmed"),
    ("COMPLETE",
     "replicated successfully",
     "replicated successfully"),
]

# Outcomes that mean an object did not demonstrably reach its destination, and
# so warrant an explicit call to action in the summary.
_ACTIONABLE_OUTCOMES: tuple[str | None, ...] = (
    "FAILED", "EXPIRED", "UNKNOWN", None,
)


def _format_completion_report_summary(
    source_bucket: str, item_count: int, outcome_counts: dict
) -> str:
    """Build the one-line, human-readable ``summary`` field for a
    Completion_Report (see :func:`build_completion_report`).

    Written for someone reading an email, not for a machine: counts carry
    thousands separators, each outcome is spelled out in plain English rather
    than exposing the internal enum, and the sentence ends by stating whether
    anything needs attention — which is the only reason to open the report.

    Kept to a single line (no embedded newlines) so it reads cleanly as one
    JSON string value in a pretty-printed message body — a multi-line value
    would render with an escaped ``\\n`` rather than a real line break.

    Outcomes are listed most severe first (see ``_OUTCOME_PHRASES``), so
    ``FAILED`` leads whenever present even when it is the minority.

    Example output::

        example-source-bucket: 1,057 objects — 1,057 were deleted before
        replication could be confirmed. No failures.

        my-bucket: 1,200 objects — 150 failed to replicate, 1,000 replicated
        successfully, 50 were deleted before replication could be confirmed.
        Action needed: 150 of 1,200 did not replicate.
    """
    if not item_count:
        return f"{source_bucket}: no objects to report."

    clauses: list[str] = []
    sole_phrase: str | None = None
    actionable = 0
    for outcome, plural, singular in _OUTCOME_PHRASES:
        count = outcome_counts.get(outcome, 0)
        if not count:
            continue
        phrase = singular if count == 1 else plural
        clauses.append(f"{count:,} {phrase}")
        if count == item_count:
            sole_phrase = phrase
        if outcome in _ACTIONABLE_OUTCOMES:
            actionable += count

    total = f"{item_count:,} object" + ("" if item_count == 1 else "s")

    if actionable:
        verdict = (
            f"Action needed: {actionable:,} of {item_count:,} did not replicate."
        )
    else:
        verdict = "No failures."
    if actionable == 0 and outcome_counts.get("COMPLETE") == item_count:
        verdict = "No action needed."

    # A single outcome covering every object absorbs the total, rather than
    # repeating the same count either side of a dash ("1,057 objects — 1,057
    # were deleted...").
    if sole_phrase is not None:
        return f"{source_bucket}: {total} {sole_phrase}. {verdict}"

    return f"{source_bucket}: {total} — {'; '.join(clauses)}. {verdict}"


def format_completion_report_subject(report: dict) -> str:
    """Build the SNS ``Subject`` line for a Completion_Report.

    Without a subject every report arrives titled with the SNS topic name, so
    an inbox of them cannot be triaged without opening each one. This carries
    the bucket, the object count, and whether anything needs attention.

    SNS requires the subject to be ASCII, free of newlines, and under 100
    characters — so this uses only ASCII punctuation (no em dash or ellipsis,
    which SNS rejects) and truncates the bucket name rather than the verdict,
    keeping the part that decides whether to open the mail.

    Requirements: 4.10
    """
    counts = report.get("outcome_counts") or {}
    item_count = report.get("item_count", 0)
    actionable = sum(counts.get(o, 0) for o in _ACTIONABLE_OUTCOMES)
    verdict = "action needed" if actionable else "no failures"
    bucket = str(report.get("source_bucket", ""))

    noun = "object" if item_count == 1 else "objects"
    tail = f": {item_count:,} {noun}, {verdict}"
    subject = f"S3 replication {bucket}{tail}"
    if len(subject) <= _SNS_MAX_SUBJECT_CHARS:
        return subject

    room = _SNS_MAX_SUBJECT_CHARS - len(f"S3 replication ...{tail}")
    return f"S3 replication {bucket[:max(room, 0)]}...{tail}"


# ---------------------------------------------------------------------------
# Report-missing detection (Decision 9)
# ---------------------------------------------------------------------------

_REPORT_OVERDUE_THRESHOLD = timedelta(hours=1)


def is_report_overdue(terminal_at: datetime, now: datetime) -> bool:
    """True iff more than 1 hour has elapsed since ``terminal_at`` (design.md
    Decision 9).

    Pure function reused by ``check_report_handler`` to decide whether a
    terminal, unconfirmed job's missing BOPS_Completion_Report should be
    escalated.

    Requirements: 8.2
    """
    return (now - terminal_at) > _REPORT_OVERDUE_THRESHOLD


# ---------------------------------------------------------------------------
# Pre-submission bucket-policy priming (Decision 10)
# ---------------------------------------------------------------------------


# security-scan-remediation Requirement 11 (Decision 8): well-formed IAM role
# ARN shape, any partition. The account segment is checked separately against
# the deployment's own account, since a syntactically valid ARN for a
# different account is exactly the tampered-configuration case this guards
# against.
_ROLE_ARN_RE = re.compile(r"^arn:(aws|aws-cn|aws-us-gov):iam::(\d{12}):role/.+$")

# Replication config ID validation: S3 replication rule IDs are documented as
# 1-255 characters, alphanumeric plus hyphens and dots. We block characters
# that would be dangerous in an S3 ARN Resource field: wildcards (*/?),
# path separators that could escape the prefix (../), and empty strings.
_CONFIG_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]{0,254}$")


def is_valid_replication_config_id(config_id: str) -> bool:
    """True iff config_id is safe to interpolate into a bucket policy Resource ARN.

    Rejects empty strings, strings containing wildcards (* or ?), path
    traversal sequences, or any character outside the documented S3
    replication rule ID character set.
    """
    if not isinstance(config_id, str):
        return False
    return _CONFIG_ID_RE.match(config_id) is not None


def validate_replication_role_arn(role_arn: str, account_id: str) -> bool:
    """True iff ``role_arn`` is a well-formed IAM role ARN whose account
    matches ``account_id`` (security-scan-remediation Requirement 11,
    Decision 8).

    ``rule_deriver.py`` reads ``Role`` from a source bucket's
    ``GetBucketReplication`` response with no validation, and it eventually
    reaches :func:`build_completion_report_bucket_policy_statement`, which
    places it directly in a bucket policy statement's ``Principal.AWS``. A
    tampered or malformed replication configuration could therefore name an
    arbitrary principal in the State_Bucket's own policy. This is the check
    that closes that gap — defence in depth, not a privilege escalation
    boundary (the premise for abuse, ``s3:PutReplicationConfiguration`` on a
    source bucket, already allows repointing replication to an
    attacker-controlled destination, a strictly greater impact).

    Requirements: 11.1, 11.2
    """
    match = _ROLE_ARN_RE.match(role_arn)
    if match is None:
        return False
    return match.group(2) == account_id


def build_completion_report_bucket_policy_statement(
    replication_config_id: str,
    replication_role_arn: str,
    state_bucket: str,
    account_id: str,
) -> dict | None:
    """Build the desired Completion_Report_Bucket_Policy_Statement for a
    ``replication_config_id`` (design.md Decision 10).

    Pure construction only — no read of any existing policy document. The
    statement grants exactly the one role about to submit a job for this
    ``replication_config_id`` write access to exactly that config's own
    report subtree on the State_Bucket:

    * ``Sid`` — derived from ``replication_config_id`` so that updating one
      config's statement never touches another config's statement
      (Requirement 9.3, 9.5).
    * ``Effect`` — always ``"Allow"``.
    * ``Principal`` — names exactly ``replication_role_arn``, never every
      role across the deployment (Requirement 9.3).
    * ``Action`` — ``"s3:PutObject"`` only.
    * ``Resource`` — limited to this config's own
      ``completion-reports/<replication_config_id>/*`` prefix on
      ``state_bucket``, not the whole ``completion-reports/`` namespace and
      not the whole bucket (Requirement 9.3).

    Returns ``None`` when ``replication_role_arn`` fails
    :func:`validate_replication_role_arn` — the caller skips this config's
    bucket policy statement entirely rather than placing an unvalidated
    value in ``Principal.AWS`` (security-scan-remediation Requirement 11.1,
    11.3). It does not raise.

    Requirements: 9.3, 11.1, 11.2, 11.3
    """
    if not validate_replication_role_arn(replication_role_arn, account_id):
        return None
    if not is_valid_replication_config_id(replication_config_id):
        return None
    return {
        "Sid": f"AllowCompletionReportWrite-{replication_config_id}",
        "Effect": "Allow",
        "Principal": {"AWS": replication_role_arn},
        "Action": "s3:PutObject",
        "Resource": f"arn:aws:s3:::{state_bucket}/completion-reports/{replication_config_id}/*",
    }


def ensure_completion_report_bucket_policy_statement(
    current_policy: dict | None,
    replication_config_id: str,
    replication_role_arn: str,
    state_bucket: str,
    account_id: str,
) -> tuple[dict | None, bool]:
    """Pure diff/merge for the State_Bucket's bucket policy (design.md
    Decision 10).

    ``current_policy`` is the current bucket policy document (a dict with a
    ``"Statement"`` list), or ``None`` when no policy exists at all yet
    (``NoSuchBucketPolicy``). Returns ``(merged_document_or_None,
    write_needed)``:

    * When ``replication_role_arn`` fails
      :func:`validate_replication_role_arn`, ``write_needed`` is ``False``
      and the first element is ``None`` — this config's statement is
      skipped entirely rather than placing an unvalidated value in
      ``Principal.AWS`` (security-scan-remediation Requirement 11.1, 11.3).
      The caller is responsible for logging the bucket and the rejected
      value (Requirement 11.3) — this function does not log.
    * When a statement with this ``replication_config_id``'s exact ``Sid``
      and identical content already exists in ``current_policy``,
      ``write_needed`` is ``False`` and the first element is ``None`` — the
      caller (``bucket_policy_adapter.ensure_completion_report_bucket_policy``)
      does not need a merged document since no write happens (Requirement
      9.4).
    * Otherwise, ``write_needed`` is ``True`` and the first element is the
      full merged document: starting from ``{"Version": "2012-10-17",
      "Statement": []}`` when ``current_policy`` is ``None`` (Requirement
      9.6), containing every pre-existing statement UNCHANGED — including
      statements under a different ``Sid`` and statements sharing the
      same ``Sid``-naming pattern for a different ``replication_config_id``
      (Requirement 9.5) — plus exactly one statement for this
      ``replication_config_id`` (replacing any prior statement under the
      same ``Sid``, e.g. one with stale/different content).

    This function never mutates ``current_policy`` in place; it always
    returns a new document when a write is needed.

    Requirements: 9.3, 9.4, 9.5, 9.6, 11.1, 11.2, 11.3
    """
    desired_statement = build_completion_report_bucket_policy_statement(
        replication_config_id, replication_role_arn, state_bucket, account_id
    )
    if desired_statement is None:
        return None, False
    sid = desired_statement["Sid"]

    if current_policy is None:
        base_statements: list[dict] = []
    else:
        base_statements = list(current_policy.get("Statement", []))

    for statement in base_statements:
        if statement.get("Sid") == sid:
            if statement == desired_statement:
                return None, False
            break

    merged_statements = [s for s in base_statements if s.get("Sid") != sid]
    merged_statements.append(desired_statement)

    merged_document = {
        "Version": "2012-10-17",
        "Statement": merged_statements,
    }
    return merged_document, True
