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

    Carries every field that resolution does not itself set through verbatim
    from ``obj``: ``source_bucket``, ``object_key``, ``version_id``,
    ``configs``, ``tagged_at``, ``last_modified``, ``matched_rules``, and
    ``destinations``. Only ``state``, ``resolved_at``, ``resolution_method``,
    and ``replication_outcome`` change here.

    Every carried field must be listed explicitly — ``TrackedObject``
    defaults them, so an omission silently reverts one to its default at the
    moment of resolution, which is exactly when the Completion_Report reads
    it.
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
        tagged_at=obj.tagged_at,
        last_modified=obj.last_modified,
        matched_rules=obj.matched_rules,
        destinations=obj.destinations,
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
        tagged_at=obj.tagged_at,
        last_modified=obj.last_modified,
        matched_rules=obj.matched_rules,
        destinations=obj.destinations,
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


def build_completion_report(
    source_bucket: str,
    items: list[TrackedObject],
    outstanding: int | None = None,
) -> dict:
    """Build the JSON-serializable Completion_Report payload for a
    per-source_bucket batch (design.md Decision 7).

    ``groups`` holds one entry per distinct
    ``(source_bucket, matched_rules, destinations)`` tuple among ``items``,
    carrying the aggregate statistics for the objects sharing that tuple:
    ``count``, per-group ``outcome_counts``, and the ``tagged_at`` and
    ``last_modified`` ranges. ``_build_group`` documents each field. In the
    common case — one bucket, one rule, one destination — the whole report is
    a single group, so its length is set by how many rule and destination
    combinations the bucket has rather than by how many objects replicated.

    Object keys and version IDs are not reported. A report of several hundred
    keys and version UUIDs cannot be read in an email, and the per-object
    detail is already in the S3 Batch Operations completion report CSV on the
    state bucket under ``completion-reports/``, which is where an operator
    goes to find out which specific object failed.

    ``format_version`` declares the payload shape for a non-email subscriber
    (SQS, Lambda, HTTPS) that parses the body.

    * ``summary`` is a one-line, human-readable headline — inserted as the
      FIRST key so it appears first when the dict is pretty-printed, letting
      an operator reading the raw notification (e.g. the SNS-to-email body)
      see the headline result without parsing JSON, while the message body
      remains strictly valid JSON for every other SNS subscriber protocol.
    * ``source_bucket`` is copied verbatim from the parameter, not read off
      any individual item.
    * ``item_count`` is ``len(items)`` — counts Tracked_Objects, and equals
      the sum of every group's ``count``.
    * ``outcome_counts`` counts ONE per Tracked_Object (the single aggregate
      outcome), not per destination. Only outcomes that actually occur
      appear as keys.

    ``outstanding`` is how many Tracked_Objects for ``source_bucket`` remain
    in tracking after this report — objects whose replication has not yet
    reached a terminal answer. A report covers what resolved since the
    previous one (Requirement 4.2), so it is not by itself the answer to
    "has everything I tagged arrived?": ``outstanding == 0`` is. Passing
    ``None`` omits both the ``outstanding`` field and its summary clause,
    which keeps a caller that does not know the count from asserting a
    misleading zero.

    ``obj.configs`` is not reported. Under the one-job-per-bucket design
    (single-batch-job-per-bucket design.md Decision D4) it holds exactly one
    entry keyed by the per-bucket sentinel, the source bucket's own name, so
    it carries no information beyond ``source_bucket``, which the group states
    directly. ``outcome_counts`` derives solely from
    ``obj.replication_outcome``, a single aggregate value per object that is
    not keyed by ``configs``.

    Requirements: 4.2, 4.3, 3.2, 1.1, 1.2, 1.3, 1.4
    """
    outcome_counts: dict[str, int] = {}
    for obj in items:
        outcome_counts[obj.replication_outcome] = (
            outcome_counts.get(obj.replication_outcome, 0) + 1
        )

    groups = _build_groups(items)

    report: dict = {
        "summary": _format_completion_report_summary(
            source_bucket, len(items), outcome_counts, outstanding
        ),
        "format_version": 2,
        "source_bucket": source_bucket,
        "item_count": len(items),
    }
    if outstanding is not None:
        report["outstanding"] = outstanding
    report["outcome_counts"] = outcome_counts
    report["groups"] = groups
    return report


# The tuple deciding which group a Tracked_Object is reported in:
# ``(source_bucket, sorted matched_rules, sorted destinations)``.
_GroupKey = tuple[str, tuple[str, ...], tuple[str, ...]]


def _group_key(obj: TrackedObject) -> _GroupKey:
    """The group *obj* belongs to.

    Both rule and destination sets are sorted into tuples, so an object
    matching several rules lands in one deterministic group rather than a
    different one per iteration order of the underlying frozensets.
    """
    return (
        obj.source_bucket,
        tuple(sorted(obj.matched_rules)),
        tuple(sorted(obj.destinations)),
    )


def _grouped_by_key(
    items: list[TrackedObject],
) -> dict[_GroupKey, list[TrackedObject]]:
    """Partition *items* by :func:`_group_key`, preserving input order.

    Groups come out in order of first appearance and each group's items stay
    in input order, so the same batch always produces the same report body.
    Shared by :func:`_build_groups` and :func:`chunk_items_for_report` so the
    grouping the chunker prices can never drift from the one published.
    """
    grouped: dict[_GroupKey, list[TrackedObject]] = {}
    for obj in items:
        grouped.setdefault(_group_key(obj), []).append(obj)
    return grouped


def _build_group(key: _GroupKey, items: list[TrackedObject]) -> dict:
    """Build the one report group for *items*, all of which share *key*.

    ``source_bucket``, ``matched_rules`` and ``destinations`` are stated once
    here rather than repeated per object, which is the repetition the grouped
    format exists to remove. ``matched_rules`` and ``destinations`` are always
    present, empty when the Solution holds no routing for these objects — an
    empty list says "none recorded" in the same position as a populated one,
    which reads better in an aggregate than a field that disappears.

    ``tagged_at_range`` and ``last_modified_range`` are ``[earliest, latest]``
    across the objects holding that value, and are omitted entirely when no
    object in the group holds it, since a range needs at least one endpoint.

    ``outcome_counts`` is per group, not per object: objects matching the same
    rule can still reach different outcomes, so a group with a ``FAILED`` count
    is how a mixed result stays visible without listing objects.
    """
    bucket, rules, dests = key

    outcome_counts: dict[str, int] = {}
    tagged_ats: list[datetime] = []
    last_modifieds: list[datetime] = []
    for obj in items:
        outcome_counts[obj.replication_outcome] = (
            outcome_counts.get(obj.replication_outcome, 0) + 1
        )
        if obj.tagged_at is not None:
            tagged_ats.append(obj.tagged_at)
        if obj.last_modified is not None:
            last_modifieds.append(obj.last_modified)

    group: dict = {
        "source_bucket": bucket,
        "matched_rules": list(rules),
        "destinations": list(dests),
        "count": len(items),
        "outcome_counts": outcome_counts,
    }
    if tagged_ats:
        group["tagged_at_range"] = [
            min(tagged_ats).isoformat(),
            max(tagged_ats).isoformat(),
        ]
    if last_modifieds:
        group["last_modified_range"] = [
            min(last_modifieds).isoformat(),
            max(last_modifieds).isoformat(),
        ]
    return group


def _build_groups(items: list[TrackedObject]) -> list[dict]:
    """Build every report group for *items* (see :func:`_build_group`)."""
    return [
        _build_group(key, group_items)
        for key, group_items in _grouped_by_key(items).items()
    ]


# ---------------------------------------------------------------------------
# Completion_Report chunking (SNS 256 KiB message limit)
# ---------------------------------------------------------------------------
# SNS rejects a Publish whose message body exceeds 256 KiB. The budget below
# is the usable allowance for report *groups*; the remainder covers the
# envelope (``summary``, ``format_version``, ``source_bucket``,
# ``item_count``, ``outcome_counts``) plus pretty-printing overhead. A
# rejected publish is not merely a lost notification: covered items are
# deleted only after a successful publish, so an oversized report would fail
# identically on every subsequent run and pin those items in the state object
# forever.
#
# A group's size is set by its rule and destination names, not by how many
# objects it covers, so the budget is reached only by a bucket with hundreds
# of distinct rule and destination combinations. One group per rule pair is
# the ceiling, and a bucket at the S3 limit of 1,000 replication rules is the
# case this guards.
_SNS_MAX_MESSAGE_BYTES = 262_144
_REPORT_GROUP_BUDGET_BYTES = 240_000

# SNS rejects a Subject longer than 100 characters.
_SNS_MAX_SUBJECT_CHARS = 100

# Serialized length of an empty ``groups`` wrapper, used to price one group at
# the exact nesting depth (and therefore the exact indentation) it occupies in
# the real report body. Measuring a group on its own at top level under-counts
# it by the added indentation of every line.
_EMPTY_GROUPS_WRAPPER_BYTES = len(json.dumps({"groups": []}, indent=2))


def _group_bytes(group: dict) -> int:
    """Serialized cost of *group* within a report body, including its
    trailing separator."""
    nested = len(json.dumps({"groups": [group]}, indent=2))
    return nested - _EMPTY_GROUPS_WRAPPER_BYTES + 1


def chunk_items_for_report(
    items: list[TrackedObject],
    max_group_bytes: int = _REPORT_GROUP_BUDGET_BYTES,
) -> list[list[TrackedObject]]:
    """Split *items* into batches each small enough for one SNS publish.

    Sizing is measured, not assumed: each group is priced at the exact
    serialized length it occupies inside the report body, so a batch with long
    rule and destination names chunks more aggressively than one with short
    ones.

    Splits between groups and never within one, so a group's header is never
    duplicated across two messages and each message stays internally
    consistent. A group whose own serialized size exceeds *max_group_bytes* is
    still placed in a batch by itself rather than dropped or looped on, so the
    function stays total for any input.

    Returns an empty list for empty input, so the caller publishes nothing.

    Requirements: 4.5, 4.9, 1.6
    """
    batches: list[list[TrackedObject]] = []
    current: list[TrackedObject] = []
    current_bytes = 0

    for key, group_items in _grouped_by_key(items).items():
        group_bytes = _group_bytes(_build_group(key, group_items))
        if current and current_bytes + group_bytes > max_group_bytes:
            batches.append(current)
            current = []
            current_bytes = 0
        current.extend(group_items)
        current_bytes += group_bytes

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


def _format_outstanding_clause(outstanding: int | None) -> str:
    """Render the trailing summary clause stating what is still being tracked.

    Empty when ``outstanding`` is ``None`` (the caller did not supply a
    count), so a summary never implies a zero it was not told.

    The wording says "in tracking" rather than "in flight" deliberately: an
    object enters tracking only once its Batch Operations job's completion
    report has been read, so a zero means nothing is left awaiting
    confirmation, not that no tagging work exists upstream of that point.
    """
    if outstanding is None:
        return ""
    if outstanding == 0:
        return " No objects remain in tracking."
    if outstanding == 1:
        return " 1 object remains in tracking."
    return f" {outstanding:,} objects remain in tracking."


def _format_completion_report_summary(
    source_bucket: str,
    item_count: int,
    outcome_counts: dict,
    outstanding: int | None = None,
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
    trailing = _format_outstanding_clause(outstanding)

    if not item_count:
        return f"{source_bucket}: no objects to report.{trailing}"

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
        return f"{source_bucket}: {total} {sole_phrase}. {verdict}{trailing}"

    return f"{source_bucket}: {total} — {'; '.join(clauses)}. {verdict}{trailing}"


def format_completion_report_subject(report: dict) -> str:
    """Build the SNS ``Subject`` line for a Completion_Report.

    Without a subject every report arrives titled with the SNS topic name, so
    an inbox of them cannot be triaged without opening each one. This carries
    the bucket, the object count, and whether anything needs attention.

    SNS requires the subject to be ASCII, free of newlines, and under 100
    characters — so this uses only ASCII punctuation (no em dash or ellipsis,
    which SNS rejects) and truncates the bucket name rather than the verdict,
    keeping the part that decides whether to open the mail.

    A non-zero ``outstanding`` count is appended, so a subject carrying no
    "still tracking" marker is the last report for that wave. The marker is
    added only when non-zero to keep the common subject short; the zero case
    is stated in the ``summary`` field instead.

    Requirements: 4.10
    """
    counts = report.get("outcome_counts") or {}
    item_count = report.get("item_count", 0)
    actionable = sum(counts.get(o, 0) for o in _ACTIONABLE_OUTCOMES)
    verdict = "action needed" if actionable else "no failures"
    bucket = str(report.get("source_bucket", ""))

    noun = "object" if item_count == 1 else "objects"
    tail = f": {item_count:,} {noun}, {verdict}"
    outstanding = report.get("outstanding")
    if outstanding:
        tail = f"{tail}, {outstanding:,} still tracking"
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
