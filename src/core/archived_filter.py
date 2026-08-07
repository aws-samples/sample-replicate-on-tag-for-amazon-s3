"""Archived_Object_Filter — excludes objects S3 will not replicate because
they are in an archived storage class.

Pure component — no AWS dependencies.

S3 does not replicate objects stored in S3 Glacier Flexible Retrieval
(``GLACIER``) or S3 Glacier Deep Archive (``DEEP_ARCHIVE``); they must be
restored and copied to another storage class first. Submitting one anyway
produces a Batch Operations task that cannot succeed, so it is a billed no-op.
Excluding them before manifest generation avoids that cost and lets the
Solution name the reason precisely, which the service's own error code does
not: a rejected object is reported only as ``SrcObjectNotEligible`` /
"Object is not eligible for replication", wording that names no storage class
and also covers unrelated ineligibility conditions.

Exclusion here is a cost and diagnostics improvement, not a correctness fix.
A rejected task leaves the object completely untouched: replication is never
initiated, so the object acquires no ``x-amz-replication-status`` and in
particular does not enter ``FAILED``. That distinction matters because S3
Lifecycle blocks transition and expiration on objects whose replication
status is ``PENDING`` or ``FAILED``, so an object rejected this way is not
lifecycle-frozen by the attempt. Both properties were verified against job
``17a27c3a-aa18-4bc7-91a6-caeaaa28dd8c`` in the us-west-2 test deployment.

**This filter must run after** :func:`src.core.journal_dedup.select_eligible_operations`.
A lifecycle transition into an archived class writes its own
``UPDATE_METADATA`` record, and since a transition changes neither key,
version, nor tags, that record shares a ``logical_operation_id`` with the
earlier tagging record. Deduplication collapses the pair and keeps the higher
``sequence_number``, which is the transition — so post-dedup the surviving
record reports the archived class and is excluded here. Applied before dedup
(or as a SQL ``WHERE`` predicate in the journal query) the transition record
would be removed first, the earlier record would survive reporting the
pre-transition class, and the object would reach a manifest anyway. The
ordering is the mechanism, not a detail.
"""
from __future__ import annotations

from src.core.models import TaggingOperation

# ---------------------------------------------------------------------------
# Archived storage classes
# ---------------------------------------------------------------------------

# The storage classes S3 refuses to replicate, and which the journal can
# actually identify.
#
# GLACIER_IR (S3 Glacier Instant Retrieval) is deliberately absent: despite
# the name it is not an archived class, needs no restore, and replicates
# normally. Excluding it would silently drop objects the Solution is supposed
# to replicate.
#
# INTELLIGENT_TIERING is also absent, and cannot be added. The S3
# Intelligent-Tiering Archive Access and Deep Archive Access tiers are equally
# unreplicable, but the journal's storage_class column reports
# INTELLIGENT_TIERING for such an object regardless of the tier it currently
# occupies, so a frequent-access object (replicable) is indistinguishable here
# from an archive-tier one (not replicable). Treating the class as archived
# would exclude every Intelligent-Tiering object in the bucket. Objects in
# those tiers therefore pass this filter and fail at the service, where the
# widened completion-report diagnosis in the orchestrator reports them.
ARCHIVED_STORAGE_CLASSES: frozenset[str] = frozenset({
    "GLACIER",
    "DEEP_ARCHIVE",
})


# ---------------------------------------------------------------------------
# Archived_Object_Filter
# ---------------------------------------------------------------------------


def is_archived(storage_class: str | None) -> bool:
    """Return ``True`` iff *storage_class* names a class S3 will not replicate.

    ``None`` and the empty string return ``False``. The journal documents
    ``storage_class`` as an optional column and populates it with ``NULL`` for
    records whose object version no longer existed when the event was
    processed, so absent evidence must not be read as evidence of archival:
    excluding on ``None`` would drop replicable objects. Comparison is
    case-insensitive and whitespace-tolerant so an unexpected rendering of a
    value still matches.
    """
    if not storage_class:
        return False
    return storage_class.strip().upper() in ARCHIVED_STORAGE_CLASSES


def filter_archived_operations(
    ops: list[TaggingOperation],
) -> tuple[list[TaggingOperation], list[TaggingOperation]]:
    """Split *ops* into those to keep and those excluded as archived.

    Pure, order-preserving, and O(n): both returned lists follow the order of
    *ops*, so the result is identical across runs for identical input.

    Unlike :func:`src.core.delete_filter.filter_deleted_versions`, which
    returns an excluded *count*, this returns the excluded operations
    themselves. The caller needs more than a total: an operator seeing objects
    silently skipped wants to know which storage class was responsible, and
    that breakdown can only be computed from the excluded records. Object keys
    are not usable for this, since they are redacted to fingerprints before
    reaching a log.

    Parameters
    ----------
    ops:
        Deduplicated, eligible operations for one interval. Must already have
        passed through :func:`src.core.journal_dedup.select_eligible_operations`
        — see this module's docstring for why the ordering is load-bearing.

    Returns
    -------
    tuple[list[TaggingOperation], list[TaggingOperation]]
        ``(kept, excluded)``. ``excluded`` is empty when no operation names an
        archived storage class, which is the common case.
    """
    kept: list[TaggingOperation] = []
    excluded: list[TaggingOperation] = []
    for op in ops:
        if is_archived(op.storage_class):
            excluded.append(op)
        else:
            kept.append(op)
    return kept, excluded


def count_by_storage_class(ops: list[TaggingOperation]) -> dict[str, int]:
    """Return a ``{storage_class: count}`` breakdown of *ops*.

    Intended for the excluded list from :func:`filter_archived_operations`, so
    a log entry can say which archived class accounted for how many objects
    rather than reporting an opaque total. Keys are normalized to upper case;
    a ``None`` storage class is counted under ``"UNKNOWN"``, which cannot
    occur for excluded operations but keeps the helper total-preserving for
    any input.
    """
    counts: dict[str, int] = {}
    for op in ops:
        name = op.storage_class.strip().upper() if op.storage_class else "UNKNOWN"
        counts[name] = counts.get(name, 0) + 1
    return counts
