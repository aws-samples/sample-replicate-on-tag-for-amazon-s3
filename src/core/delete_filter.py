"""Permanent-delete classification and Deleted_Version_Filter.

Pure components — no AWS dependencies.

Definitions (from spec Glossary):
  Permanent_Delete: a DELETE record with ``is_delete_marker = FALSE`` (or absent),
    or a null version superseded by a later null-version write.
  Soft_Delete: a DELETE record with ``is_delete_marker = TRUE``.

The ``PERMANENT_DELETE_SQL_PREDICATE`` constant is the shared SQL fragment for
the Athena reader and the UNLOAD anti-join so both stay in sync.

Requirements: 12.1, 12.3, 12.5, 13.1, 13.6, 13.8
"""
from __future__ import annotations

from src.core.models import MatchedObject


# ---------------------------------------------------------------------------
# Shared SQL predicate (Requirements 12.1, 12.3)
# ---------------------------------------------------------------------------

# Used in both read_permanent_deletes (Athena query) and the UNLOAD anti-join.
# Both consumer sites must use this constant so the classification stays in sync.
PERMANENT_DELETE_SQL_PREDICATE: str = (
    "record_type = 'DELETE' AND COALESCE(is_delete_marker, FALSE) = FALSE"
)



# ---------------------------------------------------------------------------
# Deleted_Version_Filter
# ---------------------------------------------------------------------------


def filter_deleted_versions(
    matched: set[MatchedObject],
    permanently_deleted: set[tuple[str, str | None]],
) -> tuple[set[MatchedObject], int]:
    """Remove permanently deleted versions from the matched set.

    Exclusion is null-safe: a ``MatchedObject`` with ``version_id=None`` is
    excluded when ``(object_key, None)`` is in ``permanently_deleted``.
    A non-null version id is never matched by a ``(key, None)`` entry and
    vice versa — each pair is a distinct identity (Requirements 12.2, 12.4).

    A key whose delete event was a Soft_Delete (delete marker, ``is_delete_marker
    = TRUE``) is absent from ``permanently_deleted``, so its tagged version is
    always retained (Requirement 12.5).

    Pure, order-independent, O(n) in the size of ``matched``.

    Parameters
    ----------
    matched:
        Set of :class:`~src.core.models.MatchedObject` to filter.
    permanently_deleted:
        Set of ``(object_key, version_id|None)`` pairs.  ``version_id=None``
        addresses the null version specifically (Requirement 12.2).

    Returns
    -------
    tuple[set[MatchedObject], int]
        ``(kept, excluded_count)`` where ``kept`` contains the survivors and
        ``excluded_count`` is the number removed (Requirement 13.8).

    Requirements: 13.1, 13.3, 13.6, 13.8
    """
    kept: set[MatchedObject] = set()
    excluded = 0
    for obj in matched:
        pair: tuple[str, str | None] = (obj.object_key, obj.version_id)
        if pair in permanently_deleted:
            excluded += 1
        else:
            kept.add(obj)
    return kept, excluded
