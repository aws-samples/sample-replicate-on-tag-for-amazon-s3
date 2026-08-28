"""Manifest format constant for the manifest-generation feature.

The manifest-generation *mode* selector (``select_manifest_strategy`` /
``ManifestGenerationMode``) has been removed: In_Memory_Generation is now the
Solution's sole generation path (see
``.kiro/specs/scale-threshold-and-drain-throughput/design.md``, "Manifest
strategy (simplified)"). This module now retains only the manifest *format*
constant, which is unconditionally ``INVENTORY_REPORT``.

Requirements: 1.1, 2.1, 2.2
"""
from __future__ import annotations

from enum import Enum


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ManifestFormat(Enum):
    """S3 Batch Operations manifest format.

    Only ``INVENTORY_REPORT`` is used. The ``CSV_MANIFEST`` format is no
    longer used.
    """

    INVENTORY_REPORT = "S3InventoryReport_CSV_20161130"


# ---------------------------------------------------------------------------
# Journal_Read_Row_Cap default (code-review-remediation verification-notes.md
# "scaling risk" finding)
#
# The Journal_Monitor's read_journal client-side pagination cost is
# effectively linear in row count — measured directly against the real
# Athena adapter's get_query_results pagination at ~617 ms/page (1000
# rows/page): ~62s at 100,000 rows, ~617s at 1,000,000 rows. A single-
# interval tagging burst at or above roughly 500,000-1,000,000 rows can
# therefore push read_journal's pagination alone close to or past the
# Lambda's 900s timeout, before the rest of the pipeline (matching, manifest
# writing, job submission) even runs — and a mid-pagination timeout is a
# worse failure mode than a normal job failure, since it can leave an
# in-flight lease uncleared.
#
# 500,000 was chosen to leave real headroom: at 500,000 rows, pagination
# alone is measured at roughly 300s, leaving several hundred seconds for the
# rest of the pipeline (checkpoint read, dedup/match, manifest write, job
# creation) before approaching the 900s ceiling. Per
# ``scale-threshold-and-drain-throughput``, this cap is now also the upper
# bound on the number of matched objects held in memory for a single run's
# manifest (Requirement 3.1).
# ---------------------------------------------------------------------------
JOURNAL_READ_ROW_CAP_DEFAULT: int = 500_000


# ---------------------------------------------------------------------------
# Row-budget split between the lookback tail and rows above the watermark.
#
# Journal_Read_Row_Cap governs the read as a whole. The read spans two ranges
# with different purposes: the lookback tail, re-scanned for late-arriving
# journal rows and almost entirely already processed, and the rows above the
# watermark, which are the only rows that can advance the checkpoint. When the
# tail alone can consume the whole cap, no row above the watermark is read and
# the checkpoint cannot move, so the bucket stops draining. These two constants
# are what stop that.
# ---------------------------------------------------------------------------

# Share of Journal_Read_Row_Cap the lookback tail may consume. The remainder is
# reserved for rows above the watermark, so a backlog larger than the cap still
# drains at a fifth of the cap per run instead of stalling. A policy number, not
# a measured one: it is set so a tail that fits is read whole and the ordinary
# run is unchanged.
TAIL_ROW_BUDGET_FRACTION: float = 0.8

# Floor on the reserved new-row budget: the invariant that a run can always read
# at least one row above the watermark, stated in one place rather than left
# implicit in the arithmetic.
#
# It does not bind at the fraction above, and cannot: floor(row_cap * 0.8) is at
# most row_cap - 1 for every positive integer row_cap, so the remainder is
# already at least 1. It binds only if TAIL_ROW_BUDGET_FRACTION is ever raised to
# 1.0 or beyond, which is the change this guard exists to survive — a fraction
# that gave the tail the whole cap would otherwise reduce the new-row budget to
# zero and reintroduce the stall the split exists to remove.
MIN_NEW_ROW_BUDGET: int = 1

# Smallest Journal_Read_Row_Cap the Solution accepts, and the reason it is not 1.
#
# At a cap of 1 the tail's share is floor(1 * 0.8) = 0, so a run with any lookback
# tail has no allowance to bound it against. Every way of resolving that is bad: a
# bound at the watermark drops the whole re-scan window and permanently loses any
# late arrival in it, no bound at all reads the tail unbounded, and skipping the
# bucket stalls it for as long as the cap stays there. Making the cap unreachable
# removes the case instead of picking a least-bad answer for it.
#
# 2 is the smallest value with a non-zero share for both ranges (allowance 1,
# new-row budget 1). It is still far too small to be operationally sensible; it is
# a floor on correctness, not a recommendation.
MIN_JOURNAL_READ_ROW_CAP: int = 2
