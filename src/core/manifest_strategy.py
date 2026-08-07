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
