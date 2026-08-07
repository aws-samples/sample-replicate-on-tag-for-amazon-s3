"""Tests for src/core/manifest_strategy.py.

``select_manifest_strategy`` / ``ManifestGenerationMode`` and the
``SCALE_THRESHOLD_DEFAULT`` / ``INLINE_HASH_CEILING_*_DEFAULT`` constants
have been removed (In_Memory_Generation is now the Solution's sole
generation path — see
``.kiro/specs/scale-threshold-and-drain-throughput/design.md``). This module
now only exercises the retained ``ManifestFormat`` constant and
``JOURNAL_READ_ROW_CAP_DEFAULT``.

Feature: scale-threshold-and-drain-throughput
Requirements: 1.1, 2.1, 2.2
"""
from __future__ import annotations

from src.core.manifest_strategy import (
    JOURNAL_READ_ROW_CAP_DEFAULT,
    ManifestFormat,
)


class TestManifestFormat:
    def test_inventory_report_format_string(self):
        """INVENTORY_REPORT value matches the S3 inventory format string."""
        assert ManifestFormat.INVENTORY_REPORT.value == "S3InventoryReport_CSV_20161130"


# ---------------------------------------------------------------------------
# JOURNAL_READ_ROW_CAP_DEFAULT (code-review-remediation verification-notes.md
# "scaling risk" finding) — the single scale knob now that mode selection has
# been removed (Requirement 3.1).
# ---------------------------------------------------------------------------


class TestJournalReadRowCapDefault:
    def test_default_is_500000(self):
        """500,000 was chosen to leave real headroom: pagination alone was
        measured at ~617 ms/page, so 500,000 rows is roughly 300s of
        pagination, leaving several hundred seconds for the rest of the
        pipeline before the 900s Lambda timeout."""
        assert JOURNAL_READ_ROW_CAP_DEFAULT == 500_000

    def test_is_a_positive_int(self):
        assert isinstance(JOURNAL_READ_ROW_CAP_DEFAULT, int)
        assert JOURNAL_READ_ROW_CAP_DEFAULT > 0
