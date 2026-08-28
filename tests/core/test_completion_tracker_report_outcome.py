"""Unit tests for report-derived completion outcomes.

Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5
"""
from __future__ import annotations

import pytest

from src.core.completion_tracker import outcome_from_report_row
from src.core.models import ManifestEntry


def _entry(task_status: str | None) -> ManifestEntry:
    return ManifestEntry(
        source_bucket="source-bucket",
        object_key="object-key",
        version_id="version-id",
        task_status=task_status,
    )


@pytest.mark.parametrize(
    ("task_status", "expected_outcome"),
    [
        ("succeeded", "COMPLETE"),
        ("  SuCcEeDeD\t", "COMPLETE"),
        ("failed", "FAILED"),
        ("\nFAILED  ", "FAILED"),
        (None, "UNKNOWN"),
        ("", "UNKNOWN"),
        ("cancelled", "UNKNOWN"),
    ],
)
def test_outcome_from_report_row_maps_task_status(
    task_status: str | None,
    expected_outcome: str,
) -> None:
    assert outcome_from_report_row(_entry(task_status)) == expected_outcome
