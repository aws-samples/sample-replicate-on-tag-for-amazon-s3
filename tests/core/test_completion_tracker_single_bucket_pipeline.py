"""Integration-style regression test for the D4/D5 single-batch-job-per-bucket
completion-tracking pipeline (task 5.4).

Chains the full pipeline for a single bucket-sentinel-keyed TrackedObject:

1. A TrackedObject with one bops_confirmed ConfigContext (keyed by the
   bucket sentinel) is check-eligible (design.md D4 / task 5.1).
2. reconcile_source_status_check resolves it to COMPLETE (unchanged core
   logic, exercised here against the sentinel-keyed object).
3. should_publish returns True against a quiescent ScanState recorded under
   the same bucket sentinel (design.md D5 / task 5.2).
4. build_completion_report produces the expected single-destination,
   correct-outcome report item (design.md D4 / task 5.3).

Each step already has isolated unit coverage elsewhere (tests/adapters/
test_state_store.py's TestGetCheckEligibleItems, tests/core/
test_completion_tracker_quiescence_and_publish.py, tests/core/
test_completion_tracker_build_report.py). This test exercises them chained
together in one place as a regression guard for the D4/D5 pipeline as a
whole.

_Requirements: 3.1, 3.2, 3.3_
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.adapters.source_status_adapter import SourceStatusCheckKind, SourceStatusResult
from src.core.completion_tracker import (
    build_completion_report,
    quiescence_check,
    reconcile_source_status_check,
    select_check_candidates,
    should_publish,
)
from src.core.models import CompletionState, ConfigContext, ScanState, TrackedObject

_BUCKET_SENTINEL = "my-bucket"
_MANIFEST_AT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_CHECK_AT = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)


def test_single_confirming_job_flows_end_to_end_to_a_correct_report():
    # 1. A TrackedObject with one bops_confirmed ConfigContext keyed by the
    #    bucket sentinel (as merge_completion_configs produces, task 5.1).
    pending_obj = TrackedObject(
        source_bucket=_BUCKET_SENTINEL,
        object_key="key-a",
        version_id="v1",
        configs={
            _BUCKET_SENTINEL: ConfigContext(
                replication_config_id=_BUCKET_SENTINEL,
                job_id="job-bucket-1",
                manifest_generated_at=_MANIFEST_AT,
                bops_confirmed=True,
            )
        },
        state=CompletionState.PENDING,
    )
    items = {"key-a\x00v1": pending_obj}

    # The single confirming job alone gates the item into this interval's
    # Source_Status_Check candidates -- no wait on any further confirmation.
    candidates = select_check_candidates(items)
    assert len(candidates) == 1
    assert candidates[0].obj is pending_obj

    # 2. Simulate reconcile_source_status_check resolving it to COMPLETE.
    result = SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value="COMPLETED")
    resolved_obj = reconcile_source_status_check(pending_obj, result, now=_CHECK_AT)
    assert resolved_obj.state == CompletionState.RESOLVED
    assert resolved_obj.replication_outcome == "COMPLETE"
    # configs pass through untouched -- still the single bucket-sentinel entry.
    assert list(resolved_obj.configs.keys()) == [_BUCKET_SENTINEL]

    # 3. should_publish resolves True against a quiescent ScanState recorded
    #    under the same bucket sentinel key (design.md D5).
    quiescent_scan_state = ScanState(
        last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=0
    )
    scan_state_by_config = {_BUCKET_SENTINEL: quiescent_scan_state}
    assert quiescence_check(_MANIFEST_AT, quiescent_scan_state) is True
    assert should_publish(resolved_obj, scan_state_by_config) is True

    # 4. build_completion_report produces a correct-outcome grouped report that
    #    names its source bucket, not the sentinel-keyed config, as context.
    report = build_completion_report(_BUCKET_SENTINEL, [resolved_obj])
    assert report["source_bucket"] == _BUCKET_SENTINEL
    assert report["item_count"] == 1
    assert report["outcome_counts"] == {"COMPLETE": 1}
    assert report["format_version"] == 2
    assert len(report["groups"]) == 1
    group = report["groups"][0]
    assert group["source_bucket"] == _BUCKET_SENTINEL
    assert group["count"] == 1
    assert group["outcome_counts"] == {"COMPLETE": 1}


def test_not_yet_quiescent_holds_the_publish_decision_before_reporting():
    """The 'holds/defers' half of the pipeline: a resolved, sentinel-keyed
    object whose bucket scan has not yet gone quiescent is not published
    this pass (should_publish is False) -- the orchestrator's publish phase
    would skip it and retry next interval, per Requirement 3.3."""
    resolved_obj = TrackedObject(
        source_bucket=_BUCKET_SENTINEL,
        object_key="key-b",
        version_id="v1",
        configs={
            _BUCKET_SENTINEL: ConfigContext(
                replication_config_id=_BUCKET_SENTINEL,
                job_id="job-bucket-1",
                manifest_generated_at=_MANIFEST_AT,
                bops_confirmed=True,
            )
        },
        state=CompletionState.RESOLVED,
        resolved_at=_CHECK_AT,
        resolution_method="source_status_header",
        replication_outcome="COMPLETE",
    )

    # Scan recorded under the sentinel found a nonzero match count -- not
    # yet quiescent.
    not_yet_quiescent = ScanState(
        last_scan_at=_MANIFEST_AT + timedelta(hours=1), last_scan_match_count=2
    )
    scan_state_by_config = {_BUCKET_SENTINEL: not_yet_quiescent}
    assert should_publish(resolved_obj, scan_state_by_config) is False

    # No scan recorded at all yet under the sentinel -- also holds.
    assert should_publish(resolved_obj, {}) is False
