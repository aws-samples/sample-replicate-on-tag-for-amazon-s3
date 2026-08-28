"""Window-population tests for the retag-suppression spec (task 5).

These assert on the **persisted** ``processed_window`` — read back through a
real ``StateStore`` over a moto-backed S3 — rather than on
``journal_dedup.build_submitted_refs`` alone.  The defect being guarded against
was in *what reached the window*: the pre-change code built refs correctly from
the wrong input set (every eligible operation), so a pure-function test of the
builder would have passed against it.

Each test drives a full ``run_interval``.  Only the boundaries that cannot run
locally are mocked: Athena (journal read, preflight count, permanent-delete
read), the bucket's replication configuration, and the S3 Control ``CreateJob``
call.  Everything else is the real code path — real ``StateStore``, real
conditional writes, real manifest CSV + inventory manifest written to S3, real
``rule_matcher``, real ``filter_deleted_versions``, real ``advance_checkpoint``.

Requirements: retag-suppression 4.4, 4.5, 4.6
"""
from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from src.adapters.batch_operations_adapter import SubmissionResult
from src.adapters.state_store import StateStore
from src.core.checkpoint_logic import is_eligible
from src.core.models import (
    DerivedReplicationRule,
    DestinationRef,
    SubmissionStatus,
    TaggingOperation,
)
from src.core.watermark import to_watermark
from src.orchestrator import run_interval

_ACCOUNT = "123456789012"
_REGION = "us-west-2"
_STATE_BUCKET = "state-bucket"
_SRC_BUCKET = "source-bucket"
_CONFIG = {"buckets": [{"name": _SRC_BUCKET, "region": _REGION}]}
_LOOKBACK = timedelta(seconds=3600)

# Event times sit in the recent past: a persisted watermark more than
# MAX_FUTURE_SKEW ahead of now is rejected by the checkpoint deserializer, and
# every event must stay inside the 3600s lookback window.
_BASE = (datetime.now(tz=UTC) - timedelta(minutes=20)).replace(microsecond=0)

_MATCHING_TAGS = {"replicate": "yes"}
_NON_MATCHING_TAGS = {"project": "x"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _at(seconds: int) -> datetime:
    return _BASE + timedelta(seconds=seconds)


def _op(
    key: str,
    tags: dict[str, str],
    *,
    version: str | None,
    seconds: int,
    seq: str = "seq-001",
) -> TaggingOperation:
    return TaggingOperation(
        source_bucket=_SRC_BUCKET,
        object_key=key,
        resulting_tag_set=tags,
        sequence_number=seq,
        operation="PutObjectTagging",
        event_time=_at(seconds),
        operation_version=version,
    )


def _rule() -> DerivedReplicationRule:
    return DerivedReplicationRule(
        source_bucket=_SRC_BUCKET,
        # replication_config_id must be the bucket name: the manifest generator
        # and _filter_deleted_entries both key on it.
        replication_config_id=_SRC_BUCKET,
        rule_id="rule-1",
        tag_filter=dict(_MATCHING_TAGS),
        destination=DestinationRef(bucket_arn="arn:aws:s3:::dest-bucket"),
    )


def _submitted() -> SubmissionResult:
    return SubmissionResult(
        status=SubmissionStatus.SUBMITTED,
        config_id=_SRC_BUCKET,
        object_count=1,
        job_id="job-abc",
    )


@contextlib.contextmanager
def _harness(s3_client, ops, permanent_deletes=None):
    """Patch only the AWS boundaries that cannot run locally.

    ``StateStore`` is *not* patched — ``run_interval`` constructs a real one and
    every state read/write goes to the moto-backed S3 bucket.
    """
    factory = MagicMock()
    factory.create_s3_client.return_value = s3_client
    factory.create_athena_client.return_value = MagicMock()
    # DescribeJob must report a terminal status. These tests run a second
    # interval after a submission, and the orchestrator now defers a bucket whose
    # previous job has not finished, so an unconfigured mock status would make
    # that second interval a no-op and the test would pass for the wrong reason.
    s3control_client = MagicMock()
    s3control_client.describe_job.return_value = {
        "Job": {
            "Status": "Complete",
            "CreationTime": _BASE,
            "TerminationDate": _BASE,
            "ProgressSummary": {
                "NumberOfTasksSucceeded": 1,
                "NumberOfTasksFailed": 0,
            },
        }
    }
    factory.create_s3control_client.return_value = s3control_client

    with contextlib.ExitStack() as stack:
        stack.enter_context(patch("src.orchestrator.ClientFactory", return_value=factory))
        stack.enter_context(patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule()], []),
        ))
        stack.enter_context(patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=(list(ops), []),
        ))
        stack.enter_context(patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ))
        stack.enter_context(patch("src.orchestrator.preflight_count", return_value=len(ops)))
        stack.enter_context(patch(
            "src.orchestrator.read_permanent_deletes",
            return_value=set(permanent_deletes or set()),
        ))
        stack.enter_context(patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=_submitted(),
        ))
        stack.enter_context(patch("src.orchestrator.MetricsPublisher"))
        yield


def _runtime_config() -> dict:
    return {
        "state_bucket": _STATE_BUCKET,
        "athena_workgroup": "primary",
        "athena_output_location": f"s3://{_STATE_BUCKET}/athena/",
        "account_id": _ACCOUNT,
        "region": _REGION,
        "journal_lookback_seconds": int(_LOOKBACK.total_seconds()),
    }


def _read_state(s3_client):
    """Read the persisted CheckpointState back through a real StateStore."""
    state, _etag = StateStore().get_checkpoint(s3_client, _STATE_BUCKET, _SRC_BUCKET)
    return state


def _window_ids(state) -> set[str]:
    return {ref.logical_operation_id for ref in state.processed_window}


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=_REGION)
        client.create_bucket(
            Bucket=_STATE_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": _REGION},
        )
        yield client


# ===========================================================================
# Requirement 4.4 / 4.6 — a non-matching operation is not remembered, but the
# watermark still moves past it.
# ===========================================================================


class TestNonMatchingOperationNotRemembered:
    """An eligible operation that matched no rule reaches no manifest, so it
    must not enter ``processed_window`` just because another object in the same
    bucket caused a submission (Requirement 4.4) — while the watermark still
    advances over it (Requirement 4.6, design D3)."""

    def test_non_matching_op_absent_from_window_and_watermark_advances(self, s3_client):
        matching = _op("match/obj.txt", _MATCHING_TAGS, version="v-match", seconds=0)
        # Deliberately the *latest* event of the interval: R2.5 requires the
        # watermark to advance over all eligible operations, not only over the
        # ones that reached the manifest.
        non_matching = _op("nomatch/obj.txt", _NON_MATCHING_TAGS, version="v-nomatch", seconds=60)

        with _harness(s3_client, [matching, non_matching]):
            outcome = run_interval(_CONFIG, _runtime_config())

        assert outcome.buckets[0].submitted == 1
        assert outcome.buckets[0].matched == 1

        state = _read_state(s3_client)
        ids = _window_ids(state)

        # The submitted object is remembered ...
        assert matching.logical_operation_id in ids
        # ... the non-matching one is not (Requirement 4.4).
        assert non_matching.logical_operation_id not in ids

        # ... and the watermark still passed it (Requirement 4.6 / D3), so the
        # cursor does not stall on a record that never matches.
        assert state.last_processed_watermark >= to_watermark(non_matching.event_time)

    def test_non_matching_op_is_eligible_on_the_next_interval(self, s3_client):
        # Here the non-matching operation is the *earliest* of the interval, so
        # after the watermark advances it sits below the watermark and inside
        # the lookback window. processed_window is then the only thing that
        # could suppress it, which is what makes this assertion non-vacuous.
        non_matching = _op("nomatch/obj.txt", _NON_MATCHING_TAGS, version="v-nomatch", seconds=0)
        matching = _op("match/obj.txt", _MATCHING_TAGS, version="v-match", seconds=60)

        with _harness(s3_client, [matching, non_matching]):
            run_interval(_CONFIG, _runtime_config())

        state = _read_state(s3_client)

        # Both operations are at or below the watermark and inside the lookback
        # window, so the only thing that can suppress either is
        # processed_window. The submitted one is suppressed; the non-matching
        # one is not (Requirement 4.4).
        assert to_watermark(non_matching.event_time) <= state.last_processed_watermark
        assert is_eligible(matching, state, _LOOKBACK) is False
        assert is_eligible(non_matching, state, _LOOKBACK) is True

        # Drive the second interval end to end: the same journal window is
        # re-read and only the non-matching operation survives eligibility.
        with _harness(s3_client, [matching, non_matching]):
            outcome2 = run_interval(_CONFIG, _runtime_config())

        assert outcome2.buckets[0].ops_read == 1
        assert outcome2.buckets[0].matched == 0
        assert outcome2.buckets[0].submitted == 0


# ===========================================================================
# Requirement 4.5 — loss-table row 4: an operation excluded by the
# Deleted_Version_Filter is not remembered.
# ===========================================================================


class TestDeletedVersionFilterExclusionNotRemembered:
    """The null version of a suspended-versioning bucket is excluded by the
    Deleted_Version_Filter when a later PUT supersedes it.  That operation
    reached no manifest, so it must not be recorded — otherwise the next
    tagging event on the new null version, which carries the same identity when
    the tag set is unchanged, is suppressed for the rest of the lookback window
    (loss-table row 4)."""

    def test_excluded_op_absent_while_manifest_survivors_present(self, s3_client):
        null_version = _op("suspended/obj.txt", _MATCHING_TAGS, version=None, seconds=0)
        kept_one = _op("keep/one.txt", _MATCHING_TAGS, version="v-one", seconds=60)
        kept_two = _op("keep/two.txt", _MATCHING_TAGS, version="v-two", seconds=120)

        with _harness(
            s3_client,
            [null_version, kept_one, kept_two],
            permanent_deletes={("suspended/obj.txt", None)},
        ):
            outcome = run_interval(_CONFIG, _runtime_config())

        assert outcome.buckets[0].submitted == 1

        state = _read_state(s3_client)
        ids = _window_ids(state)

        # The excluded operation is absent (Requirement 4.5, 2.7) ...
        assert null_version.logical_operation_id not in ids
        # ... while the surviving objects of the same manifest are present.
        assert kept_one.logical_operation_id in ids
        assert kept_two.logical_operation_id in ids

    def test_new_null_version_tagging_is_eligible_next_interval(self, s3_client):
        null_version = _op("suspended/obj.txt", _MATCHING_TAGS, version=None, seconds=0)
        kept_one = _op("keep/one.txt", _MATCHING_TAGS, version="v-one", seconds=60)
        kept_two = _op("keep/two.txt", _MATCHING_TAGS, version="v-two", seconds=120)

        with _harness(
            s3_client,
            [null_version, kept_one, kept_two],
            permanent_deletes={("suspended/obj.txt", None)},
        ):
            run_interval(_CONFIG, _runtime_config())

        state = _read_state(s3_client)

        # A new null version tagged with the same tag set carries the same
        # logical_operation_id (version_id is None for both null versions), so
        # this is the identity that must not be suppressed. Its watermark is
        # below the advanced watermark and inside the lookback window, so
        # processed_window is the only thing that could suppress it.
        assert to_watermark(null_version.event_time) <= state.last_processed_watermark
        assert is_eligible(null_version, state, _LOOKBACK) is True

        # The objects that did reach the manifest stay suppressed.
        assert is_eligible(kept_one, state, _LOOKBACK) is False
        assert is_eligible(kept_two, state, _LOOKBACK) is False

        # Second interval: the excluded object is re-read and re-matched, and
        # because the permanent delete no longer covers the *new* null version
        # it now reaches a manifest and is submitted.
        with _harness(s3_client, [null_version, kept_one, kept_two]):
            outcome2 = run_interval(_CONFIG, _runtime_config())

        assert outcome2.buckets[0].ops_read == 1
        assert outcome2.buckets[0].matched == 1
        assert outcome2.buckets[0].submitted == 1

        state2 = _read_state(s3_client)
        assert null_version.logical_operation_id in _window_ids(state2)
