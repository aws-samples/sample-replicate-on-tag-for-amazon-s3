"""Where the `recovery_scored` flag is committed, and where it deliberately is not.

`recovery_scored` is a durable once-only gate: once set on a submission record,
`_describe_prior_jobs` never scores that job again. What it authorizes is not
durable — a failed job's readmission is an in-memory rollback of
`last_processed_watermark` that only becomes real once the readmitted range is
resubmitted. So committing the flag before submission lands means any early
return in between consumes the readmission and drops the range for good.

These tests pin the commit points against that. For each verified early return
between scoring and submission, the flag must stay uncommitted so the next run
scores the job again and readmits the same range:

* the concurrency deferral,
* a journal read failure,
* a preflight failure,
* a manifest write failure,
* a submission failure,
* a lease acquisition failure.

Two positive cases keep the above from being vacuous: a successful submission
commits the flag, and so does a threshold breach, whose disable is itself durable
and is the whole of the work the scoring authorized.

Feature: scan-aa27a832-remediation
Requirements: 5.1, 5.2
"""
from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, patch

from src.adapters.athena_journal_adapter import JournalReadError
from src.adapters.inventory_manifest_writer import InventoryManifestWriteError
from src.orchestrator import run_interval
from tests.support import mock_state_store
from tests.test_orchestrator import (
    _BASE_RUNTIME,
    _WM_CURRENT,
    _checkpoint,
    _config,
    _diagnosed_flags,
    _failed,
    _op,
    _prior_rec,
    _rule,
    _submitted,
    _written_manifest,
)

_BUCKET = "my-bucket"

# The record whose job fails this run: scoring it readmits (_WM_LOW, _WM_CURRENT].
_FAILED_JOB = "job-scored"
# A second, still-running job, used only to drive the concurrency deferral.
_ACTIVE_JOB = "job-active"


def _run(
    *,
    describe_responses: dict | None = None,
    prior_submissions: dict | None = None,
    journal_ops: list | None = None,
    journal_errors: list | None = None,
    preflight_side_effect: Exception | None = None,
    manifest_side_effect: Exception | None = None,
    submission_result=None,
    acquire_lease_side_effect: Exception | None = None,
    max_concurrent_jobs: int = 3,
    max_batch_job_failures: int = 4,
) -> tuple[list, MagicMock]:
    """Run one interval with a prior Failed job, returning (emitted, mock_store).

    Modelled on ``_run_with_recovery_mocks`` in ``tests/test_orchestrator.py``,
    which does not return the store — and the store is the whole subject here,
    since the flag is only observable as a ``mark_report_diagnosed`` call.
    Each keyword forces exactly one of the early returns under test.
    """
    if prior_submissions is None:
        prior_submissions = {"rule-1": _prior_rec(job_id=_FAILED_JOB)}
    if describe_responses is None:
        describe_responses = {_FAILED_JOB: "Failed"}
    if journal_ops is None:
        journal_ops = [_op(_BUCKET)]

    mock_factory_cls = MagicMock()
    mock_factory = MagicMock()
    mock_factory_cls.return_value = mock_factory

    mock_s3control = MagicMock()

    def describe_job(AccountId, JobId):  # noqa: ARG001, N803 — boto3 kwarg names
        resp = describe_responses.get(JobId)
        if isinstance(resp, Exception):
            raise resp
        return {"Job": {"Status": resp}}

    mock_s3control.describe_job.side_effect = describe_job
    mock_factory.create_s3control_client.return_value = mock_s3control

    mock_store_cls = MagicMock()
    mock_store = mock_state_store()
    mock_store_cls.return_value = mock_store
    # (s3_client, state_bucket, source_bucket), passed positionally by
    # _prepare_state_and_recovery.
    mock_store.get_checkpoint.side_effect = (
        lambda *args: (_checkpoint(args[2], _WM_CURRENT), '"etag-0"')
    )
    mock_store.get_submission_records.return_value = prior_submissions
    if acquire_lease_side_effect is not None:
        mock_store.acquire_lease.side_effect = acquire_lease_side_effect
    else:
        mock_store.acquire_lease.return_value = '"etag-1"'
    mock_store.release_lease.return_value = '"etag-2"'
    mock_store.record_submission.return_value = '"etag-3"'
    mock_store.mark_report_diagnosed.return_value = '"etag-4"'

    emitted: list = []

    runtime = {
        **_BASE_RUNTIME,
        "max_concurrent_jobs_per_bucket": max_concurrent_jobs,
        "max_batch_job_failures": max_batch_job_failures,
    }

    with (
        patch("src.orchestrator.ClientFactory", mock_factory_cls),
        patch("src.orchestrator.state_store_module.StateStore", mock_store_cls),
        patch(
            "src.orchestrator.replication_config_adapter.get_replication_rules",
            return_value=([_rule(_BUCKET)], []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.read_journal",
            return_value=(journal_ops, journal_errors or []),
        ),
        patch(
            "src.orchestrator.athena_journal_adapter.find_row_count_boundary",
            return_value=None,
        ),
        patch(
            "src.orchestrator.write_in_memory_inventory_manifest",
            side_effect=manifest_side_effect,
            return_value=_written_manifest(),
        ),
        patch(
            "src.orchestrator.batch_operations_adapter.submit_batch_job",
            return_value=submission_result or _submitted(),
        ),
        patch(
            "src.orchestrator.preflight_count",
            side_effect=preflight_side_effect,
            return_value=0,
        ),
        patch("src.orchestrator.read_permanent_deletes", return_value=set()),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
        patch(
            "src.orchestrator.bops_report_reader.read_bops_completion_report",
            return_value=[],
        ),
    ):
        run_interval(_config([_BUCKET]), runtime)

    return emitted, mock_store


def _scored(mock_store) -> bool:
    """Whether the run asked to commit ``recovery_scored`` for any job.

    Deliberately not ``_diagnosed_flags``, which asserts the store method was
    called at all: several of these paths must be able to assert the flag is
    absent whether or not diagnosis happened.
    """
    return any(
        call.kwargs.get("recovery_scored", False)
        for call in mock_store.mark_report_diagnosed.call_args_list
    )


def _readmitted(emitted: list) -> bool:
    """Whether the failed job's outcome was scored, i.e. the rollback was planned.

    Every negative test below has to establish this first, or it would pass by
    scoring nothing rather than by deferring the commit.
    """
    return any(
        isinstance(e, dict)
        and e.get("event") == "audit"
        and e.get("action") == "batch_job_failure_readmit"
        for e in emitted
    )


class TestAnEarlyReturnDoesNotConsumeTheReadmission:
    """The five verified early returns between scoring and submission, plus the
    lease failure the commit-point comment names alongside them."""

    def test_the_concurrency_deferral_leaves_the_flag_uncommitted(self):
        """The deferral returns before the journal is read, so the rollback it
        would consume has not been resubmitted — and cannot be, this interval."""
        emitted, store = _run(
            prior_submissions={
                "rule-1": _prior_rec(config_id="rule-1", job_id=_FAILED_JOB),
                "rule-2": _prior_rec(config_id="rule-2", job_id=_ACTIVE_JOB),
            },
            describe_responses={_FAILED_JOB: "Failed", _ACTIVE_JOB: "Active"},
            max_concurrent_jobs=1,
        )

        assert _readmitted(emitted)
        assert any(
            isinstance(e, dict)
            and e.get("action") == "submission_deferred_job_in_flight"
            for e in emitted
        )
        assert store.record_submission.call_count == 0
        assert _scored(store) is False

    def test_a_journal_read_failure_leaves_the_flag_uncommitted(self):
        emitted, store = _run(
            journal_ops=[],
            journal_errors=[
                JournalReadError(bucket=_BUCKET, cause="Athena boom", is_fatal=True),
            ],
        )

        assert _readmitted(emitted)
        assert store.record_submission.call_count == 0
        assert _scored(store) is False

    def test_a_preflight_failure_leaves_the_flag_uncommitted(self):
        emitted, store = _run(
            preflight_side_effect=RuntimeError("preflight query FAILED"),
        )

        assert _readmitted(emitted)
        assert store.record_submission.call_count == 0
        assert _scored(store) is False

    def test_a_manifest_write_failure_leaves_the_flag_uncommitted(self):
        emitted, store = _run(
            manifest_side_effect=InventoryManifestWriteError(
                _BUCKET, "AccessDenied",
            ),
        )

        assert _readmitted(emitted)
        assert store.record_submission.call_count == 0
        assert _scored(store) is False

    def test_a_submission_failure_leaves_the_flag_uncommitted(self):
        """The nearest miss of the six: the code reaches the commit point and is
        held off it only by ``sub_outcome.succeeded``."""
        emitted, store = _run(submission_result=_failed())

        assert _readmitted(emitted)
        assert _scored(store) is False

    def test_a_lease_acquisition_failure_leaves_the_flag_uncommitted(self):
        from src.adapters import state_store as state_store_module

        emitted, store = _run(
            acquire_lease_side_effect=state_store_module.ConditionalWriteError(
                "stale ETag",
            ),
        )

        assert _readmitted(emitted)
        assert store.record_submission.call_count == 0
        assert _scored(store) is False


class TestTheFlagIsCommittedWhereTheWorkIsDurable:
    """Without these the group above would pass on a build that never commits
    the flag at all, which would re-score every failed job forever."""

    def test_a_successful_submission_commits_the_flag(self):
        emitted, store = _run()

        assert _readmitted(emitted)
        store.record_submission.assert_called_once()
        assert _diagnosed_flags(store)["recovery_scored"] is True

    def test_a_threshold_breach_commits_the_flag(self):
        """No rollback happens on this path: the bucket is disabled, which is
        durable, and is the whole of the work the scoring authorized. Committing
        keeps a re-enabled bucket from being disabled again by the same
        historical failures."""
        prior = {
            "rule-1": dataclasses.replace(
                _prior_rec(job_id=_FAILED_JOB), consecutive_failures=3,
            ),
        }
        emitted, store = _run(prior_submissions=prior, max_batch_job_failures=4)

        # Threshold reached (3 + 1 == 4): nothing was submitted this interval,
        # and the run returned from _prepare_state_and_recovery.
        assert _readmitted(emitted)
        assert store.record_submission.call_count == 0
        assert _scored(store) is True


class TestAScoredRollbackWithNothingToSubmit:
    def test_an_idle_widened_read_leaves_the_flag_uncommitted_every_run(self):
        """Deliberate, and the one case where the flag is never consumed.

        The rollback widened the read window, the widened read matched nothing,
        so there is no job and no `_SubmissionOutcome` at all. Committing here
        would be the defect this whole ordering exists to prevent: the rollback
        is in memory only, so the flag would retire the readmission without any
        billed work covering the range, and the next run would resume from the
        un-rolled-back watermark with the range gone.

        The cost of not committing is that the job is re-scored on every
        subsequent run — one readmit audit entry per interval, no other effect,
        and it stops as soon as the window has something in it. That is the
        cheaper side of the trade, so this is pinned rather than fixed.
        """
        emitted, store = _run(journal_ops=[])

        assert _readmitted(emitted)
        assert store.record_submission.call_count == 0
        assert _scored(store) is False

        # And again, on a second identical run: re-scoring is the whole point.
        emitted_again, store_again = _run(journal_ops=[])
        assert _readmitted(emitted_again)
        assert _scored(store_again) is False
