"""Row-cap forward progress: the read window `_read_journal_window` builds.

The defect these cover: the row-cap boundary was computed over rows above
``watermark - lookback`` rather than above the watermark, so a lookback tail
holding at least ``JournalReadRowCap`` rows produced a boundary at or below the
watermark. The read window then held only already-submitted rows, nothing was
submitted, the watermark did not move, and the next run computed the identical
window. The bucket stopped draining permanently and silently.

These tests drive `_read_journal_window` directly with the adapter functions
mocked, rather than driving `_process_bucket` end to end, so the read window is
asserted on rather than inferred from a submission.

Feature: row-cap-forward-progress
Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.4, 3.1, 3.2, 3.3, 4.1, 4.2, 5.1, 5.2,
              5.3, 7.1, 7.2
"""
from __future__ import annotations

import math
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from src import orchestrator
from src.core.manifest_strategy import TAIL_ROW_BUDGET_FRACTION
from src.core.models import MonitoredBucket
from src.core.watermark import EPOCH_WATERMARK

_BUCKET = "source-bucket"
_LOOKBACK = timedelta(seconds=7200)

# A watermark two hours above the lookback window start it implies, so the tail
# range is (_WINDOW_START, _WATERMARK].
_WATERMARK = "2024-06-01T12:00:00.000000Z"
_WINDOW_START = "2024-06-01T10:00:00.000000Z"

# A plausible tail floor: inside the tail, above the window start.
_TAIL_FLOOR = "2024-06-01T11:45:00.000000Z"

# A boundary above the watermark, which is what a watermark-anchored boundary
# query returns by construction.
_BOUNDARY_ABOVE = "2024-06-01T12:30:00.000000Z"

# A fixture journal for the tests that derive the boundary from the anchor the
# code supplies, rather than taking it as an input. Twelve rows inside the
# lookback tail — (_WINDOW_START, _WATERMARK] — and four above the watermark.
#
# The shape is what makes the revert detectable: anchored at the watermark, the
# 5th row above the anchor does not exist among only four, so the read is
# unbounded above and reaches every new row. Anchored at _WINDOW_START, the 5th
# row above the anchor is the 5th *tail* row, so `until` lands below the
# watermark and the window holds no new rows at all — the stall.
_TAIL_ROWS = [
    f"2024-06-01T1{h}:{m:02d}:00.000000Z"
    for h, m in [(0, 10), (0, 30), (0, 50), (1, 5), (1, 15), (1, 25),
                 (1, 35), (1, 40), (1, 45), (1, 50), (1, 55), (1, 59)]
]
_ABOVE = [
    "2024-06-01T12:05:00.000000Z",
    "2024-06-01T12:15:00.000000Z",
    "2024-06-01T12:25:00.000000Z",
    "2024-06-01T12:35:00.000000Z",
]
_JOURNAL = _TAIL_ROWS + _ABOVE


def _ctx(row_cap: int = 500_000, lookback: timedelta = _LOOKBACK):
    return orchestrator._BucketContext(
        bucket=MonitoredBucket(name=_BUCKET, region="us-west-2"),
        bucket_name=_BUCKET,
        s3_client=MagicMock(),
        athena_client=MagicMock(),
        s3control_client=MagicMock(),
        state_bucket="state-bucket",
        athena_workgroup="wg",
        athena_output_location="s3://state-bucket/athena-results/",
        account_id="123456789012",
        batch_operations_role_arn="arn:aws:iam::123456789012:role/bops",
        kms_key_arn="",
        lookback=lookback,
        journal_read_row_cap=row_cap,
        max_batch_job_failures=4,
        max_concurrent_jobs=3,
        on_bucket_disabled=None,
        on_submission_failure=None,
    )


class _Window:
    """The outcome of one `_read_journal_window` call, in assertable form."""

    def __init__(self, returned, result, emitted, read_journal, boundary,
                 tail_count, tail_floor):
        self.returned = returned
        self.result = result
        self.emitted = emitted
        self.read_journal = read_journal
        self.boundary = boundary
        self.tail_count = tail_count
        self.tail_floor = tail_floor

    # -- the read window actually issued ------------------------------------

    @property
    def since(self):
        return self.read_journal.call_args.kwargs["since_timestamp"]

    @property
    def until(self):
        return self.read_journal.call_args.kwargs["until_timestamp"]

    # -- what the boundary query was asked ----------------------------------

    @property
    def boundary_anchor(self):
        return self.boundary.call_args.kwargs["since_timestamp"]

    @property
    def boundary_row_cap(self):
        return self.boundary.call_args.kwargs["row_cap"]

    # -- observability ------------------------------------------------------

    def audits(self, action):
        return [
            e for e in self.emitted
            if e.get("event") == "audit" and e.get("action") == action
        ]

    def errors(self, needle):
        return [
            e for e in self.emitted
            if e.get("event") == "error" and needle in e.get("cause", "")
        ]


def _fake_boundary_adapter(journal: list[str]):
    """Stand in for `find_row_count_boundary` over a fixture *journal*.

    Behaves like the real adapter: counts rows strictly above the
    ``since_timestamp`` it is **given** and returns the ``row_cap``-th of them,
    or ``None`` when fewer than that many exist.

    This is what makes a test non-vacuous. Handing the boundary in as a fixed
    value asserts nothing about the anchor, because the returned window is then
    the test's own input — reverting the watermark anchor leaves such a test
    green. Deriving the boundary from the argument the code actually passed means
    a wrong anchor produces a wrong window, which the caller can then assert on.
    """
    def _boundary(*, since_timestamp, row_cap, **_kwargs):
        above = [
            r for r in journal
            if since_timestamp is None or r > since_timestamp
        ]
        if len(above) < row_cap:
            return None
        return above[row_cap - 1]

    return MagicMock(side_effect=_boundary)


def _fake_tail_floor_adapter(journal: list[str]):
    """Stand in for `find_tail_floor` over a fixture *journal*.

    Behaves like the real adapter: returns the ``tail_allowance``-th row counting
    **backwards** from the watermark within ``(window_start, watermark]``, or
    ``None`` when the tail holds fewer rows than that. Used by the caller as an
    exclusive lower bound.
    """
    def _floor(*, window_start, watermark, tail_allowance, **_kwargs):
        tail = sorted(
            r for r in journal if window_start < r <= watermark
        )
        # The row one *past* the allowance, counting back from the watermark:
        # the bound is exclusive, so this is the newest row NOT admitted.
        if len(tail) <= tail_allowance:
            return None
        return tail[-(tail_allowance + 1)]

    return MagicMock(side_effect=_floor)


def _read_window(
    *,
    watermark: str = _WATERMARK,
    row_cap: int = 500_000,
    lookback: timedelta = _LOOKBACK,
    tail_rows: int | Exception = 0,
    tail_floor: str | None | Exception = _TAIL_FLOOR,
    boundary: str | None | Exception = None,
    journal: list[str] | None = None,
    ops: list | None = None,
) -> _Window:
    """Call `_read_journal_window` with every Athena-facing call controlled.

    ``tail_rows``, ``tail_floor`` and ``boundary`` accept an exception instance
    to exercise a failure fallback instead of a value.

    Pass ``journal`` — a sorted list of canonical watermark strings — instead of
    ``boundary`` to have the boundary derived from the anchor the code supplies,
    the way the real adapter would. See :func:`_fake_boundary_adapter`.
    """
    def _mock(value):
        if isinstance(value, Exception):
            return MagicMock(side_effect=value)
        return MagicMock(return_value=value)

    mock_tail_count = _mock(tail_rows)
    if journal is not None:
        assert boundary is None, "pass journal or boundary, not both"
        mock_boundary = _fake_boundary_adapter(journal)
        mock_tail_floor = _fake_tail_floor_adapter(journal)
    else:
        mock_boundary = _mock(boundary)
        mock_tail_floor = _mock(tail_floor)
    mock_read_journal = MagicMock(return_value=(ops if ops is not None else [], []))

    result = orchestrator._BucketResult()
    emitted: list = []

    with (
        patch("src.orchestrator.athena_journal_adapter.find_row_count_boundary",
              mock_boundary),
        patch("src.orchestrator.athena_journal_adapter.find_tail_row_count",
              mock_tail_count),
        patch("src.orchestrator.athena_journal_adapter.find_tail_floor",
              mock_tail_floor),
        patch("src.orchestrator.athena_journal_adapter.read_journal",
              mock_read_journal),
        patch("src.orchestrator.observability.emit", side_effect=emitted.append),
    ):
        returned = orchestrator._read_journal_window(
            _ctx(row_cap=row_cap, lookback=lookback),
            watermark,
            result,
            MagicMock(),
        )

    return _Window(
        returned, result, emitted, mock_read_journal,
        mock_boundary, mock_tail_count, mock_tail_floor,
    )


# ---------------------------------------------------------------------------
# Requirement 1.1: the boundary is anchored at the watermark
# ---------------------------------------------------------------------------


class TestBoundaryAnchor:
    def test_boundary_is_anchored_at_the_watermark_not_the_window_start(self):
        """The fix itself. Anchoring at ``watermark - lookback`` is what let the
        tail consume the boundary and produce a window with no new rows."""
        w = _read_window(tail_rows=0)
        assert w.boundary_anchor == _WATERMARK
        assert w.boundary_anchor != _WINDOW_START

    def test_epoch_watermark_anchors_the_boundary_at_none(self):
        """A bucket's first run reads from the beginning, as before."""
        w = _read_window(watermark=EPOCH_WATERMARK)
        assert w.boundary_anchor is None

    def test_boundary_row_cap_is_the_new_row_budget_not_the_whole_cap(self):
        """The boundary bounds only the rows above the watermark; the tail's
        share of the cap is spent separately."""
        w = _read_window(row_cap=500_000, tail_rows=100_000)
        assert w.boundary_row_cap == 400_000

    def test_empty_tail_gives_the_boundary_the_whole_cap(self):
        w = _read_window(row_cap=500_000, tail_rows=0)
        assert w.boundary_row_cap == 500_000


# ---------------------------------------------------------------------------
# Requirement 2.4: the lower bound, and when it is raised
# ---------------------------------------------------------------------------


class TestLowerBound:
    def test_tail_that_fits_leaves_the_lower_bound_at_the_window_start(self):
        """The ordinary case: the whole lookback window is still re-scanned."""
        w = _read_window(row_cap=500_000, tail_rows=100_000)
        assert w.since == _WINDOW_START
        assert w.result.tail_shortened is False
        w.tail_floor.assert_not_called()

    def test_tail_exactly_at_its_allowance_is_not_shortened(self):
        w = _read_window(row_cap=500_000, tail_rows=400_000)
        assert w.since == _WINDOW_START
        assert w.result.tail_shortened is False
        w.tail_floor.assert_not_called()

    def test_tail_that_does_not_fit_raises_the_lower_bound(self):
        w = _read_window(row_cap=500_000, tail_rows=620_000)
        assert w.since == _TAIL_FLOOR
        assert w.since > _WINDOW_START
        assert w.result.tail_shortened is True

    def test_tail_floor_is_asked_for_the_tail_allowance(self):
        w = _read_window(row_cap=500_000, tail_rows=620_000)
        assert w.tail_floor.call_args.kwargs["tail_allowance"] == 400_000
        assert w.tail_floor.call_args.kwargs["window_start"] == _WINDOW_START
        assert w.tail_floor.call_args.kwargs["watermark"] == _WATERMARK

    @pytest.mark.parametrize("row_cap", [2, 5, 10, 100])
    def test_the_bound_admits_exactly_the_tail_allowance(self, row_cap):
        """Resolved against the fixture journal rather than asserted on the query
        shape: the number of tail rows the read admits must equal the allowance.

        The off-by-one this guards against was invisible to an offset assertion,
        because an exclusive bound naming the allowance-th row admits one fewer.
        At an allowance of 1 it admitted none, discarding the whole re-scan window
        while the run advanced past it.
        """
        from src.core.row_cap_validation import split_row_budget

        tail_allowance, _ = split_row_budget(row_cap, len(_TAIL_ROWS))
        w = _read_window(row_cap=row_cap, tail_rows=len(_TAIL_ROWS), journal=_JOURNAL)
        assert w.returned is not None
        _, since, _until = w.returned

        admitted_tail = [r for r in _TAIL_ROWS if since is None or r > since]
        expected = min(tail_allowance, len(_TAIL_ROWS))
        assert len(admitted_tail) == expected, (
            f"admitted {len(admitted_tail)} tail rows against an allowance of "
            f"{tail_allowance}"
        )

    def test_tail_floor_of_none_keeps_the_window_start(self):
        """The tail turned out smaller than its allowance between the count and
        the floor lookup, so nothing needs truncating."""
        w = _read_window(row_cap=500_000, tail_rows=620_000, tail_floor=None)
        assert w.since == _WINDOW_START
        assert w.result.tail_shortened is False

    def test_zero_tail_allowance_skips_the_bucket_rather_than_dropping_the_tail(self):
        """Unreachable through configuration — `MIN_JOURNAL_READ_ROW_CAP` is
        enforced by both the template and the runtime coercion — but it must fail
        safe if a change ever makes it reachable.

        Bounding at the watermark here would drop the whole re-scan window and
        permanently lose any late arrival in it, which is exactly what the
        floor-failure path refuses to do. Skipping loses nothing.
        """
        w = _read_window(row_cap=1, tail_rows=12)
        assert w.returned is None
        assert w.result.errored is True
        assert w.result.tail_shortened is False
        w.tail_floor.assert_not_called()
        w.read_journal.assert_not_called()
        assert w.errors("Lookback tail has no allowance") != []

    def test_minimum_row_cap_leaves_both_ranges_a_share(self):
        """The smallest cap the Solution accepts still splits into two non-zero
        shares, which is why it is the minimum."""
        from src.core.manifest_strategy import MIN_JOURNAL_READ_ROW_CAP
        from src.core.row_cap_validation import split_row_budget

        allowance, budget = split_row_budget(MIN_JOURNAL_READ_ROW_CAP, 1_000)
        assert allowance >= 1
        assert budget >= 1

    def test_until_is_unchanged_by_the_lower_bound(self):
        w = _read_window(
            row_cap=500_000, tail_rows=620_000, boundary=_BOUNDARY_ABOVE,
        )
        assert w.until == _BOUNDARY_ABOVE


# ---------------------------------------------------------------------------
# Requirement 5.3: the extra queries are skipped when they cannot matter
# ---------------------------------------------------------------------------


class TestTailQueryShortCircuits:
    def test_epoch_watermark_skips_both_extra_queries(self):
        """Always a bucket's first run: nothing exists below the watermark."""
        w = _read_window(watermark=EPOCH_WATERMARK)
        w.tail_count.assert_not_called()
        w.tail_floor.assert_not_called()

    def test_zero_lookback_skips_both_extra_queries(self):
        """The tail range is empty by definition, so counting it would spend an
        Athena query to learn zero."""
        w = _read_window(lookback=timedelta(0))
        w.tail_count.assert_not_called()
        w.tail_floor.assert_not_called()
        # window_start equals the watermark, so the read is strictly above it.
        assert w.since == _WATERMARK

    def test_non_empty_tail_pays_one_extra_query_and_not_the_second(self):
        w = _read_window(row_cap=500_000, tail_rows=100_000)
        assert w.tail_count.call_count == 1
        assert w.tail_floor.call_count == 0

    def test_tail_count_is_asked_for_the_tail_range(self):
        w = _read_window(row_cap=500_000, tail_rows=100_000)
        assert w.tail_count.call_args.kwargs["window_start"] == _WINDOW_START
        assert w.tail_count.call_args.kwargs["watermark"] == _WATERMARK


# ---------------------------------------------------------------------------
# Requirement 4: the tripwire
# ---------------------------------------------------------------------------


class TestNonAdvancingBoundaryTripwire:
    def test_boundary_at_the_watermark_skips_the_bucket(self):
        w = _read_window(boundary=_WATERMARK)
        assert w.returned is None
        assert w.result.errored is True
        w.read_journal.assert_not_called()

    def test_boundary_below_the_watermark_skips_the_bucket(self):
        w = _read_window(boundary="2024-06-01T11:00:00.000000Z")
        assert w.returned is None
        w.read_journal.assert_not_called()

    def test_tripwire_error_names_both_parameters_and_the_tail_size(self):
        w = _read_window(boundary=_WATERMARK, tail_rows=620_000)
        errors = w.errors("Row-cap boundary did not advance")
        assert len(errors) == 1
        cause = errors[0]["cause"]
        assert _WATERMARK in cause
        assert "checkpoint_watermark" in cause
        assert "journal_until" in cause
        assert "tail_rows=620000" in cause

    def test_tripwire_is_distinct_from_tail_shortening(self):
        """Requirement 4.2. A shortened tail is expected under backlog and does
        not fire the tripwire; a non-advancing boundary is a defect and does not
        report itself as shortening."""
        shortened = _read_window(
            row_cap=500_000, tail_rows=620_000, boundary=_BOUNDARY_ABOVE,
        )
        assert shortened.returned is not None
        assert shortened.errors("Row-cap boundary did not advance") == []
        assert shortened.errors("Lookback tail shortened") != []

        tripped = _read_window(row_cap=500_000, tail_rows=0, boundary=_WATERMARK)
        assert tripped.errors("Row-cap boundary did not advance") != []
        assert tripped.errors("Lookback tail shortened") == []

    def test_boundary_above_the_watermark_does_not_fire(self):
        w = _read_window(boundary=_BOUNDARY_ABOVE)
        assert w.returned is not None
        assert w.errors("Row-cap boundary did not advance") == []


# ---------------------------------------------------------------------------
# Requirement 5.1, 5.2: the failure fallbacks
# ---------------------------------------------------------------------------


class TestFailureFallbacks:
    def test_tail_count_failure_assumes_the_allowance_not_zero(self):
        """Conservative direction: assuming zero would hand the whole cap to
        new rows and, on a bucket with a real backlog, let the tail consume it
        — the condition this spec removes."""
        w = _read_window(tail_rows=RuntimeError("boom"))
        expected_allowance = math.floor(500_000 * TAIL_ROW_BUDGET_FRACTION)
        assert w.boundary_row_cap == 500_000 - expected_allowance
        assert w.errors("Lookback-tail row count failed") != []

    def test_tail_count_failure_still_bounds_the_tail(self):
        """The assumed count equals the allowance, so it does not exceed it and
        the ordinary truncation test would not fire. The floor is raised anyway,
        because the tail's real size is exactly what the failed query was asked
        for: leaving the bound at the window start would read all of it, however
        large, on top of the reserved new-row budget."""
        w = _read_window(tail_rows=RuntimeError("boom"))
        w.tail_floor.assert_called_once()
        assert w.since == _TAIL_FLOOR
        assert w.since > _WINDOW_START
        assert w.result.tail_shortened is True

    def test_tail_count_failure_reports_the_shortening_it_caused(self):
        """Bounding a tail that might have fitted is a real reduction in
        late-arrival tolerance, so it is reported like any other."""
        w = _read_window(tail_rows=RuntimeError("boom"))
        assert w.errors("Lookback tail shortened") != []

    def test_tail_count_failure_still_reads_the_journal(self):
        w = _read_window(tail_rows=RuntimeError("boom"))
        assert w.returned is not None
        w.read_journal.assert_called_once()

    def test_tail_floor_failure_skips_the_bucket_and_reads_nothing(self):
        """Neither alternative is acceptable, so the run declines to read.

        Keeping the nominal window start reads the tail's true size, unbounded at
        this point by construction. Bounding at the watermark skips the whole
        lookback window, and since ``is_eligible`` permanently rejects anything at
        or below ``watermark - lookback`` while this run would advance the
        watermark, a genuine late arrival in the oldest part of the window is lost
        for good.
        """
        w = _read_window(
            row_cap=500_000, tail_rows=620_000,
            tail_floor=RuntimeError("boom"),
        )
        assert w.returned is None
        assert w.result.errored is True
        w.read_journal.assert_not_called()

    def test_tail_floor_failure_leaves_the_checkpoint_able_to_retry(self):
        """Nothing was read and no shortening is claimed, so the next interval
        re-scans the whole window with a correct floor. The backlog is untouched
        rather than partially consumed."""
        w = _read_window(
            row_cap=500_000, tail_rows=620_000,
            tail_floor=RuntimeError("boom"),
        )
        assert w.result.tail_shortened is False, (
            "no tail was shortened — the run declined to read at all"
        )
        assert w.errors("Lookback tail shortened") == []

    def test_tail_floor_failure_names_the_cause_and_the_reasoning(self):
        w = _read_window(
            row_cap=500_000, tail_rows=620_000,
            tail_floor=RuntimeError("boom"),
        )
        errors = w.errors("Lookback-tail floor lookup failed")
        assert len(errors) == 1
        cause = errors[0]["cause"]
        assert "Skipping this bucket" in cause
        assert "620000" in cause
        assert "400000" in cause
        assert "checkpoint is left" in cause

    def test_tail_count_failure_then_floor_failure_also_skips(self):
        """The compounding case: a correlated Athena failure takes out both
        queries. It must not fall through to an unbounded read."""
        w = _read_window(
            tail_rows=RuntimeError("count boom"),
            tail_floor=RuntimeError("floor boom"),
        )
        assert w.returned is None
        assert w.result.errored is True
        w.read_journal.assert_not_called()

    def test_boundary_failure_proceeds_uncapped(self):
        """Existing behavior, unchanged."""
        w = _read_window(boundary=RuntimeError("boom"))
        assert w.until is None
        assert w.result.capped is False
        assert w.returned is not None
        assert w.errors("Row-count boundary check failed") != []


# ---------------------------------------------------------------------------
# Requirement 1.3, 1.5, 7.2: the regression test.
#
# Asserts on the read window, not on the absence of an exception: the broken
# code raised nothing, it just produced a window that could not progress.
# ---------------------------------------------------------------------------


class TestLivelockRegression:
    def test_tail_larger_than_row_cap_still_reads_above_the_watermark(self):
        """The original livelock's conditions: the lookback tail alone holds more
        rows than ``JournalReadRowCap``.

        The boundary is derived from the anchor the code passes, over a journal
        of 12 tail rows below the watermark and 4 above it. Reverting the
        watermark anchor makes the boundary the 5th row above
        ``watermark - lookback``, which is a tail row, so ``until`` lands below
        the watermark and this fails.
        """
        w = _read_window(row_cap=5, tail_rows=12, journal=_JOURNAL)
        assert w.returned is not None
        assert w.until is not None
        assert w.until > _WATERMARK, (
            "the read must extend above the watermark, or the run cannot advance "
            "the checkpoint"
        )

    def test_tail_larger_than_row_cap_yields_a_non_empty_eligible_set(self):
        """The window contains a row above the watermark, so a run reading it
        has something to submit rather than only already-processed rows."""
        new_row = _op(key="new.txt", event_time=_ABOVE[0])
        w = _read_window(
            row_cap=5, tail_rows=12, journal=_JOURNAL, ops=[new_row],
        )
        ops, since, until = w.returned
        assert ops == [new_row]
        assert until > _WATERMARK
        assert since is None or since <= _WATERMARK

    def test_the_window_admits_only_what_the_budget_allows(self):
        """Requirement 2.1 asserted on the resolved window rather than on the
        arithmetic: resolving the read's bounds against the fixture journal
        admits at most ``row_cap`` rows, and at least one above the
        watermark."""
        w = _read_window(row_cap=5, tail_rows=12, journal=_JOURNAL)
        _, since, until = w.returned
        admitted = [
            r for r in _JOURNAL
            if (since is None or r > since) and (until is None or r <= until)
        ]
        assert len(admitted) <= 5, f"read {len(admitted)} rows against a cap of 5"
        assert [r for r in admitted if r > _WATERMARK], "no new rows admitted"

    def test_uncapped_boundary_still_reads_above_the_watermark(self):
        """Fewer new rows than the budget, so the real adapter's shape returns no
        boundary and the read is unbounded above."""
        w = _read_window(row_cap=500_000, tail_rows=12, journal=_JOURNAL)
        assert w.returned is not None
        assert w.until is None


# ---------------------------------------------------------------------------
# Requirement 1.4: the backlog drains in a bounded number of runs.
#
# The property the defect violated. A single run progressing is necessary but
# not sufficient — the broken code's first run progressed too, and every run
# after it did not.
# ---------------------------------------------------------------------------


def _op(key: str, event_time: str):
    from datetime import datetime

    from src.core.models import TaggingOperation
    from src.core.watermark import parse_watermark

    assert isinstance(parse_watermark(event_time), datetime)
    return TaggingOperation(
        source_bucket=_BUCKET,
        object_key=key,
        resulting_tag_set={"replicate": "yes"},
        sequence_number=key,
        operation="PutObjectTagging",
        event_time=parse_watermark(event_time),
    )


class TestBacklogDrains:
    # row_cap=2 is MIN_JOURNAL_READ_ROW_CAP, the smallest the Solution accepts,
    # and the tightest budget a drain can run on: a new-row budget of 1.
    @pytest.mark.parametrize("total_rows,row_cap", [(12, 5), (100, 7), (37, 2)])
    def test_every_run_advances_and_the_backlog_clears_in_bounded_runs(
        self, total_rows, row_cap,
    ):
        """Successive runs against a fixed journal, each resolving its own read
        window and advancing the watermark to the newest row that window
        admitted.

        The boundary comes from `_fake_boundary_adapter` over the whole journal,
        so it is derived from the anchor the code passes rather than supplied by
        the test. That is what makes the drain property a property of the
        production code: reverting the watermark anchor produces a window with no
        new rows in it, the loop makes no progress, and the run-count assertion
        trips.

        The tail below the starting watermark is far larger than the cap on every
        run, so the livelock's conditions hold throughout rather than only at the
        start.
        """
        from datetime import UTC, datetime, timedelta as td

        from src.core.row_cap_validation import split_row_budget
        from src.core.watermark import to_watermark

        base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        tail_rows = 10 * row_cap  # far more than the cap, every run

        # Rows below the starting watermark are the tail; rows above it are the
        # backlog to drain. Both live in one journal, because that is what the
        # boundary query sees.
        tail = [to_watermark(base - td(seconds=i + 1)) for i in range(tail_rows)]
        backlog = [to_watermark(base + td(seconds=i + 1)) for i in range(total_rows)]
        journal = sorted(tail) + backlog

        watermark = to_watermark(base)
        _, new_row_budget = split_row_budget(row_cap, tail_rows)
        max_runs = math.ceil(total_rows / new_row_budget)

        runs = 0
        remaining = list(backlog)
        while remaining:
            runs += 1
            assert runs <= max_runs, (
                f"backlog of {total_rows} rows did not clear within {max_runs} "
                f"runs at row_cap={row_cap}; stuck at watermark {watermark}"
            )

            w = _read_window(
                watermark=watermark, row_cap=row_cap,
                tail_rows=tail_rows, journal=journal,
            )
            assert w.returned is not None, "the run must not skip the bucket"
            _, since, until = w.returned

            # Resolve the window the code produced against the journal.
            admitted = [
                r for r in journal
                if (since is None or r > since) and (until is None or r <= until)
            ]
            assert len(admitted) <= row_cap, (
                f"read {len(admitted)} rows against a cap of {row_cap}"
            )

            new_rows = [r for r in admitted if r > watermark]
            assert new_rows, (
                "every run must read at least one row above the watermark while "
                "unprocessed rows remain — this is the stall"
            )
            assert len(new_rows) <= new_row_budget

            # The checkpoint advances to the newest row read.
            new_watermark = new_rows[-1]
            assert new_watermark > watermark, "the watermark must advance"
            watermark = new_watermark
            remaining = [r for r in remaining if r > watermark]

        assert runs <= max_runs

    def test_a_run_with_no_new_rows_is_the_only_zero_progress_case(self):
        """With nothing above the watermark the run legitimately submits nothing.
        It is distinguishable from the stall because the window it read was
        unbounded above, not bounded at or below the watermark."""
        w = _read_window(row_cap=5, tail_rows=12, journal=_TAIL_ROWS, ops=[])
        ops, _, until = w.returned
        assert ops == []
        assert until is None


# ---------------------------------------------------------------------------
# Requirement 3: shortening the tail is visible.
#
# `log_audit` merges `details` flat into the entry, so an audit field is read as
# `entry["tail_rows"]`, not `entry["details"]["tail_rows"]`.
# ---------------------------------------------------------------------------


class TestJournalReadCappedFields:
    def test_capped_run_carries_the_budget_split(self):
        w = _read_window(
            row_cap=500_000, tail_rows=100_000, boundary=_BOUNDARY_ABOVE,
        )
        audits = w.audits("journal_read_capped")
        assert len(audits) == 1
        entry = audits[0]
        assert entry["tail_rows"] == 100_000
        assert entry["new_row_budget"] == 400_000
        assert entry["tail_shortened"] is False

    def test_capped_run_keeps_every_existing_field(self):
        w = _read_window(
            row_cap=500_000, tail_rows=100_000, boundary=_BOUNDARY_ABOVE,
        )
        entry = w.audits("journal_read_capped")[0]
        assert entry["source_bucket"] == _BUCKET
        assert entry["row_cap"] == 500_000
        assert entry["until_timestamp"] == _BOUNDARY_ABOVE
        assert entry["since_timestamp"] == _WINDOW_START

    def test_since_timestamp_reports_the_bound_actually_used(self):
        """A shortened tail is visible from this entry alone, by comparing
        since_timestamp against watermark - lookback."""
        w = _read_window(
            row_cap=500_000, tail_rows=620_000, boundary=_BOUNDARY_ABOVE,
        )
        entry = w.audits("journal_read_capped")[0]
        assert entry["since_timestamp"] == _TAIL_FLOOR
        assert entry["since_timestamp"] != _WINDOW_START
        assert entry["tail_shortened"] is True
        assert entry["new_row_budget"] == 100_000

    def test_uncapped_run_emits_no_entry(self):
        w = _read_window(row_cap=500_000, tail_rows=620_000, boundary=None)
        assert w.audits("journal_read_capped") == []


class TestTailShorteningError:
    def test_fires_only_when_the_tail_is_shortened(self):
        shortened = _read_window(row_cap=500_000, tail_rows=620_000)
        assert len(shortened.errors("Lookback tail shortened")) == 1

        fits = _read_window(row_cap=500_000, tail_rows=100_000)
        assert fits.errors("Lookback tail shortened") == []

    def test_does_not_fire_for_a_tail_at_exactly_its_allowance(self):
        """The boundary case between the two above. A measured tail of exactly
        the allowance is read whole; only an assumed one is bounded."""
        w = _read_window(row_cap=500_000, tail_rows=400_000)
        assert w.errors("Lookback tail shortened") == []
        assert w.since == _WINDOW_START

    def test_names_all_five_fields(self):
        w = _read_window(row_cap=500_000, tail_rows=620_000)
        entry = w.errors("Lookback tail shortened")[0]
        assert entry["bucket"] == _BUCKET
        cause = entry["cause"]
        assert "tail_rows=620000" in cause
        assert "tail_allowance=400000" in cause
        assert _TAIL_FLOOR in cause
        assert "journal_lookback_seconds=7200" in cause

    def test_is_an_error_not_an_audit_entry(self):
        """A reduction in late-arrival tolerance forced by backlog is a loss,
        not a decision the Solution is entitled to record as policy."""
        w = _read_window(row_cap=500_000, tail_rows=620_000)
        assert w.errors("Lookback tail shortened") != []
        assert w.audits("journal_tail_shortened") == []


class TestJournalTailShortenedMetric:
    def _datums(self, buckets):
        from src.adapters.metrics_publisher import _build_metric_data
        from src.core.models import RunResult

        return _build_metric_data(RunResult(buckets=buckets))

    def _bm(self, name, *, tail_shortened=False):
        from src.core.models import BucketMetrics

        return BucketMetrics(
            source_bucket=name,
            ops_read=5,
            matched=5,
            submitted=1,
            errored=False,
            tail_shortened=tail_shortened,
        )

    def test_published_per_bucket_when_shortened(self):
        datums = self._datums([self._bm("my-bucket", tail_shortened=True)])
        published = [
            d for d in datums if d["MetricName"] == "JournalTailShortened"
        ]
        assert len(published) == 1
        assert published[0]["Value"] == 1.0
        assert published[0]["Unit"] == "Count"
        assert {"Name": "SourceBucket", "Value": "my-bucket"} in published[0]["Dimensions"]

    def test_not_published_when_the_tail_fits(self):
        """Emitted only when it happened, so an alarm can treat missing data as
        not breaching."""
        datums = self._datums([self._bm("my-bucket")])
        assert not [d for d in datums if d["MetricName"] == "JournalTailShortened"]

    def test_one_buckets_shortening_does_not_affect_another(self):
        """Requirement 3.5."""
        datums = self._datums([
            self._bm("bucket-a", tail_shortened=True),
            self._bm("bucket-b", tail_shortened=False),
        ])
        published = [
            d for d in datums if d["MetricName"] == "JournalTailShortened"
        ]
        assert len(published) == 1
        assert {"Name": "SourceBucket", "Value": "bucket-a"} in published[0]["Dimensions"]

    def test_shortening_is_not_reported_as_an_error(self):
        datums = self._datums([self._bm("my-bucket", tail_shortened=True)])
        errors = [d for d in datums if d["MetricName"] == "BucketErrors"]
        assert len(errors) == 1
        assert errors[0]["Value"] == 0.0
