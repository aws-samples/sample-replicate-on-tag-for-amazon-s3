"""Mocked integration tests for src/adapters/athena_journal_adapter.py.

Covers Requirements 4.1, 4.3, 4.4, 12.3, 12.4:
- Journal queried only for buckets with derived rules (4.1)
- Resume from persisted sequence_number via after_sequence parameter (4.3)
- Athena query failure leaves checkpoint unchanged (4.4, 12.3)
- AccessDenied on journal read reported and checkpoint unchanged (12.3)
- Object tag read-permission failures surfaced as report-and-continue (12.4)
- Per-record skips for missing key or resulting_tags (4.5)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError

from src.adapters.athena_journal_adapter import (
    JournalReadError,
    _build_boundary_query,
    _build_query,
    _build_tail_count_query,
    _build_tail_floor_query,
    _parse_row,
    find_row_count_boundary,
    find_tail_floor,
    find_tail_row_count,
    read_journal,
)
from src.core.models import TaggingOperation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = "2024-01-15 10:00:00 UTC"
_BUCKET = "source-bucket"
_WORKGROUP = "test-workgroup"
_OUTPUT = "s3://scratch/athena-results/"
_EXEC_ID = "query-exec-id-abc123"

# bucket_namespace derived as "b_" + _BUCKET.replace(".", "_")
_BUCKET_NAMESPACE = "b_" + _BUCKET.replace(".", "_")


def _make_athena_client(
    state: str = "SUCCEEDED",
    failure_reason: str = "",
    rows: list[list[str]] | None = None,
    start_raises: ClientError | None = None,
    poll_raises: ClientError | None = None,
    results_raises: ClientError | None = None,
) -> MagicMock:
    """Return a mocked Athena boto3 client."""
    client = MagicMock()

    if start_raises is not None:
        client.start_query_execution.side_effect = start_raises
    else:
        client.start_query_execution.return_value = {
            "QueryExecutionId": _EXEC_ID
        }

    if poll_raises is not None:
        client.get_query_execution.side_effect = poll_raises
    else:
        status: dict[str, Any] = {"State": state}
        if failure_reason:
            status["StateChangeReason"] = failure_reason
        client.get_query_execution.return_value = {
            "QueryExecution": {"Status": status}
        }

    if results_raises is not None:
        client.get_query_results.side_effect = results_raises
    elif rows is not None:
        # Build Athena-style ResultSet with a header row followed by data rows
        header = {
            "Data": [
                {"VarCharValue": col}
                for col in [
                    "bucket",
                    "key",
                    "version_id",
                    "operation",
                    "resulting_tags",
                    "sequence_number",
                    "event_time",
                ]
            ]
        }
        data_rows = [
            {
                "Data": [{"VarCharValue": cell} for cell in row]
            }
            for row in rows
        ]
        client.get_query_results.return_value = {
            "ResultSet": {"Rows": [header] + data_rows},
            "NextToken": None,
        }
    else:
        client.get_query_results.return_value = {
            "ResultSet": {"Rows": []},
        }

    return client


def _make_client_error(code: str, message: str = "error") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "operation",
    )


_DEFAULT_TAGS = {"env": "prod"}


def _make_row(
    bucket: str = _BUCKET,
    key: str = "path/obj.txt",
    version_id: str = "v1",
    operation: str = "PutObjectTagging",
    tags: dict | None = None,
    seq: str = "seq-001",
    event_time: str = _NOW,
    use_default_tags: bool = True,
) -> list[str]:
    """Build an Athena result row.

    Pass ``tags={}`` and ``use_default_tags=False`` to produce a row with an
    empty resulting_tags JSON object.  By default ``None`` uses the sentinel
    default tag set so the helper is convenient for the common case.
    """
    if tags is None and use_default_tags:
        resolved_tags = _DEFAULT_TAGS
    else:
        resolved_tags = tags if tags is not None else {}
    tags_json = json.dumps(resolved_tags)
    return [bucket, key, version_id, operation, tags_json, seq, event_time]


# ---------------------------------------------------------------------------
# Query construction
# ---------------------------------------------------------------------------


_SINCE_WM = "2024-01-01T00:00:50.000000Z"
_SINCE_ATHENA = "2024-01-01 00:00:50.000000"


class TestBuildQuery:
    def test_no_since_omits_predicate(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        assert "bucket = 'source-bucket'" in query
        assert "UPDATE_METADATA" in query
        assert "record_timestamp >" not in query

    def test_since_adds_record_timestamp_predicate(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=_SINCE_WM)
        assert f"record_timestamp > timestamp '{_SINCE_ATHENA}'" in query

    def test_since_predicate_uses_no_sequence_number(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=_SINCE_WM)
        assert "sequence_number >" not in query

    def test_bucket_name_escaped(self):
        query = _build_query(_BUCKET_NAMESPACE, "buck'et", since_timestamp=None)
        assert "buck''et" in query

    def test_table_includes_namespace(self):
        query = _build_query("my_namespace", _BUCKET, since_timestamp=None)
        assert "my_namespace" in query

    def test_result_ordered_by_record_timestamp_asc(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        assert "ORDER BY record_timestamp ASC" in query

    def test_selects_expected_columns(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        for col in ("bucket", "key", "version_id", "operation", "resulting_tags",
                    "sequence_number", "event_time"):
            assert col in query

    def test_uses_record_type_alias(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        assert "record_type" in query

    def test_uses_object_tags_alias(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        assert "object_tags" in query

    def test_uses_record_timestamp_alias(self):
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        assert "record_timestamp" in query

    # --- until_timestamp (row-count cap) -----------------------------------

    def test_no_until_omits_upper_bound_predicate(self):
        """Default (no cap needed) — no upper-bound predicate at all."""
        query = _build_query(_BUCKET_NAMESPACE, _BUCKET, since_timestamp=None)
        assert "record_timestamp <=" not in query

    def test_until_adds_inclusive_upper_bound_predicate(self):
        query = _build_query(
            _BUCKET_NAMESPACE, _BUCKET, since_timestamp=None, until_timestamp=_SINCE_WM
        )
        assert f"record_timestamp <= timestamp '{_SINCE_ATHENA}'" in query

    def test_since_and_until_both_present(self):
        """Both bounds can be present simultaneously — a capped run on a
        subsequent interval still has its own since_timestamp lower bound
        plus the cap's upper bound."""
        until_wm = "2024-01-02T00:00:00.000000Z"
        until_athena = "2024-01-02 00:00:00.000000"
        query = _build_query(
            _BUCKET_NAMESPACE, _BUCKET, since_timestamp=_SINCE_WM, until_timestamp=until_wm
        )
        assert f"record_timestamp > timestamp '{_SINCE_ATHENA}'" in query
        assert f"record_timestamp <= timestamp '{until_athena}'" in query


# ---------------------------------------------------------------------------
# Row-count cap boundary query (code-review-remediation
# verification-notes.md "scaling risk" finding)
# ---------------------------------------------------------------------------


class TestBuildBoundaryQuery:
    def test_uses_offset_limit_one(self):
        """OFFSET (row_cap - 1) LIMIT 1 — 1-indexed row_cap-th row."""
        query = _build_boundary_query(_BUCKET_NAMESPACE, _BUCKET, None, row_cap=500_000)
        assert "OFFSET 499999 LIMIT 1" in query

    def test_selects_only_record_timestamp(self):
        query = _build_boundary_query(_BUCKET_NAMESPACE, _BUCKET, None, row_cap=10)
        assert query.strip().startswith("SELECT record_timestamp")

    def test_ordered_by_record_timestamp_asc(self):
        query = _build_boundary_query(_BUCKET_NAMESPACE, _BUCKET, None, row_cap=10)
        assert "ORDER BY record_timestamp ASC" in query

    def test_since_timestamp_included_when_provided(self):
        query = _build_boundary_query(_BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, row_cap=10)
        assert f"record_timestamp > timestamp '{_SINCE_ATHENA}'" in query

    def test_no_since_timestamp_omits_lower_bound(self):
        query = _build_boundary_query(_BUCKET_NAMESPACE, _BUCKET, None, row_cap=10)
        assert "record_timestamp >" not in query

    def test_bucket_name_escaped(self):
        query = _build_boundary_query(_BUCKET_NAMESPACE, "buck'et", None, row_cap=10)
        assert "buck''et" in query

    def test_filters_to_update_metadata(self):
        query = _build_boundary_query(_BUCKET_NAMESPACE, _BUCKET, None, row_cap=10)
        assert "UPDATE_METADATA" in query


class TestFindRowCountBoundary:
    def _make_client(self, rows: list[list[str]] | None):
        """rows=None simulates the boundary-not-reached (empty result) case;
        rows=[[ts]] simulates a found boundary."""
        return _make_athena_client(rows=rows)

    def test_returns_none_when_window_smaller_than_cap(self):
        client = self._make_client(rows=None)
        result = find_row_count_boundary(
            client, _BUCKET, since_timestamp=None, row_cap=500_000,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert result is None

    def test_returns_canonical_watermark_when_boundary_found(self):
        client = self._make_client(rows=[["2024-01-15 10:00:00.000000"]])
        result = find_row_count_boundary(
            client, _BUCKET, since_timestamp=None, row_cap=500_000,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert result == "2024-01-15T10:00:00.000000Z"

    def test_raises_value_error_for_non_positive_row_cap(self):
        client = self._make_client(rows=None)
        with pytest.raises(ValueError):
            find_row_count_boundary(
                client, _BUCKET, since_timestamp=None, row_cap=0,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )
        with pytest.raises(ValueError):
            find_row_count_boundary(
                client, _BUCKET, since_timestamp=None, row_cap=-5,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )

    def test_query_uses_offset_for_row_cap(self):
        client = self._make_client(rows=None)
        find_row_count_boundary(
            client, _BUCKET, since_timestamp=None, row_cap=250_000,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert "OFFSET 249999 LIMIT 1" in query_string

    def test_query_failure_raises(self):
        client = self._make_athena_client_for_boundary_failure()
        with pytest.raises(ValueError):
            find_row_count_boundary(
                client, _BUCKET, since_timestamp=None, row_cap=500_000,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )

    def _make_athena_client_for_boundary_failure(self):
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": _EXEC_ID}
        client.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "boom"}
            }
        }
        return client

    def test_unparseable_timestamp_raises(self):
        client = self._make_client(rows=[["not-a-timestamp"]])
        with pytest.raises(ValueError):
            find_row_count_boundary(
                client, _BUCKET, since_timestamp=None, row_cap=500_000,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )


# ---------------------------------------------------------------------------
# Lookback-tail queries (row-cap-forward-progress).
#
# find_tail_row_count sizes the tail so the row budget can be split between it
# and the rows above the watermark; find_tail_floor raises the read's lower
# bound when the tail will not fit its allowance.
#
# Feature: row-cap-forward-progress
# Requirements: 5.1, 5.2, 7.1
# ---------------------------------------------------------------------------

_WATERMARK_WM = "2024-01-01T02:00:50.000000Z"
_WATERMARK_ATHENA = "2024-01-01 02:00:50.000000"


class TestBuildTailCountQuery:
    def test_is_a_single_row_aggregate(self):
        query = _build_tail_count_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM
        )
        assert query.strip().startswith("SELECT count(*)")
        assert "OFFSET" not in query
        assert "ORDER BY" not in query

    def test_bounds_the_tail_exclusively_below_and_inclusively_above(self):
        query = _build_tail_count_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM
        )
        assert f"record_timestamp > timestamp '{_SINCE_ATHENA}'" in query
        assert f"record_timestamp <= timestamp '{_WATERMARK_ATHENA}'" in query

    def test_predicate_parity_with_build_query(self):
        """The count must be over exactly the rows read_journal would
        materialise for the same range, or the number cannot be budgeted
        against the row cap."""
        count_query = _build_tail_count_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM
        )
        read_query = _build_query(
            _BUCKET_NAMESPACE,
            _BUCKET,
            since_timestamp=_SINCE_WM,
            until_timestamp=_WATERMARK_WM,
        )
        for predicate in (
            f"bucket = '{_BUCKET}'",
            "record_type = 'UPDATE_METADATA'",
            f"record_timestamp > timestamp '{_SINCE_ATHENA}'",
            f"record_timestamp <= timestamp '{_WATERMARK_ATHENA}'",
        ):
            assert predicate in count_query
            assert predicate in read_query

    def test_uses_the_same_journal_table_path(self):
        query = _build_tail_count_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM
        )
        assert f'"s3tablescatalog/aws-s3"."{_BUCKET_NAMESPACE}"."journal"' in query

    def test_bucket_name_escaped(self):
        query = _build_tail_count_query(
            _BUCKET_NAMESPACE, "buck'et", _SINCE_WM, _WATERMARK_WM
        )
        assert "buck''et" in query


class TestBuildTailFloorQuery:
    def test_orders_descending_so_the_offset_counts_back_from_the_watermark(self):
        query = _build_tail_floor_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=400_000
        )
        assert "ORDER BY record_timestamp DESC" in query
        assert "ORDER BY record_timestamp ASC" not in query

    def test_offset_equals_the_allowance_not_allowance_minus_one(self):
        """The bound is used exclusively, so the row named must be the one *past*
        the allowance. `OFFSET allowance - 1` would name the allowance-th row and
        admit one fewer than budgeted, which differs from
        `_build_boundary_query`'s `OFFSET row_cap - 1` only because that bound is
        inclusive."""
        query = _build_tail_floor_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=400_000
        )
        assert "OFFSET 400000 LIMIT 1" in query

    def test_offset_one_for_an_allowance_of_one(self):
        """`OFFSET 0` here would name the newest tail row, and an exclusive bound
        on it admits no tail rows at all — discarding the whole tail while the run
        advances past it."""
        query = _build_tail_floor_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=1
        )
        assert "OFFSET 1 LIMIT 1" in query

    def test_selects_only_record_timestamp_over_the_tail_range(self):
        query = _build_tail_floor_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=10
        )
        assert query.strip().startswith("SELECT record_timestamp")
        assert f"record_timestamp > timestamp '{_SINCE_ATHENA}'" in query
        assert f"record_timestamp <= timestamp '{_WATERMARK_ATHENA}'" in query

    def test_filters_to_update_metadata(self):
        query = _build_tail_floor_query(
            _BUCKET_NAMESPACE, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=10
        )
        assert "record_type = 'UPDATE_METADATA'" in query


class TestFindTailRowCount:
    def test_returns_the_count(self):
        client = _make_athena_client(rows=[["620000"]])
        result = find_tail_row_count(
            client, _BUCKET, _SINCE_WM, _WATERMARK_WM,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert result == 620_000

    def test_returns_zero_for_an_empty_tail(self):
        client = _make_athena_client(rows=[["0"]])
        result = find_tail_row_count(
            client, _BUCKET, _SINCE_WM, _WATERMARK_WM,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert result == 0

    def test_fetches_a_single_row_rather_than_paginating(self):
        """One get_query_results call, no NextToken follow-up — the property
        that keeps this query's cost independent of the tail's size."""
        client = _make_athena_client(rows=[["12"]])
        find_tail_row_count(
            client, _BUCKET, _SINCE_WM, _WATERMARK_WM,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert client.get_query_results.call_count == 1

    def test_query_failure_raises(self):
        client = _make_athena_client(state="FAILED", failure_reason="boom")
        with pytest.raises(ValueError):
            find_tail_row_count(
                client, _BUCKET, _SINCE_WM, _WATERMARK_WM,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )

    def test_unparseable_count_raises(self):
        client = _make_athena_client(rows=[["not-a-number"]])
        with pytest.raises(ValueError):
            find_tail_row_count(
                client, _BUCKET, _SINCE_WM, _WATERMARK_WM,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )

    def test_empty_result_set_raises_rather_than_reporting_zero(self):
        """COUNT(*) always returns a row. Reporting zero here would hand the
        caller a tail-free budget split on a response it did not understand."""
        client = _make_athena_client(rows=None)
        with pytest.raises(ValueError):
            find_tail_row_count(
                client, _BUCKET, _SINCE_WM, _WATERMARK_WM,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )


class TestFindTailFloor:
    def test_returns_canonical_watermark_when_floor_found(self):
        client = _make_athena_client(rows=[["2024-01-01 01:30:00.000000"]])
        result = find_tail_floor(
            client, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=4,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert result == "2024-01-01T01:30:00.000000Z"

    def test_returns_none_when_tail_smaller_than_allowance(self):
        client = _make_athena_client(rows=None)
        result = find_tail_floor(
            client, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=400_000,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert result is None

    def test_fetches_a_single_row_rather_than_paginating(self):
        client = _make_athena_client(rows=[["2024-01-01 01:30:00.000000"]])
        find_tail_floor(
            client, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=4,
            athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
        )
        assert client.get_query_results.call_count == 1

    def test_rejects_non_positive_allowance(self):
        client = _make_athena_client(rows=None)
        with pytest.raises(ValueError):
            find_tail_floor(
                client, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=0,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )

    def test_query_failure_raises(self):
        client = _make_athena_client(state="FAILED", failure_reason="boom")
        with pytest.raises(ValueError):
            find_tail_floor(
                client, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=4,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )

    def test_unparseable_timestamp_raises(self):
        client = _make_athena_client(rows=[["not-a-timestamp"]])
        with pytest.raises(ValueError):
            find_tail_floor(
                client, _BUCKET, _SINCE_WM, _WATERMARK_WM, tail_allowance=4,
                athena_workgroup=_WORKGROUP, output_location=_OUTPUT,
            )


# ---------------------------------------------------------------------------
# read_journal with until_timestamp (row-count cap)
# ---------------------------------------------------------------------------


class TestReadJournalUntilTimestamp:
    def test_until_timestamp_included_in_query(self):
        client = _make_athena_client(rows=[])
        until_wm = "2024-01-02T00:00:00.000000Z"
        read_journal(
            client, _BUCKET, _WORKGROUP, _OUTPUT,
            since_timestamp=None, until_timestamp=until_wm,
        )
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert "record_timestamp <= timestamp '2024-01-02 00:00:00.000000'" in query_string

    def test_no_until_timestamp_omits_upper_bound(self):
        client = _make_athena_client(rows=[])
        read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT, since_timestamp=None)
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert "record_timestamp <=" not in query_string


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


class TestParseRow:
    def test_valid_row_returns_tagging_operation(self):
        row = _make_row()
        op, err = _parse_row(row, _BUCKET)
        assert err is None
        assert isinstance(op, TaggingOperation)
        assert op.source_bucket == _BUCKET
        assert op.object_key == "path/obj.txt"
        assert op.resulting_tag_set == {"env": "prod"}
        assert op.sequence_number == "seq-001"
        assert op.operation == "PutObjectTagging"
        assert op.operation_version == "v1"

    def test_empty_version_id_maps_to_none(self):
        row = _make_row(version_id="")
        op, err = _parse_row(row, _BUCKET)
        assert err is None
        assert op.operation_version is None

    def test_missing_object_key_returns_per_record_error(self):
        row = _make_row(key="")
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False
        assert "missing object key" in err.cause

    def test_missing_resulting_tags_returns_per_record_error(self):
        """Req 12.4: missing resulting_tags surfaced as report-and-continue."""
        row = _make_row(tags=None)
        # Override the tags field to empty string
        row[4] = ""
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False
        assert "resulting_tags" in err.cause

    def test_invalid_tags_json_returns_per_record_error(self):
        row = _make_row()
        row[4] = "{not valid json"
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False

    def test_empty_resulting_tags_dict_returns_per_record_error(self):
        row = _make_row(tags={}, use_default_tags=False)
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False
        assert "empty" in err.cause

    def test_too_few_columns_returns_per_record_error(self):
        op, err = _parse_row(["bucket", "key"], _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False
        assert "7 columns" in err.cause

    def test_unparseable_event_time_returns_per_record_error_not_now(self):
        """code-review-remediation spec Req 8.2: an unparseable event_time
        must be skipped and reported (like a missing key), never
        substituted with datetime.now(UTC) — a fabricated watermark value
        would silently exclude legitimate later records in future runs."""
        row = _make_row(event_time="not-a-timestamp")
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False
        assert "event_time" in err.cause

    def test_missing_event_time_returns_per_record_error(self):
        row = _make_row(event_time="")
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False
        assert "event_time" in err.cause

    def test_valid_event_time_formats_parse_successfully(self):
        for fmt_value in (
            "2024-01-15 10:30:00.123456 UTC",
            "2024-01-15 10:30:00 UTC",
            "2024-01-15 10:30:00.123456",
            "2024-01-15 10:30:00",
            "2024-01-15T10:30:00.123456Z",
            "2024-01-15T10:30:00Z",
        ):
            row = _make_row(event_time=fmt_value)
            op, err = _parse_row(row, _BUCKET)
            assert err is None, f"format {fmt_value!r} unexpectedly failed: {err}"
            assert op is not None
            assert op.event_time.tzinfo is not None

    def test_bucket_falls_back_to_bucket_name_param(self):
        row = _make_row(bucket="")
        op, err = _parse_row(row, "fallback-bucket")
        assert err is None
        assert op.source_bucket == "fallback-bucket"

    def test_per_record_error_carries_sequence_and_key(self):
        row = _make_row(key="some/key.txt")
        row[4] = ""  # blank resulting_tags
        op, err = _parse_row(row, _BUCKET)
        assert err.object_key == "some/key.txt"
        assert err.sequence_number == "seq-001"

    def test_tags_non_dict_json_returns_per_record_error(self):
        row = _make_row()
        row[4] = json.dumps(["a", "b"])  # list instead of dict
        op, err = _parse_row(row, _BUCKET)
        assert op is None
        assert err is not None
        assert err.is_fatal is False


# ---------------------------------------------------------------------------
# read_journal — success path
# ---------------------------------------------------------------------------


class TestReadJournalSuccess:
    def test_returns_tagging_operations_on_success(self):
        rows = [_make_row(seq="seq-001"), _make_row(key="other.txt", seq="seq-002")]
        client = _make_athena_client(rows=rows)
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert len(ops) == 2
        assert errors == []

    def test_start_query_called_with_correct_params(self):
        client = _make_athena_client(rows=[])
        read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        client.start_query_execution.assert_called_once()
        kwargs = client.start_query_execution.call_args[1]
        assert kwargs["WorkGroup"] == _WORKGROUP
        assert kwargs["ResultConfiguration"]["OutputLocation"] == _OUTPUT

    def test_query_contains_bucket_filter(self):
        client = _make_athena_client(rows=[])
        read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert f"bucket = '{_BUCKET}'" in query_string
        assert "UPDATE_METADATA" in query_string

    def test_after_sequence_included_in_query(self):
        """Req 4.3: resume from the lookback window start via record_timestamp."""
        client = _make_athena_client(rows=[])
        read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT, since_timestamp=_SINCE_WM)
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert f"record_timestamp > timestamp '{_SINCE_ATHENA}'" in query_string

    def test_no_after_sequence_reads_from_beginning(self):
        """Req 4.1: first run with no checkpoint reads all records."""
        client = _make_athena_client(rows=[])
        read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT, since_timestamp=None)
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert "record_timestamp >" not in query_string

    def test_empty_result_returns_empty_ops_and_no_errors(self):
        client = _make_athena_client(rows=[])
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert errors == []

    def test_per_record_errors_collected_alongside_valid_ops(self):
        """Req 12.4 / 4.5: valid rows and skipped rows returned together."""
        good_row = _make_row(seq="seq-001")
        bad_row = _make_row(key="", seq="seq-002")  # missing key
        client = _make_athena_client(rows=[good_row, bad_row])
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert len(ops) == 1
        assert len(errors) == 1
        assert errors[0].is_fatal is False
        assert ops[0].sequence_number == "seq-001"

    def test_operation_version_populated_from_version_id(self):
        client = _make_athena_client(rows=[_make_row(version_id="v42")])
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops[0].operation_version == "v42"

    def test_operation_set_to_put_object_tagging(self):
        client = _make_athena_client(rows=[_make_row()])
        ops, _ = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops[0].operation == "PutObjectTagging"

    def test_multiple_tag_key_value_pairs_preserved(self):
        tags = {"env": "prod", "team": "platform", "version": "v2"}
        client = _make_athena_client(rows=[_make_row(tags=tags)])
        ops, _ = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops[0].resulting_tag_set == tags

    def test_bucket_namespace_derived_from_bucket_name(self):
        """Namespace 'b_<bucket>' appears in the Athena query."""
        client = _make_athena_client(rows=[])
        read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert _BUCKET_NAMESPACE in query_string

    def test_bucket_with_dots_replaces_dots_in_namespace(self):
        """Dots in bucket name become underscores in namespace."""
        dotted_bucket = "my.bucket.name"
        expected_namespace = "b_my_bucket_name"
        client = _make_athena_client(rows=[])
        read_journal(client, dotted_bucket, _WORKGROUP, _OUTPUT)
        query_string = client.start_query_execution.call_args[1]["QueryString"]
        assert expected_namespace in query_string


# ---------------------------------------------------------------------------
# read_journal — failure paths (checkpoint must remain unchanged)
# ---------------------------------------------------------------------------


class TestReadJournalFailure:
    def test_start_query_access_denied_returns_fatal_error(self):
        """Req 12.3: AccessDenied on journal read → fatal error, checkpoint unchanged."""
        client = _make_athena_client(
            start_raises=_make_client_error("AccessDeniedException", "Access denied")
        )
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True
        assert errors[0].bucket == _BUCKET
        assert "AccessDeniedException" in errors[0].cause

    def test_start_query_generic_error_returns_fatal_error(self):
        """Req 4.4: Any start_query failure → fatal error."""
        client = _make_athena_client(
            start_raises=_make_client_error("InvalidRequestException")
        )
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True

    def test_failed_query_state_returns_fatal_error(self):
        """Req 4.4: FAILED query state → fatal error with reason."""
        client = _make_athena_client(
            state="FAILED", failure_reason="Table not found"
        )
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True
        assert "failed" in errors[0].cause.lower()

    def test_cancelled_query_state_returns_fatal_error(self):
        client = _make_athena_client(state="CANCELLED")
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True
        assert "cancelled" in errors[0].cause.lower()

    def test_poll_client_error_returns_fatal_error(self):
        client = _make_athena_client(
            poll_raises=_make_client_error("ThrottlingException")
        )
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True

    def test_get_results_access_denied_returns_fatal_error(self):
        """Req 4.4: Failure during results retrieval → fatal, checkpoint unchanged."""
        client = _make_athena_client(
            state="SUCCEEDED",
            results_raises=_make_client_error("AccessDeniedException"),
        )
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is True

    def test_fatal_error_bucket_name_preserved(self):
        client = _make_athena_client(
            start_raises=_make_client_error("AccessDeniedException")
        )
        _, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert errors[0].bucket == _BUCKET

    def test_failure_reason_from_athena_included_in_cause(self):
        client = _make_athena_client(
            state="FAILED",
            failure_reason="Iceberg table metadata unavailable",
        )
        _, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert "Iceberg table metadata unavailable" in errors[0].cause


# ---------------------------------------------------------------------------
# read_journal — object tag read permission failures (Req 12.4)
# ---------------------------------------------------------------------------


class TestReadJournalTagPermission:
    def test_row_with_empty_resulting_tags_is_non_fatal_error(self):
        """Req 12.4: missing resulting_tags surfaced as report-and-continue."""
        good_row = _make_row(seq="seq-001")
        bad_row = list(_make_row(seq="seq-002"))
        bad_row[4] = ""  # blank resulting_tags column
        client = _make_athena_client(rows=[good_row, bad_row])
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        # Valid records are still returned
        assert len(ops) == 1
        assert ops[0].sequence_number == "seq-001"
        # The bad row is reported as a non-fatal per-record error
        assert len(errors) == 1
        assert errors[0].is_fatal is False
        assert errors[0].object_key == "path/obj.txt"

    def test_all_rows_bad_returns_empty_ops_with_non_fatal_errors(self):
        """All rows failing is non-fatal — no journal-read abort occurred."""
        rows = [
            list(_make_row(seq=f"seq-{i:03d}")) for i in range(3)
        ]
        for row in rows:
            row[4] = ""  # blank resulting_tags
        client = _make_athena_client(rows=rows)
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 3
        assert all(not e.is_fatal for e in errors)

    def test_missing_key_is_non_fatal_error(self):
        """Req 4.5: missing object key → per-record skip, not a fatal abort."""
        rows = [_make_row(key="", seq="seq-001")]
        client = _make_athena_client(rows=rows)
        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert ops == []
        assert len(errors) == 1
        assert errors[0].is_fatal is False


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


class TestReadJournalPagination:
    def test_paginated_results_all_rows_returned(self):
        """get_query_results pages are fully consumed."""
        client = MagicMock()
        client.start_query_execution.return_value = {"QueryExecutionId": _EXEC_ID}
        client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        header = {
            "Data": [
                {"VarCharValue": col}
                for col in [
                    "bucket", "key", "version_id", "operation",
                    "resulting_tags", "sequence_number", "event_time",
                ]
            ]
        }

        def make_athena_row(seq: str) -> dict:
            return {
                "Data": [
                    {"VarCharValue": _BUCKET},
                    {"VarCharValue": f"obj-{seq}.txt"},
                    {"VarCharValue": ""},
                    {"VarCharValue": "PutObjectTagging"},
                    {"VarCharValue": json.dumps({"k": "v"})},
                    {"VarCharValue": seq},
                    {"VarCharValue": _NOW},
                ]
            }

        # Simulate two pages
        page1 = {
            "ResultSet": {"Rows": [header, make_athena_row("seq-001")]},
            "NextToken": "page2token",
        }
        page2 = {
            "ResultSet": {"Rows": [make_athena_row("seq-002"), make_athena_row("seq-003")]},
        }
        client.get_query_results.side_effect = [page1, page2]

        ops, errors = read_journal(client, _BUCKET, _WORKGROUP, _OUTPUT)
        assert len(ops) == 3
        assert errors == []
        seqs = [op.sequence_number for op in ops]
        assert seqs == ["seq-001", "seq-002", "seq-003"]

        # Confirm two calls were made
        assert client.get_query_results.call_count == 2
        # Second call uses the NextToken from page 1
        second_call_kwargs = client.get_query_results.call_args_list[1][1]
        assert second_call_kwargs["NextToken"] == "page2token"
