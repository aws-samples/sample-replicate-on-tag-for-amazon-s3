"""Permanent-Delete Reader — queries Athena for permanently deleted versions.

Returns a ``set[(key, version_id|None)]`` of permanently deleted
``(object_key, version_id)`` pairs for a bucket, for use by the
Deleted_Version_Filter in the in-memory generation paths.

The detection window starts at ``since_window_start`` (lookback-window start)
and has no upper bound at the watermark — a delete occurring between tagging
and manifest generation must be detected regardless of whether it happened
before or after the watermark (Requirement 13.2).

Null-version supersession (Req 12.4): when a later CREATE/PUT record writes a
new null version to a key (``version_id IS NULL``), the prior null version for
that key is treated as permanently deleted (overwritten).  A ``(key, None)``
entry is added for any key that has such a later null-version write.

Requirements: 12.1, 12.2, 12.3, 12.4, 13.2
"""
from __future__ import annotations

import logging

from botocore.exceptions import ClientError

from src.adapters.athena_journal_adapter import (
    _escape_sql_string,
    _get_query_failure_reason,
    _iter_result_rows,
    _poll_query,
    _start_query,
    _to_athena_timestamp_literal,
)
from src.core.delete_filter import PERMANENT_DELETE_SQL_PREDICATE

logger = logging.getLogger(__name__)

_COMPONENT = "Permanent_Delete_Reader"


def read_permanent_deletes(
    athena_client,
    bucket_name: str,
    since_window_start: str | None,
    athena_workgroup: str = "primary",
    output_location: str = "",
) -> set[tuple[str, str | None]]:
    """Query Athena for permanently deleted versions.

    Combines two detection sources (Requirements 12.1–12.4):

    1. **Direct permanent deletes**: DELETE records with
       ``COALESCE(is_delete_marker, FALSE) = FALSE`` — the specific version is
       gone.

    2. **Null-version supersession**: any key that has a later CREATE/PUT
       record writing a new ``NULL`` version (``version_id IS NULL``) is
       treated as having its prior null version permanently deleted (Req 12.4).

    Both are bounded from below at ``since_window_start`` (a delete cannot
    precede its tag, so this is safe and bounded), but uncapped at the watermark
    so deletes that occurred between tagging and manifest generation are caught
    (Requirement 13.2).

    Parameters
    ----------
    athena_client:
        boto3 Athena client.
    bucket_name:
        Source bucket name.
    since_window_start:
        Lookback-window start timestamp (canonical watermark), or ``None``
        for a first run.
    athena_workgroup:
        Athena workgroup.
    output_location:
        S3 URI for Athena query results.

    Returns
    -------
    set[tuple[str, str|None]]
        Set of ``(object_key, version_id|None)`` pairs.  ``version_id=None``
        addresses the null version specifically.

    Raises
    ------
    RuntimeError
        On Athena query failure (caller should treat as non-fatal and pass
        an empty set to the filter — the worst case is a failed BOPS task,
        not a missing replication).

    Requirements: 12.1, 12.2, 12.3, 12.4, 13.2
    """
    bucket_namespace = "b_" + bucket_name.replace(".", "_")
    table_path = f'"s3tablescatalog/aws-s3"."{bucket_namespace}"."journal"'
    bucket_escaped = _escape_sql_string(bucket_name)

    # Time window lower bound (Req 13.2 — lower-bounded at lookback start)
    time_filter = ""
    if since_window_start:
        ts = _to_athena_timestamp_literal(since_window_start)
        time_filter = f" AND record_timestamp > timestamp '{ts}'"

    # Query 1: direct permanent deletes (Req 12.1, 12.2, 12.3)
    # Query 2: null-version supersession — later null-version CREATE/PUT (Req 12.4)
    # Combined via UNION of two SELECT DISTINCT queries.
    query = (
        f"SELECT DISTINCT key, version_id "
        f"FROM {table_path} "
        f"WHERE bucket = '{bucket_escaped}' "
        f"AND {PERMANENT_DELETE_SQL_PREDICATE}"
        f"{time_filter}"
        f"\nUNION\n"
        f"SELECT DISTINCT key, CAST(NULL AS VARCHAR) AS version_id "
        f"FROM {table_path} "
        f"WHERE bucket = '{bucket_escaped}' "
        f"AND record_type IN ('PUT', 'COPY', 'RESTORE') "
        f"AND version_id IS NULL "
        f"{time_filter}"
    )

    try:
        qeid = _start_query(
            athena_client, query, athena_workgroup, output_location
        )
    except ClientError as exc:
        raise RuntimeError(
            f"Permanent_Delete_Reader start_query failed for {bucket_name!r}: {exc}"
        ) from exc

    try:
        state = _poll_query(athena_client, qeid)
    except (ClientError, ValueError) as exc:
        raise RuntimeError(
            f"Permanent_Delete_Reader poll failed for {bucket_name!r}: {exc}"
        ) from exc

    if state in ("FAILED", "CANCELLED"):
        reason = _get_query_failure_reason(athena_client, qeid)
        raise RuntimeError(
            f"Permanent_Delete_Reader query {state.lower()} for "
            f"{bucket_name!r}: {reason}"
        )

    try:
        rows = list(_iter_result_rows(athena_client, qeid))
    except ClientError as exc:
        raise RuntimeError(
            f"Permanent_Delete_Reader result fetch failed for "
            f"{bucket_name!r}: {exc}"
        ) from exc

    result: set[tuple[str, str | None]] = set()
    for row in rows:
        if len(row) < 2:
            continue
        key = row[0]
        vid_raw = row[1]
        version_id: str | None = vid_raw if vid_raw else None
        if key:
            result.add((key, version_id))

    return result
