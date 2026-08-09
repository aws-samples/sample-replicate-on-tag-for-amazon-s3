"""Athena journal-read adapter for the tag-based S3 replication backfill Solution.

Queries the S3 Metadata journal (Iceberg) via Athena for UPDATE_METADATA events
in a specific bucket, resuming from a persisted sequence_number checkpoint.

On journal read failure or missing permission the adapter returns a single fatal
``JournalReadError`` (``is_fatal=True``) so the caller can leave the checkpoint
unchanged and retry on the next run (Requirements 4.4, 12.3).

Per-record failures (missing fields, inaccessible tags) produce non-fatal
``JournalReadError`` entries (``is_fatal=False``) that are included alongside
the successfully parsed records so processing continues (Requirements 4.5, 12.4).

Requirements: 4.1, 4.3, 4.4, 12.3, 12.4
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, UTC

from botocore.exceptions import ClientError

from src.core.models import TaggingOperation
from src.core.observability import redact_object_key
from src.core.watermark import parse_watermark, to_watermark

logger = logging.getLogger(__name__)

# Component name used in error reporting (Requirement 11.4)
_COMPONENT = "Journal_Monitor"

# Athena query poll interval (seconds between get_query_execution calls)
_POLL_INTERVAL_SECONDS = 2.0

# Maximum number of poll attempts before treating the query as timed-out
# 300 × 2 s = 10 minutes maximum wait per query
_MAX_POLL_ATTEMPTS = 300

# Rows returned per get_query_results page
_PAGE_SIZE = 1000

# ---------------------------------------------------------------------------
# Public error type
# ---------------------------------------------------------------------------


@dataclass
class JournalReadError:
    """An error encountered while reading the S3 Metadata journal via Athena.

    Attributes
    ----------
    bucket:
        The affected source S3 bucket name.
    cause:
        Human-readable description of the failure.
    is_fatal:
        ``True`` when the entire journal read for the bucket failed
        (Requirements 4.4, 12.3).  The caller MUST leave the checkpoint
        unchanged so records are retried on the next run.

        ``False`` when a single record could not be processed
        (Requirements 4.5, 12.4).  Processing continues with the remaining
        records.
    sequence_number:
        Journal position of the affected record (per-record errors only).
    object_key:
        Object key of the affected record (per-record errors only).
    is_journal_unavailable:
        ``True`` when the failure reason indicates the journal table or its
        namespace does not exist, rather than a transient or permission
        failure — in practice, that the S3 Metadata journal is not enabled on
        this bucket, or the Region's ``s3tablescatalog`` integration was never
        registered. This is an unmet prerequisite, not a condition that
        retrying resolves, so the caller escalates it to an operator instead
        of only logging it (see
        :func:`src.orchestrator._escalate_journal_unavailable`).

        Always ``False`` for per-record errors.
    """

    bucket: str
    cause: str
    is_fatal: bool
    sequence_number: str | None = None
    object_key: str | None = None
    is_journal_unavailable: bool = False


# ---------------------------------------------------------------------------
# Internal SQL helpers
# ---------------------------------------------------------------------------


# Substrings in an Athena failure reason that identify a missing journal table
# or namespace, as opposed to a transient failure or a permission problem.
# Matched case-insensitively.
#
# Deliberately excludes access-denied wording. A Lake Formation account
# missing a grant also cannot read the journal, but the remediation is a
# grant, not enabling the journal, so telling the operator to enable a journal
# that already exists would send them down the wrong path. That case stays a
# generic fatal error.
_JOURNAL_UNAVAILABLE_MARKERS = (
    "table_not_found",
    "schema_not_found",
    "database_not_found",
    "entitynotfoundexception",
    "does not exist",
)


def _is_journal_unavailable_reason(reason: str) -> bool:
    """Return ``True`` when *reason* indicates the journal table is absent.

    Athena reports a missing table or namespace through the human-readable
    ``StateChangeReason`` rather than a distinct error code, so matching on
    its text is the only signal available. That makes this classifier
    inherently approximate, and it is built to fail in the harmless
    direction: an unrecognised reason simply stays a generic fatal journal
    error, which is exactly the behavior that existed before this
    classification was added. A false negative therefore costs only the
    escalation, never correctness.
    """
    lowered = reason.lower()
    return any(marker in lowered for marker in _JOURNAL_UNAVAILABLE_MARKERS)


def _escape_sql_string(value: str) -> str:
    """Escape a string value for safe embedding in an Athena SQL literal.

    Replaces each single-quote with two single-quotes, which is the standard
    SQL string-escaping approach accepted by Athena (Presto/Trino).
    """
    return value.replace("'", "''")


def _escape_like_pattern(value: str) -> str:
    """Escape a string for use as a prefix in an Athena LIKE pattern.

    Escapes the three characters that have special meaning in a LIKE pattern
    when the LIKE escape character is a backslash (``ESCAPE '\\'``):

    * ``\\`` — the escape character itself; must be escaped first
    * ``%``  — matches any sequence of characters
    * ``_``  — matches any single character

    The caller appends the trailing ``%`` wildcard for prefix matching and
    must include ``ESCAPE '\\\\'`` in the SQL predicate so Athena interprets
    the escaped characters as literals.  Single quotes are also doubled so
    the result is safe to embed directly in a SQL string literal.

    Example::

        prefix = "archive/2024%01"
        escaped = _escape_like_pattern(prefix)   # → "archive/2024\\%01"
        sql = f"key LIKE '{escaped}%' ESCAPE '\\\\'"
        # → key LIKE 'archive/2024\\%01%' ESCAPE '\\\\'
        # Athena: key starts with literal "archive/2024%01"
    """
    value = value.replace("\\", "\\\\")  # backslash first — it is the escape char
    value = value.replace("%", "\\%")    # literal percent sign
    value = value.replace("_", "\\_")    # literal underscore
    return value.replace("'", "''")      # SQL string delimiter


def _to_athena_timestamp_literal(canonical_watermark: str) -> str:
    """Convert a canonical watermark to an Athena ``timestamp`` literal body.

    The canonical watermark (``2024-11-15T23:26:44.899000Z``) is parsed and
    reformatted as the space-separated, microsecond-precision form Athena
    accepts inside ``timestamp '...'`` (``2024-11-15 23:26:44.899000``).  The
    value originates from a parsed timestamp, not user input, so there is no
    injection risk.
    """
    dt = parse_watermark(canonical_watermark)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def _build_query(
    bucket_namespace: str,
    bucket_name: str,
    since_timestamp: str | None,
    until_timestamp: str | None = None,
) -> str:
    """Build the Athena SQL query that reads UPDATE_METADATA events.

    Queries the real S3 Metadata journal via the 3-part Athena table path
    ``"s3tablescatalog/aws-s3"."<bucket_namespace>"."journal"``.

    Selects all UPDATE_METADATA rows for *bucket_name*.  When *since_timestamp*
    (a canonical watermark string) is provided the query includes a
    ``record_timestamp > timestamp '<since>'`` predicate so the Journal_Monitor
    resumes from the lookback window start (Requirement 4.3).  ``record_timestamp``
    is used as the cross-key cursor because S3 documents its ordering globally,
    unlike ``sequence_number`` whose ordering is only defined per (bucket, key).

    When *until_timestamp* is provided, the query additionally includes a
    ``record_timestamp <= timestamp '<until>'`` predicate — the row-count cap
    (design.md, code-review-remediation verification-notes.md "scaling risk"
    finding). This is an inclusive upper bound, not a ``LIMIT``, specifically
    so a boundary that lands mid-tie (multiple records sharing the exact same
    ``record_timestamp``) never splits that tie: every record at
    *until_timestamp* is included. Splitting a tie would let the excluded half
    fall below the watermark this run advances to, without ever having been
    submitted — silently lost, not merely delayed. See
    :func:`find_row_count_boundary` for how *until_timestamp* is determined.

    The result set is ordered by ``record_timestamp ASC``.

    Column order in the SELECT:
        0  bucket
        1  key           (object key)
        2  version_id    (per-object operation/version token; optional)
        3  operation     (aliased from record_type)
        4  resulting_tags  (aliased from CAST(object_tags AS JSON))
        5  sequence_number (used only for per-key dedup tie-breaking)
        6  event_time    (aliased from record_timestamp; the watermark source)
        7  last_modified_date (object last-modified timestamp from the journal)
        8  storage_class (object storage class as of this record)

    ``storage_class`` is projected but deliberately **not** filtered on here.
    Excluding archived objects with a ``WHERE`` predicate would remove the
    lifecycle-transition record before deduplication runs, leaving the
    earlier pre-transition tagging record as the surviving one; that record
    reports the storage class the object held *before* it was archived, so it
    would pass the filter and reach a manifest. The exclusion therefore
    belongs after deduplication — see
    :func:`src.core.archived_filter.filter_archived_operations` and the
    ``storage_class`` note on
    :attr:`src.core.models.TaggingOperation.logical_operation_id`.
    """
    # 3-part Athena table path for S3 Metadata journal (S3 Tables catalog)
    table_path = f'"s3tablescatalog/aws-s3"."{bucket_namespace}"."journal"'
    bucket_escaped = _escape_sql_string(bucket_name)

    query = (
        "SELECT bucket, key, version_id, record_type AS operation, "
        "CAST(object_tags AS JSON) AS resulting_tags, "
        "sequence_number, record_timestamp AS event_time, "
        "last_modified_date, storage_class "
        f"FROM {table_path} "
        f"WHERE bucket = '{bucket_escaped}' "
        "AND record_type = 'UPDATE_METADATA'"
    )

    if since_timestamp is not None:
        athena_ts = _to_athena_timestamp_literal(since_timestamp)
        query += f" AND record_timestamp > timestamp '{athena_ts}'"

    if until_timestamp is not None:
        until_ts = _to_athena_timestamp_literal(until_timestamp)
        query += f" AND record_timestamp <= timestamp '{until_ts}'"

    query += " ORDER BY record_timestamp ASC"
    return query


def _build_boundary_query(
    bucket_namespace: str,
    bucket_name: str,
    since_timestamp: str | None,
    row_cap: int,
) -> str:
    """Build the cheap boundary-finder query for the row-count cap.

    Returns a query that yields exactly one row — the ``record_timestamp``
    of the ``row_cap``-th matching record (1-indexed) — when the window
    contains at least ``row_cap`` rows, or zero rows when it contains fewer.

    Uses ``LIMIT 1 OFFSET (row_cap - 1)`` rather than fetching and counting
    rows client-side: Athena computes the offset server-side, so this query's
    *result* is always a single row (or none) regardless of how large the
    underlying window is — the client-side pagination cost that motivates
    this cap in the first place (see verification-notes.md's "scaling risk"
    finding: ~617 ms/page at 1000 rows/page, effectively linear in row
    count) is avoided entirely for this check. Athena's own server-side
    execution time to compute the offset was measured at under 4 seconds
    even for a ~900,000-row window.

    Same window predicate (``bucket``, ``record_type = 'UPDATE_METADATA'``,
    optional ``since_timestamp`` lower bound) and ordering
    (``record_timestamp ASC``) as :func:`_build_query`, so the row this
    returns is the same row that would be the ``row_cap``-th row of the real
    query's result set.
    """
    table_path = f'"s3tablescatalog/aws-s3"."{bucket_namespace}"."journal"'
    bucket_escaped = _escape_sql_string(bucket_name)

    query = (
        "SELECT record_timestamp "
        f"FROM {table_path} "
        f"WHERE bucket = '{bucket_escaped}' "
        "AND record_type = 'UPDATE_METADATA'"
    )

    if since_timestamp is not None:
        athena_ts = _to_athena_timestamp_literal(since_timestamp)
        query += f" AND record_timestamp > timestamp '{athena_ts}'"

    query += f" ORDER BY record_timestamp ASC OFFSET {row_cap - 1} LIMIT 1"
    return query


# ---------------------------------------------------------------------------
# Internal Athena execution helpers
# ---------------------------------------------------------------------------


def _start_query(
    athena_client,
    query: str,
    athena_workgroup: str,
    output_location: str,
) -> str:
    """Submit an Athena query and return the ``QueryExecutionId``.

    Uses ``WorkGroup`` instead of ``QueryExecutionContext`` so the query runs
    in the caller's nominated workgroup with its own result settings.

    Result encryption is governed entirely by the workgroup's own
    ``EncryptionConfiguration`` (``AthenaWorkGroup`` in ``deploy/template.yaml``,
    SSE-KMS when ``KmsKeyArn`` is supplied, SSE-S3 otherwise) — the workgroup
    sets ``EnforceWorkGroupConfiguration: true``, so a client-supplied
    ``ResultConfiguration`` would be silently ignored rather than honored
    (security-scan-remediation Decisions 1, 2).

    Raises ``ClientError`` on failure (e.g., AccessDeniedException).
    """
    response = athena_client.start_query_execution(
        QueryString=query,
        WorkGroup=athena_workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )
    return response["QueryExecutionId"]


def _poll_query(athena_client, query_execution_id: str) -> str:
    """Poll Athena until the query reaches a terminal state.

    Returns one of ``"SUCCEEDED"``, ``"FAILED"``, or ``"CANCELLED"``.
    Raises ``ValueError`` when ``_MAX_POLL_ATTEMPTS`` is exceeded without
    reaching a terminal state.
    Raises ``ClientError`` if ``get_query_execution`` itself fails.
    """
    for _ in range(_MAX_POLL_ATTEMPTS):
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = response["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state
        time.sleep(_POLL_INTERVAL_SECONDS)

    raise ValueError(
        f"Athena query {query_execution_id!r} did not complete within "
        f"{_MAX_POLL_ATTEMPTS * _POLL_INTERVAL_SECONDS:.0f} seconds"
    )


def _run_scalar_query(
    athena_client,
    query: str,
    athena_workgroup: str,
    output_location: str,
) -> str | None:
    """Run a query expected to return at most one row/column; return its
    value, or ``None`` if the result set is empty.

    Shared submit/poll/fetch-one-row helper for :func:`find_row_count_boundary`.
    Raises ``ClientError``/``ValueError`` on failure, exactly like
    :func:`_start_query`/:func:`_poll_query` — the caller is expected to
    treat this the same as any other Athena failure.
    """
    qeid = _start_query(athena_client, query, athena_workgroup, output_location)
    state = _poll_query(athena_client, qeid)
    if state != "SUCCEEDED":
        reason = _get_query_failure_reason(athena_client, qeid)
        raise ValueError(f"Athena query {qeid!r} {state.lower()}: {reason}")

    for row in _iter_result_rows(athena_client, qeid):
        return row[0] if row else None
    return None


def find_row_count_boundary(
    athena_client,
    bucket_name: str,
    since_timestamp: str | None,
    row_cap: int,
    athena_workgroup: str = "primary",
    output_location: str = "",
) -> str | None:
    """Find the ``record_timestamp`` upper bound that caps a journal read to
    at most ``row_cap`` rows, without paginating the full result set.

    Runs the cheap single-row boundary query (:func:`_build_boundary_query`)
    and returns its result reformatted as a canonical watermark string
    suitable for passing as ``until_timestamp`` to :func:`read_journal` and
    as the equivalent upper bound to
    :func:`~src.adapters.preflight_counter.preflight_count`.

    Returns ``None`` when the window contains fewer than ``row_cap`` rows —
    the common case — meaning no capping is needed and every downstream
    call should omit its upper-bound parameter entirely (an unbounded read,
    exactly as before this cap existed).

    This function's own cost is bounded regardless of the underlying
    window's true size: see :func:`_build_boundary_query`'s docstring.

    Parameters
    ----------
    athena_client:
        boto3 Athena client.
    bucket_name:
        Source bucket name.
    since_timestamp:
        Lookback-window start as a canonical watermark string, or ``None``
        for a first run — same semantics as :func:`read_journal`'s
        parameter of the same name.
    row_cap:
        Maximum number of rows a capped read should return. Must be a
        positive integer.
    athena_workgroup, output_location:
        Same as :func:`read_journal`.

    Returns
    -------
    str | None
        A canonical watermark string (the boundary record's
        ``record_timestamp``), or ``None`` when the window has fewer than
        ``row_cap`` rows.

    Raises
    ------
    ValueError
        If ``row_cap`` is not a positive integer, or the boundary query
        fails/times out.
    ClientError
        On other Athena failures.
    """
    if row_cap <= 0:
        raise ValueError(f"row_cap must be a positive integer, got {row_cap!r}")

    bucket_namespace = "b_" + bucket_name.replace(".", "_")
    query = _build_boundary_query(bucket_namespace, bucket_name, since_timestamp, row_cap)

    raw_value = _run_scalar_query(
        athena_client, query, athena_workgroup, output_location
    )
    if raw_value is None:
        return None

    # Athena returns record_timestamp in the same string form _parse_event_time
    # already knows how to parse; reuse it and reformat as a canonical
    # watermark via to_watermark for consistency with every other watermark
    # value in this codebase.
    parsed = _parse_event_time(raw_value)
    if parsed is None:
        raise ValueError(
            f"Boundary query returned an unparseable record_timestamp: {raw_value!r}"
        )
    return to_watermark(parsed)


def _get_query_failure_reason(athena_client, query_execution_id: str) -> str:
    """Retrieve the human-readable failure reason for a FAILED/CANCELLED query."""
    try:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        return response["QueryExecution"]["Status"].get(
            "StateChangeReason", "unknown reason"
        )
    except Exception:  # noqa: BLE001 — best-effort; don't mask the original error
        return "unknown reason"


def _iter_result_rows(athena_client, query_execution_id: str):
    """Yield each data row (as a list of strings) from the paginated Athena results.

    The header row on the first page is automatically skipped so every yielded
    list contains only data values.
    """
    next_token: str | None = None
    first_page = True

    while True:
        kwargs: dict = {
            "QueryExecutionId": query_execution_id,
            "MaxResults": _PAGE_SIZE,
        }
        if next_token:
            kwargs["NextToken"] = next_token

        response = athena_client.get_query_results(**kwargs)
        rows = response.get("ResultSet", {}).get("Rows", [])

        # The first page always starts with a header row; skip it.
        start_idx = 1 if first_page else 0
        first_page = False

        for row in rows[start_idx:]:
            yield [datum.get("VarCharValue", "") for datum in row.get("Data", [])]

        next_token = response.get("NextToken")
        if not next_token:
            break


# ---------------------------------------------------------------------------
# Internal row parser
# ---------------------------------------------------------------------------


def _parse_event_time(raw: str) -> datetime | None:
    """Parse an ``event_time`` string from Athena into a timezone-aware datetime.

    Tries several common Athena timestamp formats. Returns ``None`` when
    *raw* is empty or unparseable by any of them — the caller (:func:`_parse_row`)
    treats that as a per-record validity failure (a :class:`JournalReadError`,
    same as a missing object key), rather than substituting a fabricated
    "now" timestamp.

    This matters because ``event_time`` feeds ``to_watermark()`` and can
    become the run's candidate high-water mark, which is persisted as the
    new checkpoint watermark on a successful submission. A fabricated "now"
    value would advance the watermark past the true position of any
    legitimate record whose real ``event_time`` falls between the actual
    journal position and "now" — silently excluding it as "already
    processed" in every subsequent run, with no way to recover it. Skipping
    the record and reporting it (so an operator can see and investigate)
    is the safe failure mode; advancing the watermark on bad data is not.
    """
    if not raw:
        return None

    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f UTC",
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue

    return None


def _parse_row(
    row: list[str],
    bucket_name: str,
) -> tuple[TaggingOperation | None, JournalReadError | None]:
    """Parse one Athena result row into a ``TaggingOperation``.

    Expected column order (0-indexed):
        0  bucket
        1  key           (object key)
        2  version_id    (operation_version; may be empty)
        3  operation     (record_type alias; already filtered to UPDATE_METADATA)
        4  resulting_tags  (CAST(object_tags AS JSON) alias)
        5  sequence_number
        6  event_time    (record_timestamp alias)
        7  last_modified_date (object last-modified timestamp)
        8  storage_class (object storage class as of this record)

    Columns 7 and 8 are optional in this parser: both are read defensively so
    a shorter row still yields a usable operation rather than being dropped.
    A missing or empty ``storage_class`` becomes ``None``, which the
    archived-object filter treats as "not known to be archived" and allows
    through, rather than excluding an object on absent evidence.

    Returns ``(TaggingOperation, None)`` on success.
    Returns ``(None, JournalReadError)`` for any per-record failure; the
    error is non-fatal (``is_fatal=False``) so the caller continues
    processing the remaining rows (Requirements 4.5, 12.4).
    """
    if len(row) < 7:
        return None, JournalReadError(
            bucket=bucket_name,
            cause=(
                f"malformed row: expected at least 7 columns, got {len(row)}"
            ),
            is_fatal=False,
        )

    bucket = row[0] or bucket_name
    key = row[1]
    version_id: str | None = row[2] if row[2] else None
    # row[3] is operation alias — already filtered to 'UPDATE_METADATA' by SQL
    resulting_tags_raw = row[4]
    sequence_number = row[5]
    event_time_raw = row[6]

    # Requirement 4.5: skip records missing the object key
    if not key:
        return None, JournalReadError(
            bucket=bucket,
            cause="missing object key in journal record",
            is_fatal=False,
            sequence_number=sequence_number or None,
            object_key=None,
        )

    # Requirement 4.5 / 12.4: skip records missing or inaccessible resulting_tags.
    # A null/empty resulting_tags column may indicate a tag-read permission
    # restriction on this object (surfaced as report-and-continue per Req 12.4).
    if not resulting_tags_raw:
        return None, JournalReadError(
            bucket=bucket,
            cause=(
                f"missing or inaccessible resulting_tags for key "
                f"{redact_object_key(key)} (possible tag-read permission issue)"
            ),
            is_fatal=False,
            sequence_number=sequence_number or None,
            object_key=key,
        )

    # Parse the resulting_tags JSON
    try:
        resulting_tags: dict = json.loads(resulting_tags_raw)
        if not isinstance(resulting_tags, dict):
            raise ValueError("resulting_tags is not a JSON object")
    except (json.JSONDecodeError, ValueError) as exc:
        return None, JournalReadError(
            bucket=bucket,
            cause=f"invalid resulting_tags JSON for key {redact_object_key(key)}: {exc}",
            is_fatal=False,
            sequence_number=sequence_number or None,
            object_key=key,
        )

    # Requirement 4.5: skip records whose resulting_tags dict is empty
    if not resulting_tags:
        return None, JournalReadError(
            bucket=bucket,
            cause=f"empty resulting_tags for key {redact_object_key(key)}",
            is_fatal=False,
            sequence_number=sequence_number or None,
            object_key=key,
        )

    # Requirement 4.5: skip records with a missing or unparseable event_time
    # rather than substituting the current time — see _parse_event_time's
    # docstring for why a fabricated watermark value is unsafe.
    event_time = _parse_event_time(event_time_raw)
    if event_time is None:
        return None, JournalReadError(
            bucket=bucket,
            cause=(
                f"unparseable event_time {event_time_raw!r} for key "
                f"{redact_object_key(key)}"
            ),
            is_fatal=False,
            sequence_number=sequence_number or None,
            object_key=key,
        )

    op = TaggingOperation(
        source_bucket=bucket,
        object_key=key,
        resulting_tag_set=resulting_tags,
        sequence_number=sequence_number,
        operation="PutObjectTagging",
        event_time=event_time,
        operation_version=version_id,
        last_modified=_parse_event_time(row[7]) if len(row) > 7 and row[7] else None,
        storage_class=row[8] if len(row) > 8 and row[8] else None,
    )
    return op, None


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def read_journal(
    athena_client,
    bucket_name: str,
    athena_workgroup: str = "primary",
    output_location: str = "",
    since_timestamp: str | None = None,
    until_timestamp: str | None = None,
) -> tuple[list[TaggingOperation], list[JournalReadError]]:
    """Query the S3 Metadata journal via Athena and return tagging operations.

    Queries the Iceberg journal table for ``UPDATE_METADATA`` events in
    ``bucket_name``.  When ``since_timestamp`` is provided, only records whose
    ``record_timestamp`` is strictly greater are returned so the Journal_Monitor
    resumes from the lookback window start (Requirement 4.3).

    The bucket namespace is derived from ``bucket_name`` as
    ``"b_" + bucket_name.replace(".", "_")`` — S3 Metadata converts dots to
    underscores in namespace names; hyphens remain unchanged.

    On success the function returns the parsed ``TaggingOperation`` list
    together with any non-fatal per-record ``JournalReadError`` entries.

    On failure (Athena query error, ``AccessDeniedException``, or timeout)
    the function returns ``([], [fatal_error])`` where
    ``fatal_error.is_fatal is True``.  **The caller MUST leave the checkpoint
    unchanged** so the records are retried on the next run
    (Requirements 4.4, 12.3).

    When the failure reason identifies the journal table or its namespace as
    absent, the fatal error additionally carries
    ``is_journal_unavailable=True``.  That distinguishes an unmet
    prerequisite, which retrying never resolves and which therefore warrants
    escalating to an operator, from a transient failure that the next run
    clears on its own.

    Parameters
    ----------
    athena_client:
        A ``boto3`` Athena client (``boto3.client("athena", ...)``)
        created with source-side credentials.
    bucket_name:
        The source S3 bucket whose journal records are to be read
        (Requirement 4.1).
    athena_workgroup:
        Athena workgroup to run queries in.  Defaults to ``"primary"``.
    output_location:
        S3 URI where Athena writes query results
        (e.g. ``"s3://${StateBucket}/athena-results/"``).
    since_timestamp:
        Resume position as a canonical watermark string (the lookback window
        start computed from the bucket's persisted ``record_timestamp``
        watermark).  Only records with ``record_timestamp`` strictly greater
        than this are read.  Pass ``None`` on first run to read all records.
    until_timestamp:
        Optional inclusive upper bound as a canonical watermark string — the
        row-count cap (see :func:`find_row_count_boundary`). When provided,
        only records with ``record_timestamp <= until_timestamp`` are read,
        so a single-interval tagging burst large enough to make client-side
        pagination exceed the Lambda timeout is bounded to a partial read
        instead. Pass ``None`` (the default) for an unbounded read — the
        common case, when the window's row count is below the configured
        cap.

    Returns
    -------
    tuple[list[TaggingOperation], list[JournalReadError]]
        - **ops**: Parsed ``TaggingOperation`` records ready for the
          Journal_Monitor's deduplication and matching pipeline.  Empty
          when no matching records exist *or* when a fatal error occurred.
        - **errors**: Per-record non-fatal errors (``is_fatal=False``) and/or
          a single fatal error (``is_fatal=True``).  A fatal error means the
          caller must not advance the checkpoint.

    Requirements: 4.1, 4.3, 4.4, 12.3, 12.4
    """
    errors: list[JournalReadError] = []

    # Derive the S3 Metadata bucket namespace from the bucket name.
    # S3 Metadata converts dots to underscores; hyphens are preserved.
    bucket_namespace = "b_" + bucket_name.replace(".", "_")

    # ------------------------------------------------------------------
    # Step 1: start the Athena query
    # ------------------------------------------------------------------
    query = _build_query(bucket_namespace, bucket_name, since_timestamp, until_timestamp)

    try:
        query_execution_id = _start_query(
            athena_client, query, athena_workgroup, output_location
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "UnknownError")
        cause = (
            f"Athena start_query_execution failed for bucket {bucket_name!r} "
            f"({error_code}): {exc}"
        )
        logger.error("%s | %s | %s", _COMPONENT, bucket_name, cause)
        # A missing namespace can also surface here, as an InvalidRequestException
        # at submission rather than a FAILED terminal state, so the same
        # classification applies to this branch.
        return [], [JournalReadError(
            bucket=bucket_name,
            cause=cause,
            is_fatal=True,
            is_journal_unavailable=_is_journal_unavailable_reason(str(exc)),
        )]

    # ------------------------------------------------------------------
    # Step 2: poll until the query reaches a terminal state
    # ------------------------------------------------------------------
    try:
        final_state = _poll_query(athena_client, query_execution_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "UnknownError")
        cause = (
            f"Athena get_query_execution failed for query "
            f"{query_execution_id!r} ({error_code}): {exc}"
        )
        logger.error("%s | %s | %s", _COMPONENT, bucket_name, cause)
        return [], [JournalReadError(bucket=bucket_name, cause=cause, is_fatal=True)]
    except ValueError as exc:
        # Query timed out without reaching a terminal state
        cause = str(exc)
        logger.error("%s | %s | %s", _COMPONENT, bucket_name, cause)
        return [], [JournalReadError(bucket=bucket_name, cause=cause, is_fatal=True)]

    if final_state in ("FAILED", "CANCELLED"):
        reason = _get_query_failure_reason(athena_client, query_execution_id)
        cause = (
            f"Athena query {query_execution_id!r} {final_state.lower()} "
            f"for bucket {bucket_name!r}: {reason}"
        )
        logger.error("%s | %s | %s", _COMPONENT, bucket_name, cause)
        return [], [JournalReadError(
            bucket=bucket_name,
            cause=cause,
            is_fatal=True,
            is_journal_unavailable=_is_journal_unavailable_reason(reason),
        )]

    # ------------------------------------------------------------------
    # Step 3: retrieve and parse result rows
    # ------------------------------------------------------------------
    ops: list[TaggingOperation] = []

    try:
        for row in _iter_result_rows(athena_client, query_execution_id):
            op, err = _parse_row(row, bucket_name)
            if err is not None:
                # Non-fatal per-record error (Req 4.5, 12.4): log and collect
                logger.warning(
                    "%s | %s | per-record skip: %s",
                    _COMPONENT,
                    bucket_name,
                    err.cause,
                )
                errors.append(err)
            else:
                assert op is not None  # _parse_row guarantees one of op/err is set
                ops.append(op)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "UnknownError")
        cause = (
            f"Athena get_query_results failed for query {query_execution_id!r} "
            f"({error_code}): {exc}"
        )
        logger.error("%s | %s | %s", _COMPONENT, bucket_name, cause)
        # Treat a results-fetch failure as fatal: we cannot trust the partial
        # result set, so leave the checkpoint unchanged (Req 4.4).
        return [], [JournalReadError(bucket=bucket_name, cause=cause, is_fatal=True)]

    return ops, errors
