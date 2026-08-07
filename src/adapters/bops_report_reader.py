"""BOPS_Completion_Report reader — list-then-read of the S3 Batch Operations
job completion report.

This adapter reads a BOPS-service-written CSV under a service-generated
subpath, with its own column schema. The report is authored and located by
S3 Batch Operations rather than by the Solution, and accessed with a
list-then-read pattern rather than a direct single-object read.

The report transposes ``ErrorCode`` and ``HTTPStatusCode`` relative to the
order AWS documents, and the layout belongs to the report schema version named
by ``Report.Format`` rather than being fixed. This reader therefore resolves
those two columns by content instead of by position, so it parses either order
correctly. See ``_REPORT_COLUMN_ORDER_NOTE`` in this module before altering
that.

S3 Batch Operations writes the report under a service-generated subpath
(``job-<job_id>/results/<manifest-hash>.csv``) of the configured
``Report.Prefix``, so the reader must list every object under the prefix
before reading any of them — the exact final key is not knowable in
advance.

Requirements: 2.1, 2.7
"""
from __future__ import annotations

import csv
import io
from urllib.parse import unquote

from src.core.models import ManifestEntry

# ---------------------------------------------------------------------------
# read_bops_completion_report — public interface
# ---------------------------------------------------------------------------


def read_bops_completion_report(
    s3_client,
    state_bucket: str,
    report_prefix: str,
) -> list[ManifestEntry]:
    """List every CSV object under ``report_prefix``, read and parse each,
    and return one ``ManifestEntry`` per row.

    Parses all seven columns of the report CSV; every listed object version is
    included regardless of its per-task ``TaskStatus``, since even a
    task-failed object has a source replication status worth reporting
    (design.md Decision 4). ``ErrorCode`` is empty (parsed as ``None``) for a
    succeeded task, and carries the service error code (e.g.
    ``InitiateReplicationNotPermitted``, ``SrcObjectNotEligible``) for a
    failed one.

    The column order used is the verified one, not the documented one — see
    ``_REPORT_COLUMN_ORDER_NOTE``.

    An empty ``VersionId`` column (an unversioned bucket) is parsed as
    ``version_id=None`` — the null-version marker used elsewhere in the
    Solution's manifest handling (Requirement 2.3).

    Parameters
    ----------
    s3_client:
        A boto3 ``s3`` client scoped to the account/region that owns
        ``state_bucket``.
    state_bucket:
        The State_Bucket where the BOPS_Completion_Report was written.
    report_prefix:
        The prefix passed as ``Report.Prefix`` to ``submit_batch_job`` for
        this job — the reader lists every object under this prefix
        (paginating as needed) rather than reading one specific key,
        because the service-generated subpath under it is not knowable in
        advance.

    Returns
    -------
    list[ManifestEntry]
        One entry per report row, across every CSV object found under
        ``report_prefix``, in listing then row order. Empty when no report
        object exists yet under the prefix.

    Raises
    ------
    Exception
        Any exception from the underlying S3 ``list_objects_v2`` or
        ``get_object`` calls is propagated unchanged — the caller (the
        orchestrator's per-config ``DescribeJob`` loop) is responsible for
        isolating this failure per Requirement 6.1.

    Requirements: 2.1, 2.7
    """
    entries: list[ManifestEntry] = []
    for key in _list_report_object_keys(s3_client, state_bucket, report_prefix):
        if not _is_report_results_csv_key(key):
            # Skip the job's manifest.json and manifest.json.md5 sidecar
            # objects written alongside the results/ CSV under the same
            # prefix — only the results/*.csv files contain per-object
            # completion rows; the sidecars are not CSV and would otherwise
            # be misparsed as garbage rows.
            continue
        response = s3_client.get_object(Bucket=state_bucket, Key=key)
        csv_text = response["Body"].read().decode("utf-8")
        entries.extend(_parse_report_csv(csv_text))
    return entries


# ---------------------------------------------------------------------------
# report_object_exists — public interface (design.md Decision 9)
# ---------------------------------------------------------------------------


def report_object_exists(
    s3_client,
    state_bucket: str,
    report_prefix: str,
) -> bool:
    """Existence-only check for whether a BOPS_Completion_Report object has
    appeared under ``report_prefix``.

    A lighter variant of :func:`read_bops_completion_report`'s list step:
    issues a single ``list_objects_v2`` call (capped at one page via
    ``MaxKeys=1`` — a single listed object is enough to answer the
    existence question, so there is no need to paginate through every
    object under the prefix) and returns whether any object was found. Does
    NOT read or parse any CSV content — used by ``check_report_handler``
    (design.md Decision 9) to cheaply detect whether a terminal job's report
    has appeared, without paying the cost of a full read-and-parse.

    Parameters
    ----------
    s3_client:
        A boto3 ``s3`` client scoped to the account/region that owns
        ``state_bucket``.
    state_bucket:
        The State_Bucket where the BOPS_Completion_Report would be written.
    report_prefix:
        The prefix passed as ``Report.Prefix`` to ``submit_batch_job`` for
        the job being checked.

    Returns
    -------
    bool
        ``True`` if at least one object exists under ``report_prefix``,
        ``False`` if the prefix is empty (no report has appeared yet).

    Raises
    ------
    Exception
        Any exception from the underlying ``list_objects_v2`` call is
        propagated unchanged — the caller (``check_report_handler``) is
        responsible for isolating this failure per Requirement 8.8.

    Requirements: 8.1
    """
    response = s3_client.list_objects_v2(
        Bucket=state_bucket, Prefix=report_prefix, MaxKeys=1
    )
    return bool(response.get("Contents"))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_report_results_csv_key(key: str) -> bool:
    """True iff *key* is a BOPS_Completion_Report results CSV, not a sidecar.

    S3 Batch Operations writes three object types under ``Report.Prefix``:
    ``job-<job_id>/manifest.json`` (an envelope describing the report),
    ``job-<job_id>/manifest.json.md5`` (its checksum), and one or more
    ``job-<job_id>/results/<hash>.csv`` files containing the actual
    per-object completion rows. Only the ``results/`` CSVs should be parsed
    as report data; the manifest and its checksum are not row-oriented CSV
    and would otherwise be misparsed as garbage entries.
    """
    return "/results/" in key and key.endswith(".csv")


def _list_report_object_keys(s3_client, state_bucket: str, report_prefix: str) -> list[str]:
    """List every object key under ``report_prefix``, paginating as needed."""
    keys: list[str] = []
    continuation_token: str | None = None
    while True:
        kwargs: dict = {"Bucket": state_bucket, "Prefix": report_prefix}
        if continuation_token is not None:
            kwargs["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            keys.append(obj["Key"])
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return keys


_REPORT_COLUMN_ORDER_NOTE = """\
The completion report's ``ErrorCode`` and ``HTTPStatusCode`` columns are not
in the order AWS documents, so this reader does not rely on their position.

Under report schema version ``Report_CSV_20180820`` the observed layout is:

    0 Bucket, 1 Key, 2 VersionId, 3 TaskStatus,
    4 HTTPStatusCode, 5 ErrorCode, 6 ResultMessage

Both "Examples: S3 Batch Operations completion reports" and the
``ReportSchema`` string the service writes into the report's own
``manifest.json`` instead declare:

    Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage

Observed on a two-task job, one succeeding and one failing, so both row shapes
were available. Its manifest.json carried the declaration above while its
results rows read:

    ...,succeeded,200,,success
    ...,failed,500,SrcObjectNotEligible,Object is not eligible for replication

The values identify themselves: 200 and 500 are HTTP status codes,
SrcObjectNotEligible is an error code, and "success" is the ResultMessage. A
succeeded row leaves ErrorCode empty, which in the documented order would put
the empty field at index 4; instead the empty field is at index 5 and index 4
holds 200.

The documented order is also contradicted by AWS's own published examples. On
the "Examples: S3 Batch Operations completion reports" page, every example
row across the Lambda-invoke and Compute-checksum sets places the status code
before the error code, matching what is observed rather than the column lists
and ``ReportSchema`` string on that same page. For instance the documented
failed Lambda row is ``...,failed,200,PermanentFailure,"Lambda returned
function error: ..."``, where 200 is the status and PermanentFailure the code.

``Report.Format`` names a report *schema version*, so this layout is a property
of that version rather than a fixed fact, and a future version could reasonably
present these columns in the declared order instead. That is why the two are
resolved by content here rather than by index: it costs almost nothing and it
removes the layout from the set of things this reader can be wrong about. See
:func:`_resolve_status_and_error`.

Getting these two columns the wrong way round fails silently rather than
loudly, which is what makes the content check worth having. Both are strings,
and both are populated on a failure, so a transposed read yields a
plausible-looking value instead of an error: reading ErrorCode from the status
column returns a number that never equals any real error code, so every
comparison against a known code fails while every succeeded task appears to
carry the error code "200". That defect was live in this reader and no test
caught it, because the test helper encoded the same wrong order.
"""

# Bounds for recognising an HTTP status code. Both ends are inclusive.
_HTTP_STATUS_MIN = 100
_HTTP_STATUS_MAX = 599


def _looks_like_http_status(value: str) -> bool:
    """True iff *value* is a bare three-digit HTTP status code.

    Deliberately strict. An ``ErrorCode`` is either empty or a symbolic
    identifier such as ``SrcObjectNotEligible``, ``PermanentFailure``, or
    ``AccessDenied``, none of which satisfies this, so the test cleanly
    separates the two columns.
    """
    candidate = value.strip()
    if len(candidate) != 3 or not candidate.isdigit():
        return False
    return _HTTP_STATUS_MIN <= int(candidate) <= _HTTP_STATUS_MAX


def _resolve_status_and_error(first: str, second: str) -> tuple[str, str]:
    """Resolve report columns 4 and 5 into ``(http_status_code, error_code)``.

    Decided per row by content rather than by position, so the reader is
    correct under both the emitted order and the declared order. The layout is
    a property of the report schema version named by ``Report.Format`` (see
    :data:`_REPORT_COLUMN_ORDER_NOTE`), so a version this Solution has not seen
    could present these two columns either way round. Resolving by content
    means such a difference needs no change here and cannot silently corrupt a
    parse.

    The two columns are trivially separable: exactly one of them is ever a
    bare three-digit number.

    Ambiguous inputs fall back to the order observed under
    ``Report_CSV_20180820``, which is the version this Solution requests:

    * Neither value looks like a status code, which is the case when both are
      empty. The choice cannot matter, since an empty ``ErrorCode`` parses to
      ``None`` either way.
    * Both values look like a status code. Not observed in any report or in
      any documented example, and it would require a numeric ``ErrorCode``.
      Preferring the known layout keeps behavior predictable if it ever
      happens.
    """
    first_is_status = _looks_like_http_status(first)
    second_is_status = _looks_like_http_status(second)

    if second_is_status and not first_is_status:
        # Declared order: ErrorCode then HTTPStatusCode.
        return second, first
    # Observed order under Report_CSV_20180820, and the fallback for the two
    # ambiguous cases described above.
    return first, second


def _parse_report_csv(csv_text: str) -> list[ManifestEntry]:
    """Parse a single BOPS_Completion_Report CSV's rows into ``ManifestEntry`` values.

    Headerless CSV — S3 Batch Operations completion reports carry no header
    row, matching the Solution's own manifest CSV convention.

    ``HTTPStatusCode`` and ``ErrorCode`` are resolved by content rather than by
    position, so both the emitted and the declared column order parse
    correctly. See :data:`_REPORT_COLUMN_ORDER_NOTE`.
    """
    entries: list[ManifestEntry] = []
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        if not row:
            continue
        bucket = row[0]
        # The key is percent-decoded to match the form Tracked_Objects are
        # keyed by. The Solution's own manifest percent-encodes object keys
        # (``ManifestEntry.to_csv_row``, ``/`` preserved) so that a comma or
        # newline in a key cannot break the CSV, and S3 Batch Operations
        # echoes the manifest's key into the completion report rather than
        # the decoded original — so without this the report's key never
        # matches the Tracked_Object for any key containing a character
        # that needed encoding, and that object never resolves.
        #
        # AWS does not document the report's key encoding, and every example
        # in "Examples: S3 Batch Operations completion reports" uses a key
        # needing no encoding. Verified empirically instead, against job
        # ceeee132-7fb2-4abf-826e-4d05b6272ec0 in the us-west-2 test
        # deployment: an object keyed ``retag-verify/pct%20literal.txt`` (a
        # literal percent sequence) was written into the manifest as
        # ``retag-verify/pct%2520literal.txt``, and the completion report's
        # Key column came back byte-identical to the manifest form, not as
        # the original key. So the report echoes the manifest key and this
        # decode is its exact inverse.
        #
        # A literal percent sequence is the only key shape that
        # distinguishes the two possibilities — for every other key, the
        # encoded and original forms decode to the same string — which is
        # why the test for this in tests/adapters/test_bops_report_reader.py
        # centres on that case.
        key = unquote(row[1]) if len(row) > 1 else ""
        version_id_raw = row[2] if len(row) > 2 else ""
        version_id = version_id_raw if version_id_raw else None
        # ErrorCode (column index 4): empty for a succeeded task. Carried
        # through so the orchestrator can diagnose a permission-shaped
        # failure (e.g. InitiateReplicationNotPermitted) without a second
        # read of the report — see _CompletionHooks.on_job_terminal.
        # Columns 4 and 5 carry HTTPStatusCode and ErrorCode, in an order that
        # is a property of the report schema version rather than a fixed fact,
        # so they are resolved by content. See _REPORT_COLUMN_ORDER_NOTE.
        task_status_raw = row[3] if len(row) > 3 else ""
        http_status_raw, error_code_raw = _resolve_status_and_error(
            row[4] if len(row) > 4 else "",
            row[5] if len(row) > 5 else "",
        )
        result_message_raw = row[6] if len(row) > 6 else ""
        error_code = error_code_raw if error_code_raw else None
        entries.append(
            ManifestEntry(
                source_bucket=bucket,
                object_key=key,
                version_id=version_id,
                error_code=error_code,
                task_status=task_status_raw if task_status_raw else None,
                http_status_code=http_status_raw if http_status_raw else None,
                result_message=result_message_raw if result_message_raw else None,
            )
        )
    return entries
