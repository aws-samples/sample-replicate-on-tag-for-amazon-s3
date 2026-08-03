"""BOPS_Completion_Report reader — list-then-read of the S3 Batch Operations
job completion report.

This adapter reads a BOPS-service-written CSV under a service-generated
subpath, with its own column schema (``Bucket, Key, VersionId, TaskStatus,
ErrorCode, HTTPStatusCode, ResultMessage``). The report is authored and
located by S3 Batch Operations rather than by the Solution, and accessed
with a list-then-read pattern rather than a direct single-object read.

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

    Parses only the ``Bucket``, ``Key``, and ``VersionId`` columns of the
    report CSV (``Bucket, Key, VersionId, TaskStatus, ErrorCode,
    HTTPStatusCode, ResultMessage``); every listed object version is
    included regardless of its per-task ``TaskStatus``, since even a
    task-failed object has a source replication status worth reporting
    (design.md Decision 4).

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


def _parse_report_csv(csv_text: str) -> list[ManifestEntry]:
    """Parse a single BOPS_Completion_Report CSV's rows into ``ManifestEntry`` values.

    Column schema: ``Bucket, Key, VersionId, TaskStatus, ErrorCode,
    HTTPStatusCode, ResultMessage`` (no header row — S3 Batch Operations
    completion reports are headerless CSVs, matching the Solution's own
    manifest CSV convention).
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
        entries.append(
            ManifestEntry(source_bucket=bucket, object_key=key, version_id=version_id)
        )
    return entries
