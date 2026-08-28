"""Read and validate S3 Batch Operations completion reports."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import unquote

from botocore.exceptions import ClientError

from src.core.models import ManifestEntry, normalize_version_id

_REPORT_COLUMN_ORDER_NOTE = """\
The completion report's ``ErrorCode`` and ``HTTPStatusCode`` columns are not
emitted in the order AWS documents, so this reader does not rely on their
position.

Under report schema version ``Report_CSV_20180820`` the observed layout is:

    0 Bucket, 1 Key, 2 VersionId, 3 TaskStatus,
    4 HTTPStatusCode, 5 ErrorCode, 6 ResultMessage

while the ``ReportSchema`` string inside the report's own ``manifest.json``
declares:

    Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage

Observed on two real jobs in ``us-west-2``:

    0f65a1b7-9b4c-4124-a1ad-06ea77d7224f  2026-07-21  100,001 succeeded, 1 failed
    b2f9f42d-dba4-4a77-945e-074cef95450e  2026-08-27  3 succeeded, 0 failed

The second has no failed task at all and still emits HTTP status before error
code (``succeeded,200,,success``), so the ordering is a property of the schema
version rather than of failure rows. A schema version this Solution has not seen
could present the two either way round, which is why
:func:`_resolve_status_and_error` decides by content.

Both reports are committed under ``tests/fixtures/bops_reports/`` and asserted on
by ``tests/test_bops_report_golden.py``, so this note cannot drift away from the
behavior without a test failing.
"""

_REPORT_FORMAT = "Report_CSV_20180820"
_REPORT_SCHEMA = (
    "Bucket",
    "Key",
    "VersionId",
    "TaskStatus",
    "ErrorCode",
    "HTTPStatusCode",
    "ResultMessage",
)


@dataclass(frozen=True)
class BopsCompletionReport:
    """A validated S3 Batch Operations completion report."""

    created_at: datetime
    entries: tuple[ManifestEntry, ...]


class CompletionReportNotReady(Exception):
    """Raised when a completion report has not been fully written yet."""


class CompletionReportMalformed(Exception):
    """Raised when a completion report fails integrity or schema validation."""


def read_bops_completion_report(
    s3_client,
    state_bucket: str,
    report_prefix: str,
    job_id: str,
    expected_row_count: int,
) -> BopsCompletionReport:
    """Read a complete, manifest-led completion report for one job.

    The top-level manifest is the commit marker. Missing objects remain
    retryable; invalid manifests, rows, checksums, identities, and row counts
    are rejected rather than returning a partial report.

    *expected_row_count* is ``NumberOfTasksSucceeded + NumberOfTasksFailed``
    from ``DescribeJob``, and those counts are a sound signal. Job
    ``0f65a1b7-9b4c-4124-a1ad-06ea77d7224f`` reports
    ``TotalNumberOfTasks: 100002, NumberOfTasksSucceeded: 100001,
    NumberOfTasksFailed: 1``, and its report parses to exactly 100,002 rows
    across two result objects. An earlier docstring in ``job_recovery`` claimed
    these counts were unreliable at large object counts, citing that same job
    with the two figures transposed; the job is still queryable and it is not.

    The caller must not pass ``0`` for a job whose ``DescribeJob`` response
    carried no ``ProgressSummary`` at all. An absent summary means the count is
    unknown, not zero, and zero takes the caller's synthetic-empty-report path,
    which would mark the job processed with no outcomes.
    """
    if expected_row_count < 0:
        raise ValueError("expected_row_count must not be negative")

    manifest_key = _manifest_key(report_prefix, job_id)
    manifest_body = _get_object_body(s3_client, state_bucket, manifest_key)
    manifest = _parse_manifest(manifest_body)
    created_at, result_descriptors = _validate_manifest(manifest)

    entries: list[ManifestEntry] = []
    for descriptor in result_descriptors:
        result_key = descriptor["Key"]
        result_body = _get_object_body(s3_client, state_bucket, result_key)
        _verify_md5(result_body, descriptor["MD5Checksum"], result_key)
        entries.extend(_parse_report_csv(result_body.decode("utf-8")))

    if len(entries) != expected_row_count:
        raise CompletionReportMalformed(
            f"report contains {len(entries)} rows; expected {expected_row_count}"
        )

    identities = {
        (entry.source_bucket, entry.object_key, entry.version_id) for entry in entries
    }
    if len(identities) != len(entries):
        raise CompletionReportMalformed("report contains duplicate object identities")

    return BopsCompletionReport(created_at=created_at, entries=tuple(entries))


def report_manifest_written_at(
    s3_client,
    state_bucket: str,
    report_prefix: str,
    job_id: str,
) -> datetime | None:
    """Return when a job's top-level completion-report manifest was written.

    ``None`` means the manifest does not exist. The report manifest is the
    completion report's commit marker, so checking this exact key prevents
    sidecar or partially visible result objects from suppressing a
    report-missing alert.

    The timestamp is returned rather than a bare boolean because it is the only
    honest clock for "how long has this report gone unread". Job timestamps are
    not: a job's duration is unbounded, so measuring from ``CreationTime``
    (the fallback when ``TerminationDate`` is absent) gives a long-running job no
    grace at all. ``LastModified`` on the manifest measures exactly the interval
    that matters and is independent of how long the job took.

    ``None`` is also returned when the response carries no ``LastModified``,
    which no live S3 response does, so a caller cannot mistake an unusable
    timestamp for a fresh one.
    """
    try:
        response = s3_client.head_object(
            Bucket=state_bucket,
            Key=_manifest_key(report_prefix, job_id),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    written_at = response.get("LastModified")
    return written_at if isinstance(written_at, datetime) else None


def _manifest_key(report_prefix: str, job_id: str) -> str:
    """Return the exact manifest key S3 Batch Operations writes.

    S3 appends ``/job-<id>/manifest.json`` to the submitted report prefix.
    The Solution submits prefixes ending in ``/``, so S3 places the manifest
    below a double slash. Preserve the prefix rather than normalizing it: a
    historical job must be read from the same key S3 wrote.

    Verified on two real jobs in ``us-west-2`` whose report manifests are at
    ``<prefix>//job-<id>/manifest.json``:
    ``0f65a1b7-9b4c-4124-a1ad-06ea77d7224f`` (2026-07-21) and
    ``b2f9f42d-dba4-4a77-945e-074cef95450e`` (2026-08-27). The 1.0.1 reader
    listed the prefix and was insensitive to the separator; this exact-key read
    is not, so the double slash is load-bearing rather than cosmetic.
    """
    return f"{report_prefix}/job-{job_id}/manifest.json"


def _get_object_body(s3_client, bucket: str, key: str) -> bytes:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            raise CompletionReportNotReady(f"report object is not available: {key}") from exc
        raise

    try:
        body = response["Body"].read()
    except (KeyError, AttributeError) as exc:
        raise CompletionReportMalformed(f"report object has no readable body: {key}") from exc
    if not isinstance(body, bytes):
        raise CompletionReportMalformed(f"report object body is not bytes: {key}")
    return body


def _parse_manifest(body: bytes) -> dict:
    try:
        manifest = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CompletionReportMalformed("report manifest is not valid JSON") from exc
    if not isinstance(manifest, dict):
        raise CompletionReportMalformed("report manifest must be an object")
    return manifest


def _validate_manifest(manifest: dict) -> tuple[datetime, list[dict[str, str]]]:
    if manifest.get("Format") != _REPORT_FORMAT:
        raise CompletionReportMalformed("report manifest has an unsupported format")

    schema = manifest.get("ReportSchema")
    if not isinstance(schema, str) or tuple(part.strip() for part in schema.split(",")) != _REPORT_SCHEMA:
        raise CompletionReportMalformed("report manifest has an unsupported schema")

    creation_date = manifest.get("ReportCreationDate")
    if not isinstance(creation_date, str):
        raise CompletionReportMalformed("report manifest has no creation date")
    try:
        created_at = datetime.fromisoformat(creation_date.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CompletionReportMalformed("report manifest has an invalid creation date") from exc
    if created_at.tzinfo is None:
        raise CompletionReportMalformed("report manifest creation date has no timezone")

    results = manifest.get("Results")
    if not isinstance(results, list):
        raise CompletionReportMalformed("report manifest has no results list")

    validated_results: list[dict[str, str]] = []
    for result in results:
        if not isinstance(result, dict):
            raise CompletionReportMalformed("report manifest contains an invalid result descriptor")
        key = result.get("Key")
        checksum = result.get("MD5Checksum")
        if not isinstance(key, str) or not key:
            raise CompletionReportMalformed("report result has no object key")
        if not isinstance(checksum, str):
            raise CompletionReportMalformed("report result has no MD5 checksum")
        if not _is_valid_md5_checksum(checksum):
            raise CompletionReportMalformed("report result has an invalid MD5 checksum")
        validated_results.append({"Key": key, "MD5Checksum": checksum})

    return created_at, validated_results


def _is_valid_md5_checksum(checksum: str) -> bool:
    """Accept the hexadecimal form S3 emits and the base64 form used by fixtures."""
    if len(checksum) == hashlib.md5(usedforsecurity=False).digest_size * 2:
        try:
            bytes.fromhex(checksum)
        except ValueError:
            return False
        return True
    try:
        decoded_checksum = base64.b64decode(checksum, validate=True)
    except ValueError:
        return False
    return len(decoded_checksum) == hashlib.md5(usedforsecurity=False).digest_size


def _verify_md5(body: bytes, expected_checksum: str, key: str) -> None:
    digest = hashlib.md5(body, usedforsecurity=False).digest()
    actual_checksum = (
        digest.hex()
        if len(expected_checksum) == len(digest) * 2
        else base64.b64encode(digest).decode("ascii")
    )
    if actual_checksum != expected_checksum:
        raise CompletionReportMalformed(f"report result checksum does not match: {key}")


def _parse_report_csv(csv_text: str) -> list[ManifestEntry]:
    """Parse strictly valid seven-column BOPS result rows.

    A blank line yields ``[]`` from :func:`csv.reader` and is skipped rather
    than rejected: result objects are CRLF-terminated, so a trailing or
    interior blank line must not make an otherwise valid report permanently
    malformed. Any other unexpected column count is still malformed.

    ``VersionId`` is normalized through :func:`normalize_version_id`, so every
    spelling of the null version becomes ``None`` and a null-version object
    carries the same identity here as it does in the manifest and in state.

    ``HTTPStatusCode`` and ``ErrorCode`` are resolved by content rather than by
    position; see :data:`_REPORT_COLUMN_ORDER_NOTE`.
    """
    entries: list[ManifestEntry] = []
    for row in csv.reader(io.StringIO(csv_text)):
        if not row:
            continue
        if len(row) != len(_REPORT_SCHEMA):
            raise CompletionReportMalformed("report contains a malformed row")
        bucket, encoded_key, version_id_raw, task_status, first, second, result_message = row
        if not bucket or not encoded_key:
            raise CompletionReportMalformed("report contains a malformed row")
        http_status, error_code = _resolve_status_and_error(first, second)
        entries.append(
            ManifestEntry(
                source_bucket=bucket,
                object_key=unquote(encoded_key),
                version_id=normalize_version_id(version_id_raw),
                error_code=error_code or None,
                task_status=task_status or None,
                http_status_code=http_status or None,
                result_message=result_message or None,
            )
        )
    return entries


def _looks_like_http_status(value: str) -> bool:
    candidate = value.strip()
    return len(candidate) == 3 and candidate.isdigit() and 100 <= int(candidate) <= 599


def _resolve_status_and_error(first: str, second: str) -> tuple[str, str]:
    """Resolve the two service variants for status and error column order."""
    if _looks_like_http_status(second) and not _looks_like_http_status(first):
        return second, first
    return first, second
