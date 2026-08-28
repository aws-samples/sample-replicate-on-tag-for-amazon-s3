"""Golden-file tests over completion reports S3 actually wrote.

Every other reader test builds its own report, which means it can only confirm
that the reader agrees with the test author. During the 1.1.0 review that
agreement hid three defects at once: the column order was recorded backwards, a
null version was stored as the literal string ``'null'``, and the row-count check
was justified by a transposed pair of task counts. These tests read real bytes
instead, so a wrong assumption has nothing to agree with.

Fixtures, their provenance, and the one respect in which they are not verbatim:
``tests/fixtures/bops_reports/README.md``.

**Validates: Requirements 8.7, 9.1, 9.2, 9.3, 10.3**
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
from pathlib import Path

import pytest

from src.adapters.bops_report_reader import (
    CompletionReportMalformed,
    read_bops_completion_report,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "bops_reports"

# Job b2f9f42d-dba4-4a77-945e-074cef95450e, us-west-2, 2026-08-27. DescribeJob
# reported TotalNumberOfTasks 3, succeeded 3, failed 0. Bucket names in the
# fixtures are placeholders; see the fixtures README for what was changed.
_NULLTEST_JOB_ID = "b2f9f42d-dba4-4a77-945e-074cef95450e"
_NULLTEST_PREFIX = "completion-reports/nulltest/"
_NULLTEST_BUCKET = "example-state-bucket"
_NULLTEST_EXPECTED_ROWS = 3

# The exact keys S3 wrote, including the double slash a report prefix ending in
# "/" produces. Spelled out rather than derived so that a change to
# _manifest_key's own logic cannot make this test pass by agreeing with itself.
_NULLTEST_MANIFEST_KEY = (
    f"completion-reports/nulltest//job-{_NULLTEST_JOB_ID}/manifest.json"
)
_NULLTEST_RESULT_KEY = (
    f"completion-reports/nulltest//job-{_NULLTEST_JOB_ID}/results/"
    "f47b7e9d4ea62beb3251ae3ae522503744384fda.csv"
)


class _StubS3:
    """Serves fixture bytes for exact keys; raises 404 for anything else."""

    def __init__(self, bucket: str, objects: dict[str, bytes]) -> None:
        self._bucket = bucket
        self._objects = objects
        self.requested: list[str] = []

    def get_object(self, Bucket, Key):  # noqa: N803 — botocore parameter casing
        assert Bucket == self._bucket, Bucket
        self.requested.append(Key)
        try:
            body = self._objects[Key]
        except KeyError:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
            ) from None
        return {"Body": io.BytesIO(body)}


def _read_fixture(*parts: str) -> bytes:
    return (_FIXTURES.joinpath(*parts)).read_bytes()


@pytest.fixture
def nulltest_client() -> _StubS3:
    return _StubS3(
        _NULLTEST_BUCKET,
        {
            _NULLTEST_MANIFEST_KEY: _read_fixture("nulltest", "manifest.json"),
            _NULLTEST_RESULT_KEY: _read_fixture("nulltest", "result.csv"),
        },
    )


def _read_nulltest(client: _StubS3, expected_rows: int = _NULLTEST_EXPECTED_ROWS):
    return read_bops_completion_report(
        client,
        _NULLTEST_BUCKET,
        _NULLTEST_PREFIX,
        _NULLTEST_JOB_ID,
        expected_rows,
    )


class TestRealReportReadEndToEnd:
    def test_reads_the_real_report_at_the_keys_s3_wrote(self, nulltest_client):
        """Manifest key, declared-result key, MD5, and row count all real."""
        report = _read_nulltest(nulltest_client)

        assert nulltest_client.requested == [
            _NULLTEST_MANIFEST_KEY,
            _NULLTEST_RESULT_KEY,
        ]
        assert len(report.entries) == _NULLTEST_EXPECTED_ROWS

    def test_created_at_comes_from_the_reports_own_manifest(self, nulltest_client):
        report = _read_nulltest(nulltest_client)

        declared = json.loads(_read_fixture("nulltest", "manifest.json"))
        assert report.created_at.isoformat().startswith("2026-08-27T09:49:46")
        assert declared["ReportCreationDate"].startswith("2026-08-27T09:49:46")
        assert report.created_at.tzinfo is not None

    def test_row_count_matches_real_describe_job_totals(self, nulltest_client):
        """succeeded + failed from the real job equals the parsed row count.

        The same equality holds on job 0f65a1b7-9b4c-4124-a1ad-06ea77d7224f at
        100,002 rows, which is the evidence the row-count check rests on.
        """
        tasks_succeeded, tasks_failed = 3, 0

        report = _read_nulltest(nulltest_client, tasks_succeeded + tasks_failed)

        assert len(report.entries) == tasks_succeeded + tasks_failed

    def test_wrong_expected_count_is_rejected(self, nulltest_client):
        with pytest.raises(CompletionReportMalformed, match="expected"):
            _read_nulltest(nulltest_client, _NULLTEST_EXPECTED_ROWS + 1)


class TestNullVersionNormalization:
    """A null version must be one identity, not two.

    Everything downstream of the reader represents the null version as ``None``,
    including ``ManifestEntry.from_versioned_csv_row`` and the ``_NULL_VERSION``
    identity sentinel. A reader that keeps the report's literal ``'null'`` splits
    one object across two identities and puts a bare ``null`` in the operator's
    email.
    """

    def test_pre_versioning_objects_normalize_to_none(self, nulltest_client):
        report = _read_nulltest(nulltest_client)
        by_key = {entry.object_key: entry for entry in report.entries}

        assert by_key["null-a.txt"].version_id is None
        assert by_key["null-b.txt"].version_id is None

    def test_real_version_id_is_passed_through_untouched(self, nulltest_client):
        report = _read_nulltest(nulltest_client)
        by_key = {entry.object_key: entry for entry in report.entries}

        assert by_key["real-c.txt"].version_id == "s6APnfF_JsFwgASf8NsprnRnUaBqVaiP"

    def test_raw_fixture_really_does_carry_the_literal_null(self):
        """Guards the test itself: the fixture must contain what we claim.

        Without this, a fixture that silently lost its ``null`` column would make
        the normalization tests above pass for the wrong reason.
        """
        rows = _read_fixture("nulltest", "result.csv").decode("utf-8").splitlines()
        null_rows = [row for row in rows if row.split(",")[1].startswith("null-")]

        assert len(null_rows) == 2
        assert all(row.split(",")[2] == "null" for row in null_rows), null_rows

    def test_backslash_n_from_an_empty_manifest_field_normalizes_to_none(self):
        """The other spelling, from the real 2026-07-21 failed row.

        The row is verbatim; the manifest around it is fixture-built because its
        real sibling result object is 10 MB. See the fixtures README.
        """
        result_body = _read_fixture("failed-row-2026-07-21.csv")
        job_id = "0f65a1b7-9b4c-4124-a1ad-06ea77d7224f"
        prefix = "completion-reports/legacy/"
        manifest_key = f"{prefix}/job-{job_id}/manifest.json"
        result_key = f"{prefix}/job-{job_id}/results/failed.csv"
        manifest = {
            "Format": "Report_CSV_20180820",
            "ReportCreationDate": "2026-07-21T21:28:58.559244258Z",
            "Results": [
                {
                    "TaskExecutionStatus": "failed",
                    "Bucket": "fixture-state-bucket",
                    "MD5Checksum": hashlib.md5(  # noqa: S324 — S3 declares MD5
                        result_body, usedforsecurity=False
                    ).hexdigest(),
                    "Key": result_key,
                }
            ],
            "ReportSchema": (
                "Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, "
                "ResultMessage"
            ),
        }
        client = _StubS3(
            "fixture-state-bucket",
            {
                manifest_key: json.dumps(manifest).encode("utf-8"),
                result_key: result_body,
            },
        )

        report = read_bops_completion_report(
            client, "fixture-state-bucket", prefix, job_id, 1
        )

        (entry,) = report.entries
        assert b",\\N," in result_body, result_body
        assert entry.version_id is None
        assert entry.object_key == "verify-test/null-version-req2.txt"

    def test_a_version_id_resembling_a_sentinel_is_not_normalized(self):
        """Exact match only: S3 version IDs are case-sensitive."""
        from src.core.models import normalize_version_id

        assert normalize_version_id("NULL") == "NULL"
        assert normalize_version_id("Null") == "Null"
        assert normalize_version_id("nullish") == "nullish"
        assert normalize_version_id("null") is None
        assert normalize_version_id("\\N") is None
        assert normalize_version_id("") is None

    def test_normalization_agrees_with_the_manifest_parser(self):
        """The inbound and outbound halves must not drift apart."""
        from src.core.models import ManifestEntry, normalize_version_id

        for token in ("", "null", "\\N"):
            row = f"bucket,key.txt,{token}"
            assert ManifestEntry.from_versioned_csv_row(row).version_id is None
            assert normalize_version_id(token) is None


class TestRealColumnOrder:
    """The report's emitted column order contradicts its own ReportSchema."""

    def test_declared_schema_and_emitted_rows_disagree(self):
        """Recorded as an assertion so the fact cannot be lost again."""
        manifest = json.loads(_read_fixture("nulltest", "manifest.json"))
        declared = [part.strip() for part in manifest["ReportSchema"].split(",")]
        row = _read_fixture("nulltest", "result.csv").decode("utf-8").splitlines()[0]
        columns = row.split(",")

        assert declared[4:6] == ["ErrorCode", "HTTPStatusCode"]
        # The emitted row puts the HTTP status where ErrorCode is declared.
        assert columns[4] == "200"
        assert columns[5] == ""

    def test_status_and_error_are_resolved_by_content(self, nulltest_client):
        report = _read_nulltest(nulltest_client)

        assert {entry.http_status_code for entry in report.entries} == {"200"}
        assert {entry.error_code for entry in report.entries} == {None}

    def test_a_failed_row_resolves_its_error_code_and_status(self):
        """On a job with no failures the order is unprovable from success rows
        alone; the real failed row supplies the other shape."""
        row = _read_fixture("failed-row-2026-07-21.csv").decode("utf-8").strip()
        columns = row.split(",")

        assert columns[3] == "failed"
        assert columns[4] == "500"
        assert columns[5] == "SrcObjectNotFound"


class TestRealTaskStatusMapping:
    def test_succeeded_rows_map_to_complete(self, nulltest_client):
        from src.core.completion_tracker import outcome_from_report_row

        report = _read_nulltest(nulltest_client)

        assert {entry.task_status for entry in report.entries} == {"succeeded"}
        assert {outcome_from_report_row(entry) for entry in report.entries} == {
            "COMPLETE"
        }

    def test_the_real_failed_row_maps_to_failed(self):
        from src.core.completion_tracker import outcome_from_report_row
        from src.core.models import ManifestEntry

        row = _read_fixture("failed-row-2026-07-21.csv").decode("utf-8").strip()
        columns = row.split(",")
        entry = ManifestEntry(
            source_bucket=columns[0],
            object_key=columns[1],
            task_status=columns[3],
        )

        assert outcome_from_report_row(entry) == "FAILED"


class TestRealReportIntegrityChecks:
    def test_a_tampered_result_body_fails_the_declared_checksum(
        self, nulltest_client
    ):
        """The MD5 in the real manifest is enforced, not merely parsed."""
        tampered = _read_fixture("nulltest", "result.csv").replace(b"null-a", b"null-z")
        nulltest_client._objects[_NULLTEST_RESULT_KEY] = tampered

        with pytest.raises(CompletionReportMalformed, match="checksum"):
            _read_nulltest(nulltest_client)

    def test_the_declared_checksum_is_hexadecimal_as_s3_emits_it(self):
        manifest = json.loads(_read_fixture("nulltest", "manifest.json"))
        declared = manifest["Results"][0]["MD5Checksum"]
        body = _read_fixture("nulltest", "result.csv")
        digest = hashlib.md5(body, usedforsecurity=False).digest()  # noqa: S324

        assert declared == digest.hex()
        assert declared != base64.b64encode(digest).decode("ascii")

    def test_a_trailing_blank_line_does_not_break_a_real_report(self):
        """Result objects are CRLF-terminated; a blank line must be skipped."""
        from src.adapters.bops_report_reader import _parse_report_csv

        body = _read_fixture("nulltest", "result.csv").decode("utf-8")

        assert len(_parse_report_csv(body)) == _NULLTEST_EXPECTED_ROWS
        assert len(_parse_report_csv(body + "\r\n")) == _NULLTEST_EXPECTED_ROWS
        assert len(_parse_report_csv(body + "\n\n")) == _NULLTEST_EXPECTED_ROWS

    def test_a_blank_line_between_rows_is_skipped(self):
        from src.adapters.bops_report_reader import _parse_report_csv

        rows = _read_fixture("nulltest", "result.csv").decode("utf-8").splitlines()
        interleaved = "\r\n".join([rows[0], "", rows[1], "", rows[2]]) + "\r\n"

        assert len(_parse_report_csv(interleaved)) == _NULLTEST_EXPECTED_ROWS

    def test_a_short_row_is_still_malformed(self):
        """Skipping blanks must not weaken the column-count check."""
        from src.adapters.bops_report_reader import _parse_report_csv

        with pytest.raises(CompletionReportMalformed, match="malformed"):
            _parse_report_csv("bucket,key.txt,null,succeeded\r\n")
