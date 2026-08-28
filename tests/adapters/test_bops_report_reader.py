"""Focused tests for manifest-led BOPS completion report reads."""
from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, call

import pytest
from botocore.exceptions import ClientError

from src.adapters.bops_report_reader import (
    BopsCompletionReport,
    CompletionReportMalformed,
    CompletionReportNotReady,
    _manifest_key,
    read_bops_completion_report,
    report_manifest_written_at,
)

_STATE_BUCKET = "state-bucket"
_PREFIX = "completion-reports/cfg-1/manifest-1/"
_JOB_ID = "abc"
_MANIFEST_KEY = f"{_PREFIX}/job-{_JOB_ID}/manifest.json"
_SCHEMA = "Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage"


def _body(contents: bytes) -> MagicMock:
    body = MagicMock()
    body.read.return_value = contents
    return body


def _result(key: str, contents: bytes) -> dict[str, str]:
    # MD5 is the digest the S3 Batch Operations report manifest format specifies,
    # so it is not a choice here; usedforsecurity=False says so, matching
    # data_file_hasher and inventory_manifest_writer. See FP1 in
    # .holmes/accepted-risks.md.
    return {
        "Key": key,
        "MD5Checksum": base64.b64encode(
            hashlib.md5(contents, usedforsecurity=False).digest()
        ).decode("ascii"),
    }


def _s3_result(key: str, contents: bytes) -> dict[str, str]:
    """Mirror the hexadecimal MD5 form emitted by S3 Batch Operations."""
    return {
        "Key": key,
        "MD5Checksum": hashlib.md5(contents, usedforsecurity=False).hexdigest(),
    }


def _manifest(results: list[dict[str, str]], creation_date: str = "2026-08-26T12:34:56Z") -> bytes:
    return json.dumps(
        {
            "Format": "Report_CSV_20180820",
            "ReportCreationDate": creation_date,
            "ReportSchema": _SCHEMA,
            "Results": results,
        }
    ).encode("utf-8")


def _row(
    bucket: str = "source-bucket",
    key: str = "object.txt",
    version_id: str = "version-1",
    status: str = "succeeded",
    http_status: str = "200",
    error_code: str = "",
    message: str = "success",
) -> bytes:
    return f"{bucket},{key},{version_id},{status},{http_status},{error_code},{message}".encode()


def _client(manifest: bytes, results: dict[str, bytes]) -> MagicMock:
    client = MagicMock()
    client.get_object.side_effect = [
        {"Body": _body(manifest)},
        *({"Body": _body(contents)} for contents in results.values()),
    ]
    return client


class TestReportManifestExists:
    def test_preserves_trailing_prefix_separator_used_by_s3_batch_operations(self):
        """S3 appends its job segment even when the configured prefix ends in /.

        This matches the report placement observed from a real Batch Operations
        job: ``<submitted-prefix>//job-<id>/manifest.json``.
        """
        trailing_prefix = "completion-reports/cfg-1/manifest-1/"

        assert _manifest_key(trailing_prefix, _JOB_ID) == (
            "completion-reports/cfg-1/manifest-1//job-abc/manifest.json"
        )

    def test_returns_the_manifests_last_modified(self):
        written_at = datetime(2026, 8, 27, 9, 49, 46, tzinfo=UTC)
        client = MagicMock()
        client.head_object.return_value = {"LastModified": written_at}

        assert (
            report_manifest_written_at(client, _STATE_BUCKET, _PREFIX, _JOB_ID)
            == written_at
        )

        client.head_object.assert_called_once_with(Bucket=_STATE_BUCKET, Key=_MANIFEST_KEY)
        client.list_objects_v2.assert_not_called()

    def test_a_response_without_last_modified_yields_none(self):
        """An unusable timestamp must not read as a freshly written report.

        Returning something truthy here would make the unconsumed-report
        escalation measure elapsed time from nothing.
        """
        client = MagicMock()
        client.head_object.return_value = {}

        assert report_manifest_written_at(client, _STATE_BUCKET, _PREFIX, _JOB_ID) is None

    def test_sidecars_without_the_top_level_manifest_are_not_a_ready_report(self):
        """Sidecar result objects cannot suppress a missing-report alert."""
        client = MagicMock()
        client.head_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "HeadObject"
        )
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"}]
        }

        assert report_manifest_written_at(client, _STATE_BUCKET, _PREFIX, _JOB_ID) is None

        client.head_object.assert_called_once_with(Bucket=_STATE_BUCKET, Key=_MANIFEST_KEY)
        client.list_objects_v2.assert_not_called()


class TestReadBopsCompletionReport:
    def test_accepts_hex_md5_checksum_from_an_s3_completion_report(self):
        result = _row()
        result_key = f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"
        client = _client(_manifest([_s3_result(result_key, result)]), {result_key: result})

        report = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 1)

        assert [entry.object_key for entry in report.entries] == ["object.txt"]

    def test_reads_only_manifest_declared_results_and_returns_typed_report(self):
        first = _row(key="first.txt")
        second = _row(key="second.txt", status="failed", http_status="500", error_code="AccessDenied")
        first_key = f"{_PREFIX}/job-{_JOB_ID}/results/first.csv"
        second_key = f"{_PREFIX}/job-{_JOB_ID}/results/second.csv"
        client = _client(
            _manifest([_result(first_key, first), _result(second_key, second)]),
            {first_key: first, second_key: second},
        )

        report = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 2)

        assert report == BopsCompletionReport(
            created_at=datetime(2026, 8, 26, 12, 34, 56, tzinfo=UTC),
            entries=tuple(report.entries),
        )
        assert [entry.object_key for entry in report.entries] == ["first.txt", "second.txt"]
        assert report.entries[1].error_code == "AccessDenied"
        assert client.get_object.call_args_list == [
            call(Bucket=_STATE_BUCKET, Key=_MANIFEST_KEY),
            call(Bucket=_STATE_BUCKET, Key=first_key),
            call(Bucket=_STATE_BUCKET, Key=second_key),
        ]
        client.list_objects_v2.assert_not_called()

    def test_missing_top_level_manifest_is_not_ready(self):
        client = MagicMock()
        client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject"
        )

        with pytest.raises(CompletionReportNotReady):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 1)

        client.get_object.assert_called_once_with(Bucket=_STATE_BUCKET, Key=_MANIFEST_KEY)

    def test_missing_manifest_declared_result_is_not_ready(self):
        result_key = f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"
        result = _row()
        client = MagicMock()
        client.get_object.side_effect = [
            {"Body": _body(_manifest([_result(result_key, result)]))},
            ClientError({"Error": {"Code": "404", "Message": "missing"}}, "GetObject"),
        ]

        with pytest.raises(CompletionReportNotReady):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 1)

    def test_checksum_mismatch_is_malformed(self):
        result_key = f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"
        expected = _row()
        client = _client(_manifest([_result(result_key, expected)]), {result_key: _row(key="changed.txt")})

        with pytest.raises(CompletionReportMalformed, match="checksum"):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 1)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("Format", "unexpected"),
            ("ReportSchema", "Bucket, Key"),
            ("ReportCreationDate", "not-a-date"),
        ],
    )
    def test_invalid_manifest_schema_or_creation_date_is_malformed(self, field, value):
        result = _row()
        result_key = f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"
        payload = json.loads(_manifest([_result(result_key, result)]))
        payload[field] = value
        client = _client(json.dumps(payload).encode(), {result_key: result})

        with pytest.raises(CompletionReportMalformed):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 1)

    def test_row_count_mismatch_is_malformed(self):
        result_key = f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"
        result = _row()
        client = _client(_manifest([_result(result_key, result)]), {result_key: result})

        with pytest.raises(CompletionReportMalformed, match="expected 2"):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 2)

    @pytest.mark.parametrize(
        "contents",
        [
            b"source-bucket,object.txt,version-1,succeeded,200",
            b",object.txt,version-1,succeeded,200,,success",
            b"source-bucket,,version-1,succeeded,200,,success",
        ],
    )
    def test_malformed_result_row_is_rejected(self, contents):
        result_key = f"{_PREFIX}/job-{_JOB_ID}/results/part.csv"
        client = _client(_manifest([_result(result_key, contents)]), {result_key: contents})

        with pytest.raises(CompletionReportMalformed, match="malformed row"):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 1)

    def test_duplicate_identity_across_declared_results_is_rejected(self):
        first = _row()
        second = _row(status="failed", http_status="500", error_code="AccessDenied")
        first_key = f"{_PREFIX}/job-{_JOB_ID}/results/first.csv"
        second_key = f"{_PREFIX}/job-{_JOB_ID}/results/second.csv"
        client = _client(
            _manifest([_result(first_key, first), _result(second_key, second)]),
            {first_key: first, second_key: second},
        )

        with pytest.raises(CompletionReportMalformed, match="duplicate"):
            read_bops_completion_report(client, _STATE_BUCKET, _PREFIX, _JOB_ID, 2)
