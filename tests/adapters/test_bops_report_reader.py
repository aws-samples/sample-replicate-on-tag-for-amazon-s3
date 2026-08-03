"""Unit tests for src/adapters/bops_report_reader.py — task 13.2.

Covers Requirements 2.1, 2.7, 8.1:
- List-then-read across one and several report CSV objects under a prefix.
- Rows with mixed TaskStatus are all included.
- Empty prefix returns [].
- VersionId column parsed (including empty -> None).
- report_object_exists existence-only check.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.adapters.bops_report_reader import (
    read_bops_completion_report,
    report_object_exists,
)
from src.core.models import ManifestEntry

_STATE_BUCKET = "state-bucket"
_PREFIX = "completion-reports/cfg-1/job-abc/"


def _mock_body(text: str) -> MagicMock:
    body = MagicMock()
    body.read.return_value = text.encode("utf-8")
    return body


def _csv_row(bucket: str, key: str, version_id: str, task_status: str = "SUCCEEDED") -> str:
    return f"{bucket},{key},{version_id},{task_status},,200,"


class TestReadBopsCompletionReportSingleObject:
    def test_single_object_single_row(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
            "IsTruncated": False,
        }
        client.get_object.return_value = {
            "Body": _mock_body(_csv_row("my-bucket", "key-a", "v1"))
        }
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert entries == [
            ManifestEntry(source_bucket="my-bucket", object_key="key-a", version_id="v1")
        ]

    def test_single_object_multiple_rows(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
            "IsTruncated": False,
        }
        csv_text = "\n".join(
            [
                _csv_row("my-bucket", "key-a", "v1"),
                _csv_row("my-bucket", "key-b", "v2", task_status="FAILED"),
            ]
        )
        client.get_object.return_value = {"Body": _mock_body(csv_text)}
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert len(entries) == 2
        assert entries[0].object_key == "key-a"
        assert entries[1].object_key == "key-b"

    def test_mixed_task_status_rows_all_included(self):
        """Every listed object version is included regardless of TaskStatus."""
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
            "IsTruncated": False,
        }
        csv_text = "\n".join(
            [
                _csv_row("my-bucket", "key-ok", "v1", task_status="SUCCEEDED"),
                _csv_row("my-bucket", "key-failed", "v2", task_status="FAILED"),
            ]
        )
        client.get_object.return_value = {"Body": _mock_body(csv_text)}
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        keys = {e.object_key for e in entries}
        assert keys == {"key-ok", "key-failed"}

    def test_empty_version_id_column_parsed_as_none(self):
        """An unversioned bucket's report row has an empty VersionId column."""
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
            "IsTruncated": False,
        }
        client.get_object.return_value = {
            "Body": _mock_body(_csv_row("my-bucket", "key-unversioned", ""))
        }
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert len(entries) == 1
        assert entries[0].version_id is None


class TestReportKeyPercentDecoding:
    """The report's Key column is percent-decoded back to the object key.

    The Solution's manifest percent-encodes object keys (``/`` preserved) so a
    comma or newline in a key cannot break the CSV row, and S3 Batch Operations
    echoes the manifest key into the completion report. The reader must decode
    it, or the report row never joins to the Tracked_Object it describes.

    AWS does not document the report's key encoding; it was verified against a
    real job — see the note at the decode site in
    ``bops_report_reader._parse_report_csv``. A key containing a literal percent
    sequence is the only shape that distinguishes an echoed manifest key from an
    original key, because every other key decodes to the same string either way,
    so ``test_a_literal_percent_sequence_is_the_discriminating_case`` is the
    load-bearing test here. These tests pin the reader as the inverse of
    ``ManifestEntry.to_csv_row``.
    """

    @staticmethod
    def _read_one(csv_text: str) -> ManifestEntry:
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
            "IsTruncated": False,
        }
        client.get_object.return_value = {"Body": _mock_body(csv_text)}
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert len(entries) == 1
        return entries[0]

    @pytest.mark.parametrize(
        "object_key",
        [
            "plain-key.txt",
            "prefix/nested/key.txt",
            "key with spaces.txt",
            "key,with,commas.txt",
            "key#with?reserved&chars.txt",
            "unicode/ключ/文字.txt",
            "key+with+plus.txt",
            "pct%20literal.txt",
        ],
    )
    def test_round_trips_the_solutions_own_manifest_encoding(self, object_key):
        """Encode with the real manifest encoder, then read it back.

        Driving the encoding through ``ManifestEntry.to_csv_row`` rather than
        writing the encoded form by hand means the test fails if either side
        of the pair changes independently.
        """
        manifest_row = ManifestEntry(
            source_bucket="my-bucket", object_key=object_key, version_id="v1"
        ).to_csv_row()
        _bucket, encoded_key, _version = manifest_row.split(",")

        entry = self._read_one(
            f"my-bucket,{encoded_key},v1,SUCCEEDED,,200,"
        )

        assert entry.object_key == object_key

    def test_a_literal_percent_sequence_is_the_discriminating_case(self):
        """The one key shape that proves the report echoes the manifest key.

        For a key with no literal percent, the encoded and original forms
        decode to the same string, so decoding is correct either way and the
        test proves nothing about which form the report carries. A key holding
        the text ``%20`` separates them: the manifest double-encodes it to
        ``%2520``, so decoding once yields the real key only if the report
        carried the manifest form. Verified against a real job — see the class
        docstring.
        """
        real_key = "retag-verify/pct%20literal.txt"
        manifest_row = ManifestEntry(
            source_bucket="my-bucket", object_key=real_key, version_id="v1"
        ).to_csv_row()
        assert manifest_row == "my-bucket,retag-verify/pct%2520literal.txt,v1"

        # The report's Key column, as observed from a real completion report.
        entry = self._read_one(
            "my-bucket,retag-verify/pct%2520literal.txt,v1,SUCCEEDED,,200,"
        )
        assert entry.object_key == real_key

    def test_prefix_slashes_are_not_encoded_so_decoding_leaves_them(self):
        """``to_csv_row`` uses ``safe="/"``, so a prefix survives untouched."""
        entry = self._read_one("my-bucket,a/b/c.txt,v1,SUCCEEDED,,200,")
        assert entry.object_key == "a/b/c.txt"

    def test_plus_is_not_treated_as_a_space(self):
        """``unquote``, not ``unquote_plus``: ``+`` is a literal in an S3 key.

        ``to_csv_row`` emits ``+`` unencoded, so decoding it to a space would
        corrupt every key containing a plus sign.
        """
        entry = self._read_one("my-bucket,a+b.txt,v1,SUCCEEDED,,200,")
        assert entry.object_key == "a+b.txt"

    def test_encoded_comma_keeps_the_column_count_intact(self):
        """A comma in a key is encoded, so it cannot be read as a delimiter."""
        manifest_row = ManifestEntry(
            source_bucket="my-bucket", object_key="a,b.txt", version_id="v1"
        ).to_csv_row()
        assert manifest_row == "my-bucket,a%2Cb.txt,v1"

        entry = self._read_one(f"{manifest_row},SUCCEEDED,,200,")
        assert entry.object_key == "a,b.txt"


class TestReadBopsCompletionReportMultipleObjects:
    def test_several_report_csv_objects_all_read(self):
        """A job may write more than one report CSV object under the prefix."""
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"{_PREFIX}job-abc/results/hash1.csv"},
                {"Key": f"{_PREFIX}job-abc/results/hash2.csv"},
            ],
            "IsTruncated": False,
        }
        client.get_object.side_effect = [
            {"Body": _mock_body(_csv_row("my-bucket", "key-a", "v1"))},
            {"Body": _mock_body(_csv_row("my-bucket", "key-b", "v2"))},
        ]
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        keys = {e.object_key for e in entries}
        assert keys == {"key-a", "key-b"}
        assert client.get_object.call_count == 2

    def test_paginated_listing_all_pages_read(self):
        """list_objects_v2 pagination (IsTruncated) is followed to completion."""
        client = MagicMock()
        client.list_objects_v2.side_effect = [
            {
                "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
                "IsTruncated": True,
                "NextContinuationToken": "token-1",
            },
            {
                "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash2.csv"}],
                "IsTruncated": False,
            },
        ]
        client.get_object.side_effect = [
            {"Body": _mock_body(_csv_row("my-bucket", "key-a", "v1"))},
            {"Body": _mock_body(_csv_row("my-bucket", "key-b", "v2"))},
        ]
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        keys = {e.object_key for e in entries}
        assert keys == {"key-a", "key-b"}
        assert client.list_objects_v2.call_count == 2
        second_call_kwargs = client.list_objects_v2.call_args_list[1][1]
        assert second_call_kwargs.get("ContinuationToken") == "token-1"


class TestReadBopsCompletionReportSkipsNonResultsSidecars:
    """S3 Batch Operations writes manifest.json and manifest.json.md5
    sidecar objects under the same Report.Prefix as the results/ CSVs.
    Only the results/*.csv objects contain per-object completion rows;
    the sidecars must be skipped rather than misparsed as CSV data.
    """

    def test_manifest_json_sidecar_is_not_parsed_as_csv_row(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"{_PREFIX}job-abc/manifest.json"},
                {"Key": f"{_PREFIX}job-abc/manifest.json.md5"},
                {"Key": f"{_PREFIX}job-abc/results/hash1.csv"},
            ],
            "IsTruncated": False,
        }
        client.get_object.return_value = {
            "Body": _mock_body(_csv_row("my-bucket", "key-a", "v1"))
        }
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert entries == [
            ManifestEntry(source_bucket="my-bucket", object_key="key-a", version_id="v1")
        ]
        # Only the results/ CSV is ever fetched — sidecars are skipped
        # before any get_object call.
        assert client.get_object.call_count == 1
        client.get_object.assert_called_once_with(
            Bucket=_STATE_BUCKET, Key=f"{_PREFIX}job-abc/results/hash1.csv"
        )

    def test_only_sidecars_present_returns_empty_list(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"{_PREFIX}job-abc/manifest.json"},
                {"Key": f"{_PREFIX}job-abc/manifest.json.md5"},
            ],
            "IsTruncated": False,
        }
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert entries == []
        client.get_object.assert_not_called()


class TestReadBopsCompletionReportEmptyPrefix:
    def test_empty_prefix_returns_empty_list(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert entries == []
        client.get_object.assert_not_called()

    def test_no_contents_key_returns_empty_list(self):
        """Contents key entirely absent (rather than an empty list) is tolerated."""
        client = MagicMock()
        client.list_objects_v2.return_value = {"IsTruncated": False}
        entries = read_bops_completion_report(client, _STATE_BUCKET, _PREFIX)
        assert entries == []


# ---------------------------------------------------------------------------
# report_object_exists — task 23.4 (Requirements 8.1)
# ---------------------------------------------------------------------------


class TestReportObjectExists:
    def test_returns_true_when_object_present_under_prefix(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
        }
        assert report_object_exists(client, _STATE_BUCKET, _PREFIX) is True

    def test_returns_false_when_prefix_empty(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {}
        assert report_object_exists(client, _STATE_BUCKET, _PREFIX) is False

    def test_returns_false_when_contents_key_absent(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"IsTruncated": False}
        assert report_object_exists(client, _STATE_BUCKET, _PREFIX) is False

    def test_returns_false_when_contents_is_empty_list(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"Contents": []}
        assert report_object_exists(client, _STATE_BUCKET, _PREFIX) is False

    def test_does_not_call_get_object(self):
        """Existence-only — never reads or parses any CSV content."""
        client = MagicMock()
        client.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{_PREFIX}job-abc/results/hash1.csv"}],
        }
        report_object_exists(client, _STATE_BUCKET, _PREFIX)
        client.get_object.assert_not_called()

    def test_calls_list_objects_v2_with_max_keys_one(self):
        client = MagicMock()
        client.list_objects_v2.return_value = {"Contents": []}
        report_object_exists(client, _STATE_BUCKET, _PREFIX)
        kwargs = client.list_objects_v2.call_args[1]
        assert kwargs.get("Bucket") == _STATE_BUCKET
        assert kwargs.get("Prefix") == _PREFIX
        assert kwargs.get("MaxKeys") == 1
