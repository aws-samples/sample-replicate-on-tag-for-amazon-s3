"""Tests for src/adapters/inventory_manifest_writer.py — Task 6.4.

Property 4: Inventory manifest round-trip and integrity.

Feature: large-scale-manifest-generation
Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.adapters.data_file_hasher import DataFileWithMd5
from src.adapters.inventory_manifest_writer import (
    InventoryManifestWriteError,
    _FIELD_SCHEMA,
    build_manifest_checksum,
    build_manifest_json,
    write_in_memory_inventory_manifest,
    write_inventory_manifest,
)
from src.core.manifest_generator import deserialize as deserialize_csv_manifest
from src.core.manifest_generator import serialize as serialize_csv_manifest
from src.core.models import ManifestEntry


# ---------------------------------------------------------------------------
# Unit tests: build_manifest_json
# ---------------------------------------------------------------------------


class TestBuildManifestJson:
    def test_structure(self):
        """manifest.json has required fields (Req 6.1)."""
        files = [DataFileWithMd5(key="data/a.csv", size=1024, md5_hex="abc123")]
        doc_bytes = build_manifest_json("my-bucket", "scratch-bucket", files)
        doc = json.loads(doc_bytes.decode("utf-8"))
        assert doc["sourceBucket"] == "my-bucket"
        assert doc["destinationBucket"] == "arn:aws:s3:::scratch-bucket"
        assert doc["version"] == "2016-11-30"
        assert doc["fileFormat"] == "CSV"
        assert doc["fileSchema"] == "Bucket, Key, VersionId"
        assert "creationTimestamp" in doc
        assert len(doc["files"]) == 1
        assert doc["files"][0]["key"] == "data/a.csv"
        assert doc["files"][0]["size"] == 1024
        assert doc["files"][0]["MD5checksum"] == "abc123"

    def test_field_schema_always_versioned(self):
        """fileSchema is always the versioned form (Req 6.4)."""
        doc_bytes = build_manifest_json("b", "s", [])
        doc = json.loads(doc_bytes.decode("utf-8"))
        assert "VersionId" in doc["fileSchema"]

    def test_empty_files_list(self):
        doc_bytes = build_manifest_json("b", "s", [])
        doc = json.loads(doc_bytes.decode("utf-8"))
        assert doc["files"] == []


# ---------------------------------------------------------------------------
# Unit tests: build_manifest_checksum
# ---------------------------------------------------------------------------


class TestBuildManifestChecksum:
    def test_equals_md5_of_bytes(self):
        data = b"some manifest json"
        expected = hashlib.md5(data, usedforsecurity=False).hexdigest()
        assert build_manifest_checksum(data) == expected


# ---------------------------------------------------------------------------
# Unit tests: write_inventory_manifest
# ---------------------------------------------------------------------------


def _make_mock_s3(etag: str = "abc123") -> MagicMock:
    mock = MagicMock()
    mock.put_object.return_value = {"ETag": f'"{etag}"'}
    return mock


class TestWriteInventoryManifest:
    _FILES = [DataFileWithMd5(key="manifests/cfg/ts/data/data.csv", size=500, md5_hex="deadbeef")]
    _TS = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_object_count_sums_row_counts_not_file_count(self):
        """object_count must be the sum of each file's row_count (the true
        object count), not the number of data files — code-review-
        remediation spec Req 7. A single data file can hold many rows."""
        mock_s3 = _make_mock_s3()
        files = [
            DataFileWithMd5(key="data/a.csv", size=100, md5_hex="aaa", row_count=7),
            DataFileWithMd5(key="data/b.csv", size=200, md5_hex="bbb", row_count=3),
        ]
        written = write_inventory_manifest(
            mock_s3, "scratch", "manifests/cfg/ts/",
            "src-bucket", "cfg", files, timestamp=self._TS,
        )
        assert written.object_count == 10
        assert written.object_count != len(files)

    def test_object_count_falls_back_to_one_when_row_count_missing(self):
        """A DataFileWithMd5 constructed without row_count (None) counts as
        exactly one object for that file, rather than raising."""
        mock_s3 = _make_mock_s3()
        files = [DataFileWithMd5(key="data/a.csv", size=100, md5_hex="aaa")]
        written = write_inventory_manifest(
            mock_s3, "scratch", "manifests/cfg/ts/",
            "src-bucket", "cfg", files, timestamp=self._TS,
        )
        assert written.object_count == 1

    def test_writes_checksum_before_manifest(self):
        """manifest.checksum written before manifest.json (Req 6.3)."""
        mock_s3 = _make_mock_s3()
        write_inventory_manifest(
            mock_s3, "scratch", "manifests/cfg/ts/",
            "src-bucket", "cfg", self._FILES, timestamp=self._TS,
        )
        calls = [c.kwargs["Key"] for c in mock_s3.put_object.call_args_list]
        # checksum must come first
        checksum_idx = next(i for i, k in enumerate(calls) if "checksum" in k)
        manifest_idx = next(i for i, k in enumerate(calls) if "manifest.json" in k)
        assert checksum_idx < manifest_idx

    def test_sse_s3_when_no_kms(self):
        """No KMS → ServerSideEncryption='AES256' (Req 7.2)."""
        mock_s3 = _make_mock_s3()
        write_inventory_manifest(
            mock_s3, "scratch", "manifests/cfg/ts/",
            "src-bucket", "cfg", self._FILES, kms_key_arn=None, timestamp=self._TS,
        )
        for c in mock_s3.put_object.call_args_list:
            assert c.kwargs.get("ServerSideEncryption") == "AES256"

    def test_sse_kms_when_key_configured(self):
        """KMS key → SSE-KMS on both objects (Req 7.1)."""
        mock_s3 = _make_mock_s3()
        write_inventory_manifest(
            mock_s3, "scratch", "manifests/cfg/ts/",
            "src-bucket", "cfg", self._FILES,
            kms_key_arn="arn:aws:kms:us-east-1:123:key/k1",
            timestamp=self._TS,
        )
        for c in mock_s3.put_object.call_args_list:
            assert c.kwargs.get("ServerSideEncryption") == "aws:kms"
            assert "SSEKMSKeyId" in c.kwargs

    def test_returns_written_manifest_with_etag(self):
        mock_s3 = _make_mock_s3("deadbeef")
        result = write_inventory_manifest(
            mock_s3, "scratch", "manifests/cfg/ts/",
            "src-bucket", "cfg", self._FILES, timestamp=self._TS,
        )
        assert result.etag == "deadbeef"
        assert result.s3_location.bucket == "scratch"
        assert result.all_versioned is True

    def test_raises_on_checksum_write_failure(self):
        mock_s3 = MagicMock()
        mock_s3.put_object.side_effect = RuntimeError("S3 error")
        with pytest.raises(InventoryManifestWriteError, match="manifest.checksum"):
            write_inventory_manifest(
                mock_s3, "scratch", "manifests/cfg/ts/",
                "src-bucket", "cfg", self._FILES, timestamp=self._TS,
            )

    def test_raises_on_manifest_json_write_failure(self):
        mock_s3 = MagicMock()
        call_count = [0]

        def side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 2:  # second call is manifest.json
                raise RuntimeError("S3 error on manifest.json")
            return {"ETag": '"etag"'}

        mock_s3.put_object.side_effect = side_effect
        with pytest.raises(InventoryManifestWriteError, match="manifest.json"):
            write_inventory_manifest(
                mock_s3, "scratch", "manifests/cfg/ts/",
                "src-bucket", "cfg", self._FILES, timestamp=self._TS,
            )


# ---------------------------------------------------------------------------
# Property 4: Inventory manifest round-trip and integrity
# Feature: large-scale-manifest-generation, Property 4: Inventory manifest round-trip and integrity
# Requirements: 6.1, 6.2, 6.4
# ---------------------------------------------------------------------------

_md5_hex_st = st.text(
    alphabet="0123456789abcdef", min_size=32, max_size=32
)


@given(
    file_count=st.integers(min_value=0, max_value=20),
    source_bucket=st.text(min_size=1, max_size=30, alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="-"
    )),
)
@settings(max_examples=100)
def test_property_4_inventory_manifest_round_trip(
    file_count: int,
    source_bucket: str,
) -> None:
    """Property 4: written manifest.json lists exactly the given files; checksum = MD5.

    # Feature: large-scale-manifest-generation, Property 4: Inventory manifest round-trip and integrity
    """
    files = [
        DataFileWithMd5(key=f"prefix/file{i:04d}.csv.gz", size=i + 1, md5_hex=f"{i:032x}")
        for i in range(file_count)
    ]

    manifest_bytes = build_manifest_json(source_bucket, "scratch-bucket", files)
    checksum = build_manifest_checksum(manifest_bytes)

    doc = json.loads(manifest_bytes.decode("utf-8"))

    # Source bucket preserved
    assert doc["sourceBucket"] == source_bucket
    # destinationBucket and creationTimestamp present
    assert "destinationBucket" in doc
    assert "creationTimestamp" in doc

    # Exactly the given files
    assert len(doc["files"]) == file_count
    for i, entry in enumerate(doc["files"]):
        assert entry["key"] == files[i].key
        assert entry["size"] == files[i].size
        assert entry["MD5checksum"] == files[i].md5_hex

    # fileSchema always versioned (Req 6.4)
    assert "VersionId" in doc["fileSchema"]

    # checksum is MD5 of the manifest bytes (Req 6.2)
    expected_checksum = hashlib.md5(manifest_bytes, usedforsecurity=False).hexdigest()
    assert checksum == expected_checksum


# ---------------------------------------------------------------------------
# Writer output verification — retained from the former read_manifest_entries
# round-trip tests.  These verify that write_inventory_manifest and
# write_in_memory_inventory_manifest produce parseable output whose content
# matches the entries that were written.  (Req 2.7)
# ---------------------------------------------------------------------------


class _FakeBody:
    """Minimal stand-in for a boto3 StreamingBody."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    """In-memory fake S3 client supporting ``put_object``/``get_object``.

    Real enough for round-tripping objects written by
    ``write_inventory_manifest``/``write_in_memory_inventory_manifest`` and
    verified by reading them back directly — no mocking of the write path,
    so the test exercises the actual dispatch and serialization logic.
    """

    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        body = kwargs["Body"]
        if isinstance(body, str):
            body = body.encode("utf-8")
        self._objects[(bucket, key)] = body
        return {"ETag": '"fakeetag"'}

    def get_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        data = self._objects[(bucket, key)]
        return {"Body": _FakeBody(data)}

    def get_bytes(self, bucket: str, key: str) -> bytes:
        return self._objects[(bucket, key)]


class TestWriteInMemoryInventoryManifestOutput:
    """Verify write_in_memory_inventory_manifest writes data that deserializes
    back to the original entries."""

    def test_written_data_file_deserializes_to_original_entries(self):
        entries = [
            ManifestEntry(source_bucket="src-bucket", object_key="x.txt", version_id="v1"),
            ManifestEntry(source_bucket="src-bucket", object_key="y.txt", version_id="v2"),
        ]
        csv_bytes = serialize_csv_manifest(entries).encode("utf-8")
        fake_s3 = _FakeS3()
        data_file_key = "manifests/cfg-1/ts/data/data.csv"
        write_in_memory_inventory_manifest(
            fake_s3,
            scratch_bucket="state-bucket",
            config_id="cfg-1",
            source_bucket="src-bucket",
            csv_bytes=csv_bytes,
            data_file_key=data_file_key,
            timestamp=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        )

        # Read the data file back and parse it
        written_csv = fake_s3.get_bytes("state-bucket", data_file_key).decode("utf-8")
        result = deserialize_csv_manifest(written_csv)
        assert result == entries

    def test_object_count_reflects_row_count_not_file_count(self):
        """write_in_memory_inventory_manifest's single synthetic data file
        must report object_count equal to the number of CSV rows, not 1
        (the file count) — code-review-remediation spec Req 7."""
        entries = [
            ManifestEntry(source_bucket="src-bucket", object_key=f"k{i}.txt", version_id=f"v{i}")
            for i in range(5)
        ]
        csv_bytes = serialize_csv_manifest(entries).encode("utf-8")
        fake_s3 = _FakeS3()
        written = write_in_memory_inventory_manifest(
            fake_s3,
            scratch_bucket="state-bucket",
            config_id="cfg-1",
            source_bucket="src-bucket",
            csv_bytes=csv_bytes,
            data_file_key="manifests/cfg-1/ts/data/data.csv",
            timestamp=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
        )
        assert written.object_count == 5


class TestWriteInventoryManifestMultiFileOutput:
    """Verify write_inventory_manifest with multiple data files produces a
    manifest.json whose file list references data that deserializes to the
    original entries."""

    def test_multi_file_manifest_json_references_all_data_files(self):
        entries_a = [ManifestEntry(source_bucket="src-bucket", object_key="a.txt", version_id="v1")]
        entries_b = [ManifestEntry(source_bucket="src-bucket", object_key="b.txt", version_id="v2")]
        csv_a = serialize_csv_manifest(entries_a).encode("utf-8")
        csv_b = serialize_csv_manifest(entries_b).encode("utf-8")

        fake_s3 = _FakeS3()
        ts = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        dest_prefix = "manifests/cfg-1/ts/"
        key_a = f"{dest_prefix}data/part-0.csv"
        key_b = f"{dest_prefix}data/part-1.csv"
        fake_s3.put_object(Bucket="state-bucket", Key=key_a, Body=csv_a)
        fake_s3.put_object(Bucket="state-bucket", Key=key_b, Body=csv_b)

        data_files = [
            DataFileWithMd5(key=key_a, size=len(csv_a), md5_hex=hashlib.md5(csv_a, usedforsecurity=False).hexdigest()),
            DataFileWithMd5(key=key_b, size=len(csv_b), md5_hex=hashlib.md5(csv_b, usedforsecurity=False).hexdigest()),
        ]
        written = write_inventory_manifest(
            fake_s3,
            scratch_bucket="state-bucket",
            dest_prefix=dest_prefix,
            source_bucket="src-bucket",
            config_id="cfg-1",
            data_files=data_files,
            timestamp=ts,
        )

        # Read back the manifest.json and verify it references the data files
        manifest_json_bytes = fake_s3.get_bytes("state-bucket", written.s3_location.key)
        envelope = json.loads(manifest_json_bytes.decode("utf-8"))
        assert len(envelope["files"]) == 2
        assert envelope["files"][0]["key"] == key_a
        assert envelope["files"][1]["key"] == key_b

        # Verify data file contents deserialize correctly
        all_entries: list[ManifestEntry] = []
        for file_ref in envelope["files"]:
            data_csv = fake_s3.get_bytes("state-bucket", file_ref["key"]).decode("utf-8")
            all_entries.extend(deserialize_csv_manifest(data_csv))
        assert all_entries == entries_a + entries_b
