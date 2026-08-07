"""Inventory Manifest Writer — assembles and writes the S3 Inventory report manifest.

Writes the ``manifest.json`` envelope and ``manifest.checksum`` file that S3 Batch
Operations requires for the ``S3InventoryReport_CSV_20161130`` format.

Write order: ``manifest.checksum`` BEFORE ``manifest.json`` so a partial failure
never leaves a manifest.json without its checksum (Requirement 6.3).

The field schema is always ``"Bucket, Key, VersionId"`` (Requirement 6.4).

SSE-KMS is applied to both files when ``kms_key_arn`` is set; SSE-S3 otherwise
(Requirements 7.1, 7.2).

Requirements: 4.3, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, UTC

from src.adapters.data_file_hasher import DataFileWithMd5, compute_in_memory_md5
from src.core.models import S3Location

logger = logging.getLogger(__name__)

_COMPONENT = "Inventory_Manifest_Writer"

# S3 Inventory report manifest format string (Req 6.1)
_INVENTORY_FORMAT_VERSION = "2016-11-30"
_FIELD_SCHEMA = "Bucket, Key, VersionId"  # always versioned (Req 6.4)



# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class WrittenManifest:
    """The outcome of a successful manifest write.

    ``s3_location`` and ``etag`` are the values S3 Batch Operations requires
    when the manifest is used as job input (Req 6.4).

    ``all_versioned`` is passed through to the batch adapter so it can select
    the correct manifest fields (Bucket, Key, VersionId vs Bucket, Key).
    """

    s3_location: S3Location
    etag: str
    object_count: int
    all_versioned: bool = False


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------


class InventoryManifestWriteError(Exception):
    """Raised when writing the manifest.json or manifest.checksum fails.

    Requirements: 6.6
    """

    def __init__(self, config_id: str, reason: str) -> None:
        self.config_id = config_id
        self.reason = reason
        super().__init__(
            f"Inventory manifest write failed for config '{config_id}': {reason}"
        )


# ---------------------------------------------------------------------------
# Envelope builder (pure)
# ---------------------------------------------------------------------------


def build_manifest_json(
    source_bucket: str,
    scratch_bucket: str,
    data_files: list[DataFileWithMd5],
    timestamp: datetime | None = None,
) -> bytes:
    """Build the manifest.json envelope bytes (pure, no I/O).

    Includes all fields required by S3 Batch Operations for the
    ``S3InventoryReport_CSV_20161130`` format: ``sourceBucket``,
    ``destinationBucket`` (ARN of the scratch bucket where data files live),
    ``version``, ``creationTimestamp``, ``fileFormat``, ``fileSchema``,
    and ``files``.

    Parameters
    ----------
    source_bucket:
        The source S3 bucket the manifest covers.
    scratch_bucket:
        The S3 bucket where the data files and manifest are stored (used as
        the ``destinationBucket`` ARN required by S3 Batch Operations).
    data_files:
        Data files with their sizes and MD5 hex digests.
    timestamp:
        Creation time for the ``creationTimestamp`` field (Unix ms).
        Defaults to current UTC.

    Returns
    -------
    bytes
        UTF-8-encoded JSON envelope.

    Requirements: 6.1, 6.4
    """
    if timestamp is None:
        timestamp = datetime.now(UTC)
    creation_ts_ms = int(timestamp.timestamp() * 1000)

    doc = {
        "sourceBucket": source_bucket,
        "destinationBucket": f"arn:aws:s3:::{scratch_bucket}",
        "version": _INVENTORY_FORMAT_VERSION,
        "creationTimestamp": str(creation_ts_ms),
        "fileFormat": "CSV",
        "fileSchema": _FIELD_SCHEMA,
        "files": [
            {
                "key": df.key,
                "size": df.size,
                "MD5checksum": df.md5_hex,
            }
            for df in data_files
        ],
    }
    return json.dumps(doc, indent=2, ensure_ascii=False).encode("utf-8")


def build_manifest_checksum(manifest_json_bytes: bytes) -> str:
    """Compute the hex MD5 of the manifest.json bytes (Req 6.2).

    Parameters
    ----------
    manifest_json_bytes:
        The exact bytes that will be (or were) written as manifest.json.

    Returns
    -------
    str
        Lowercase hex MD5 digest.
    """
    return hashlib.md5(manifest_json_bytes, usedforsecurity=False).hexdigest()


# ---------------------------------------------------------------------------
# Public writer
# ---------------------------------------------------------------------------


def _put_object(
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
    kms_key_arn: str | None,
) -> str:
    """Upload an object and return its ETag (stripped of quotes).

    Applies SSE-KMS when ``kms_key_arn`` is set, else SSE-S3 (Req 7.1, 7.2).

    Raises
    ------
    Exception
        Any exception from boto3 ``put_object``.
    """
    kwargs: dict = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "ContentType": content_type,
    }
    if kms_key_arn:
        kwargs["ServerSideEncryption"] = "aws:kms"
        kwargs["SSEKMSKeyId"] = kms_key_arn
    else:
        kwargs["ServerSideEncryption"] = "AES256"

    resp = s3_client.put_object(**kwargs)
    raw_etag = resp.get("ETag", "")
    return raw_etag.strip('"')


def write_inventory_manifest(
    s3_client,
    scratch_bucket: str,
    dest_prefix: str,
    source_bucket: str,
    config_id: str,
    data_files: list[DataFileWithMd5],
    kms_key_arn: str | None = None,
    timestamp: datetime | None = None,
) -> WrittenManifest:
    """Assemble and write the Inventory_Report_Manifest.

    Writes ``manifest.checksum`` BEFORE ``manifest.json`` (Req 6.3).
    Both objects are written with SSE-KMS when ``kms_key_arn`` is set,
    else SSE-S3 (Req 7.1, 7.2).

    Parameters
    ----------
    s3_client:
        boto3 S3 client.
    scratch_bucket:
        State/scratch bucket where the manifest files will be written.
    dest_prefix:
        S3 key prefix under which to write the manifest files
        (e.g. ``manifests/<config_id>/<ts>/``). Must end with ``/``.
    source_bucket:
        Name of the source S3 bucket.
    config_id:
        Replication configuration identifier (for error messages).
    data_files:
        Data files with their keys, sizes, and MD5 hex digests.
    kms_key_arn:
        When set, both objects are written with SSE-KMS (Req 7.1).
    timestamp:
        UTC datetime for key generation; defaults to current UTC.

    Returns
    -------
    WrittenManifest
        Location + ETag of manifest.json and the total object count across
        all data files (derived from data_files list length since UNLOAD
        emits one row per object).

    Raises
    ------
    InventoryManifestWriteError
        If either write fails (Req 6.6).

    Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2
    """
    if timestamp is None:
        timestamp = datetime.now(UTC)

    ts_str = timestamp.strftime("%Y%m%dT%H%M%SZ")
    checksum_key = f"{dest_prefix}{ts_str}_manifest.checksum"
    manifest_key = f"{dest_prefix}{ts_str}_manifest.json"

    # Build the manifest.json bytes (pure)
    manifest_bytes = build_manifest_json(source_bucket, scratch_bucket, data_files, timestamp)
    checksum_hex = build_manifest_checksum(manifest_bytes)

    # Step 1: write manifest.checksum FIRST (Req 6.3)
    try:
        _put_object(
            s3_client,
            scratch_bucket,
            checksum_key,
            checksum_hex.encode("utf-8"),
            "text/plain",
            kms_key_arn,
        )
    except Exception as exc:
        raise InventoryManifestWriteError(
            config_id=config_id,
            reason=f"Failed to write manifest.checksum: {exc}",
        ) from exc

    # Step 2: write manifest.json (Req 6.1, 6.5)
    try:
        etag = _put_object(
            s3_client,
            scratch_bucket,
            manifest_key,
            manifest_bytes,
            "application/json",
            kms_key_arn,
        )
    except Exception as exc:
        raise InventoryManifestWriteError(
            config_id=config_id,
            reason=f"Failed to write manifest.json: {exc}",
        ) from exc

    if not etag:
        raise InventoryManifestWriteError(
            config_id=config_id,
            reason="S3 PutObject for manifest.json returned no ETag",
        )

    # Object count = total CSV rows across every data file (one row per
    # object, per manifest_generator's row format), not the number of data
    # files — a single UNLOAD data file holds many rows. `row_count` is
    # populated by `data_file_hasher.hash_data_files` in the same streaming
    # pass used to compute each file's MD5 (code-review-remediation spec
    # Req 7); `write_in_memory_inventory_manifest`'s single synthetic
    # DataFileWithMd5 always has `row_count` set to the true count of the
    # in-memory CSV. A `None` row_count is only possible for a caller that
    # constructed DataFileWithMd5 directly without a row count — in that
    # case, fall back to counting the file itself as one row rather than
    # raising, since exactness here is best-effort (this function has no way
    # to independently verify the file's actual content without re-reading
    # it).
    object_count = sum(
        df.row_count if df.row_count is not None else 1 for df in data_files
    )

    logger.debug(
        "%s | Inventory manifest written: bucket=%s prefix=%s etag=%s files=%d",
        _COMPONENT,
        scratch_bucket,
        dest_prefix,
        etag,
        len(data_files),
    )

    return WrittenManifest(
        s3_location=S3Location(bucket=scratch_bucket, key=manifest_key),
        etag=etag,
        object_count=object_count,
        all_versioned=True,  # Inventory manifests always carry VersionId (Req 6.4)
    )




def write_in_memory_inventory_manifest(
    s3_client,
    scratch_bucket: str,
    config_id: str,
    source_bucket: str,
    csv_bytes: bytes,
    data_file_key: str,
    kms_key_arn: str | None = None,
    timestamp: datetime | None = None,
) -> WrittenManifest:
    """Write an in-memory CSV as a single-file Inventory_Report_Manifest.

    Used by the In_Memory + Inventory_Report path (small volume, KMS key
    configured).  Avoids an extra S3 round-trip by computing the MD5 from
    the in-memory bytes (Requirement 4.3) rather than streaming from S3.

    Steps:
      1. Write the single data file (``csv_bytes``) with SSE-KMS/SSE-S3.
      2. Compute the MD5 from the in-memory bytes (Req 4.3).
      3. Delegate to :func:`write_inventory_manifest` for checksum + envelope.

    Parameters
    ----------
    s3_client:
        boto3 S3 client.
    scratch_bucket:
        State/scratch bucket.
    config_id:
        Replication configuration identifier (for error messages).
    source_bucket:
        Name of the source S3 bucket.
    csv_bytes:
        Serialized manifest CSV bytes already in memory.
    data_file_key:
        S3 key where the data file should be written.
    kms_key_arn:
        When set, uses SSE-KMS; else SSE-S3 (Req 7.1, 7.2).
    timestamp:
        UTC datetime used for manifest.json key naming; defaults to current UTC.

    Returns
    -------
    WrittenManifest
        With ``object_count`` set to the number of entries in ``csv_bytes``
        (caller must override if a more precise count is available).

    Raises
    ------
    InventoryManifestWriteError
        On any write failure (Req 6.6).

    Requirements: 4.3, 6.1, 6.2, 6.3, 6.4, 7.1, 7.2
    """
    # Step 1: write the single data file
    try:
        _put_object(
            s3_client,
            scratch_bucket,
            data_file_key,
            csv_bytes,
            "text/csv",
            kms_key_arn,
        )
    except Exception as exc:
        raise InventoryManifestWriteError(
            config_id=config_id,
            reason=f"Failed to write in-memory data file: {exc}",
        ) from exc

    # Step 2: MD5 from in-memory bytes — no extra S3 read (Req 4.3)
    md5_hex = compute_in_memory_md5(csv_bytes)

    # Row count from the in-memory bytes directly (same format as
    # data_file_hasher's streaming row count: one row per line, no header,
    # no trailing newline — see manifest_generator.serialize).
    row_count = csv_bytes.count(b"\n") + 1 if csv_bytes else 0

    single_data_file = DataFileWithMd5(
        key=data_file_key,
        size=len(csv_bytes),
        md5_hex=md5_hex,
        row_count=row_count,
    )

    # Step 3: write manifest.checksum and manifest.json
    # derive the dest_prefix from data_file_key (strip the filename)
    dest_prefix = data_file_key.rsplit("/data/", 1)[0] + "/"

    written = write_inventory_manifest(
        s3_client=s3_client,
        scratch_bucket=scratch_bucket,
        dest_prefix=dest_prefix,
        source_bucket=source_bucket,
        config_id=config_id,
        data_files=[single_data_file],
        kms_key_arn=kms_key_arn,
        timestamp=timestamp,
    )
    return written
