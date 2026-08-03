"""In-Memory MD5 helper for the Inventory_Report manifest path.

Retained after the UNLOAD generation path was removed (see the
scale-threshold-and-drain-throughput spec, Requirement 1.4): the UNLOAD
data-file listing (``list_data_files``), streaming-MD5 hashing
(``stream_md5`` / ``hash_data_files``), and the Inline_Hash_Ceiling gate
(``check_ceiling`` / ``InlineHashCeilingExceeded``) were removed along with
``unload_generator.py``, since the In_Memory path is now the sole
manifest-generation path and never streams a data file from S3.

``DataFileWithMd5`` and ``compute_in_memory_md5`` remain: the In_Memory +
Inventory_Report path (``inventory_manifest_writer.write_in_memory_inventory_manifest``)
uses ``compute_in_memory_md5`` to hash its single synthetic data file from
bytes already held in memory rather than from an S3 read (Requirement 4.3).

Requirements: 4.3
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class DataFileWithMd5:
    """Data file metadata plus its computed plaintext MD5 hex digest.

    ``row_count`` is the number of CSV rows in the file (one row per line,
    no header, no trailing newline — the exact format
    ``manifest_generator.serialize`` writes). ``None`` only for a caller
    that has not supplied it.
    """

    key: str
    size: int
    md5_hex: str
    row_count: int | None = None


# ---------------------------------------------------------------------------
# In-memory MD5
# ---------------------------------------------------------------------------


def compute_in_memory_md5(data: bytes) -> str:
    """Compute the MD5 of in-memory bytes without an S3 round-trip.

    Used for the In_Memory_Generation path: the manifest CSV is already held
    in memory, so no extra S3 read is needed (Requirement 4.3).

    Parameters
    ----------
    data:
        Raw bytes to hash.

    Returns
    -------
    str
        Lowercase hex MD5 digest.

    Requirements: 4.3
    """
    return hashlib.md5(data, usedforsecurity=False).hexdigest()
