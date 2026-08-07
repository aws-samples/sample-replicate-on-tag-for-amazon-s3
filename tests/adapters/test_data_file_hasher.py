"""Tests for src/adapters/data_file_hasher.py.

After the UNLOAD generation path was removed (scale-threshold-and-
drain-throughput spec, Task 1.2), this module retains only the in-memory
MD5 helper used by the In_Memory + Inventory_Report manifest path.

Feature: large-scale-manifest-generation
Requirements: 4.3
"""
from __future__ import annotations

import hashlib

from src.adapters.data_file_hasher import DataFileWithMd5, compute_in_memory_md5


# ---------------------------------------------------------------------------
# Unit tests: compute_in_memory_md5
# ---------------------------------------------------------------------------


class TestComputeInMemoryMd5:
    def test_matches_hashlib(self):
        data = b"some csv content here"
        assert compute_in_memory_md5(data) == hashlib.md5(data, usedforsecurity=False).hexdigest()

    def test_empty_bytes(self):
        assert compute_in_memory_md5(b"") == hashlib.md5(b"", usedforsecurity=False).hexdigest()


# ---------------------------------------------------------------------------
# Unit tests: DataFileWithMd5
# ---------------------------------------------------------------------------


class TestDataFileWithMd5:
    def test_row_count_defaults_to_none(self):
        df = DataFileWithMd5(key="data/a.csv", size=10, md5_hex="abc123")
        assert df.row_count is None

    def test_row_count_can_be_set(self):
        df = DataFileWithMd5(key="data/a.csv", size=10, md5_hex="abc123", row_count=3)
        assert df.row_count == 3
