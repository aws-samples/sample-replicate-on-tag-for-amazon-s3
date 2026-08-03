"""Tests for src/core/row_cap_validation.py.

Covers ``IN_MEMORY_MEMORY_CEILING`` and ``validate_row_cap`` — the
config-load memory-safety check that makes ``Journal_Read_Row_Cap`` the
single scale-and-memory knob (see
``.kiro/specs/scale-threshold-and-drain-throughput/design.md``,
"Row-cap memory validation (core, pure)").

Feature: scale-threshold-and-drain-throughput
Requirements: 3.1, 3.2, 3.3
"""
from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.config_loader import ConfigError
from src.core.row_cap_validation import (
    IN_MEMORY_MEMORY_CEILING,
    validate_row_cap,
)

_SUPPORTED_MEMORY_SIZES = [1024, 2048, 3072, 4096, 6144, 8192, 10240]


# ---------------------------------------------------------------------------
# IN_MEMORY_MEMORY_CEILING table shape
# ---------------------------------------------------------------------------


class TestInMemoryMemoryCeilingTable:
    def test_covers_every_supported_lambda_memory_mb(self):
        """All CloudFormation-allowed LambdaMemoryMB values have a ceiling
        (deploy/template.yaml LambdaMemoryMB AllowedValues)."""
        assert set(IN_MEMORY_MEMORY_CEILING) == set(_SUPPORTED_MEMORY_SIZES)

    def test_ceilings_are_positive_ints(self):
        for mb, ceiling in IN_MEMORY_MEMORY_CEILING.items():
            assert isinstance(ceiling, int)
            assert ceiling > 0

    def test_ceilings_are_monotonically_nondecreasing_with_memory(self):
        """More memory never yields a smaller safe-row ceiling."""
        sizes = sorted(IN_MEMORY_MEMORY_CEILING)
        ceilings = [IN_MEMORY_MEMORY_CEILING[mb] for mb in sizes]
        assert ceilings == sorted(ceilings)

    def test_default_journal_read_row_cap_fits_default_lambda_memory(self):
        """The shipped JOURNAL_READ_ROW_CAP_DEFAULT (500,000) must not
        itself violate the ceiling at the CloudFormation LambdaMemoryMB
        default (2048), or the Solution would fail fast on first deploy."""
        from src.core.manifest_strategy import JOURNAL_READ_ROW_CAP_DEFAULT

        validate_row_cap(JOURNAL_READ_ROW_CAP_DEFAULT, 2048)  # must not raise

    def test_ceiling_values_match_calibrated_table(self):
        """Pin the corrected table (benchmarks/real_manifest_benchmark.py:
        ~1.9 KiB/object real-path peak, 250,000 safe rows per 1,024 MiB with
        tie-overshoot headroom). Guards against silent recalibration drift."""
        assert IN_MEMORY_MEMORY_CEILING == {
            1024: 250_000,
            2048: 500_000,
            3072: 750_000,
            4096: 1_000_000,
            6144: 1_500_000,
            8192: 2_000_000,
            10240: 2_500_000,
        }

    def test_default_lambda_memory_ceiling_covers_the_row_cap_default(self):
        """The shipped JOURNAL_READ_ROW_CAP_DEFAULT (500,000) must fit within
        the ceiling for the CloudFormation default LambdaMemoryMB (2,048),
        measured at ≈928 MiB peak RSS (real-path benchmark, with deduped_ops
        resident) — comfortably within 2,048 MiB. The default sits exactly at
        the 2,048 ceiling, which validate_row_cap accepts (strict >)."""
        assert IN_MEMORY_MEMORY_CEILING[2048] >= 500_000


# ---------------------------------------------------------------------------
# Boundary / example tests
# ---------------------------------------------------------------------------


class TestValidateRowCapBoundaries:
    @pytest.mark.parametrize("memory_mb", _SUPPORTED_MEMORY_SIZES)
    def test_exact_ceiling_accepted(self, memory_mb):
        ceiling = IN_MEMORY_MEMORY_CEILING[memory_mb]
        validate_row_cap(ceiling, memory_mb)  # must not raise

    @pytest.mark.parametrize("memory_mb", _SUPPORTED_MEMORY_SIZES)
    def test_ceiling_plus_one_rejected(self, memory_mb):
        ceiling = IN_MEMORY_MEMORY_CEILING[memory_mb]
        with pytest.raises(ConfigError):
            validate_row_cap(ceiling + 1, memory_mb)

    @pytest.mark.parametrize("memory_mb", _SUPPORTED_MEMORY_SIZES)
    def test_one_row_always_accepted(self, memory_mb):
        validate_row_cap(1, memory_mb)  # must not raise

    def test_error_message_names_value_ceiling_and_memory_size(self):
        """Requirement 3.2: the error names the offending value, the
        ceiling, and the configured memory size."""
        ceiling = IN_MEMORY_MEMORY_CEILING[1024]
        row_cap = ceiling + 12345
        with pytest.raises(ConfigError) as exc_info:
            validate_row_cap(row_cap, 1024)
        message = str(exc_info.value)
        assert str(row_cap) in message
        assert str(ceiling) in message
        assert "1024" in message

    def test_nonexact_memory_size_uses_nearest_supported(self):
        """A memory size not in AllowedValues (e.g. a stray value from a
        non-CFN caller) is matched to the nearest supported size rather
        than raising a KeyError."""
        ceiling_2048 = IN_MEMORY_MEMORY_CEILING[2048]
        # 2000 is nearer to 2048 than to 1024
        validate_row_cap(ceiling_2048, 2000)  # must not raise
        with pytest.raises(ConfigError):
            validate_row_cap(ceiling_2048 + 1, 2000)


# ---------------------------------------------------------------------------
# Property 3: Row-cap validation matches the memory ceiling
# Feature: scale-threshold-and-drain-throughput, Property 3: Row-cap validation matches the memory ceiling
# Validates: Requirement 3.2
# ---------------------------------------------------------------------------


class TestValidateRowCapProperty:
    @given(
        row_cap=st.integers(min_value=1, max_value=10_000_000),
        lambda_memory_mb=st.sampled_from(_SUPPORTED_MEMORY_SIZES),
    )
    @settings(max_examples=100)
    def test_raises_iff_row_cap_exceeds_ceiling(self, row_cap, lambda_memory_mb):
        """For any (row_cap, lambda_memory_mb) with lambda_memory_mb an
        exactly-supported size, validate_row_cap raises iff row_cap exceeds
        that size's ceiling, and accepts otherwise."""
        ceiling = IN_MEMORY_MEMORY_CEILING[lambda_memory_mb]

        if row_cap > ceiling:
            with pytest.raises(ConfigError):
                validate_row_cap(row_cap, lambda_memory_mb)
        else:
            validate_row_cap(row_cap, lambda_memory_mb)  # must not raise

    @given(
        row_cap=st.integers(min_value=1, max_value=10_000_000),
        lambda_memory_mb=st.integers(min_value=128, max_value=20_000),
    )
    @settings(max_examples=100)
    def test_total_for_any_memory_size_nearest_lookup(self, row_cap, lambda_memory_mb):
        """For any (row_cap, lambda_memory_mb) — including sizes outside the
        supported set — validate_row_cap is total (never raises anything
        other than ConfigError) and its accept/reject decision matches the
        ceiling of the nearest supported memory size."""
        supported = sorted(IN_MEMORY_MEMORY_CEILING)
        nearest = min(supported, key=lambda mb: (abs(mb - lambda_memory_mb), mb))
        ceiling = IN_MEMORY_MEMORY_CEILING[nearest]

        if row_cap > ceiling:
            with pytest.raises(ConfigError):
                validate_row_cap(row_cap, lambda_memory_mb)
        else:
            validate_row_cap(row_cap, lambda_memory_mb)  # must not raise
