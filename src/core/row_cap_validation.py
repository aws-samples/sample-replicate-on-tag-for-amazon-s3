"""Row-cap memory validation (core, pure).

``Journal_Read_Row_Cap`` is now the single scale knob: it bounds both
journal-read pagination cost (see ``manifest_strategy.JOURNAL_READ_ROW_CAP_DEFAULT``)
and, since the ``Unload_Generation`` path was removed, the number of
``Matched_Object`` entries held in memory for a single run's In_Memory
manifest (design.md, "Row-cap memory validation (core, pure)";
Requirement 3.1). This module provides the memory-safety check for that
second role: it validates that a configured ``Journal_Read_Row_Cap`` does
not exceed the largest row count the In_Memory manifest path can safely
hold at the Lambda's configured memory size, so an operator who raises the
cap without also raising ``LambdaMemoryMB`` is rejected at configuration
load — rather than risking an out-of-memory failure mid-run
(Requirement 3.2).

No AWS dependencies — pure core, per the repo's "pure core, thin I/O shell"
architecture.

Ceiling table derivation (Requirement 3.3)
-------------------------------------------
The ceilings are derived from ``benchmarks/real_manifest_benchmark.py``,
which measures peak RSS of the **real** production path
(``ManifestGenerator.accumulate`` → ``.finalize()`` →
``manifest_generator.serialize()``) with realistic ~300-char object keys
and real-length (32-char) version IDs — not the synthetic raw-CSV build in
the older ``benchmarks/manifest_benchmark.py``, which never constructs a
``MatchedObject``, ``ManifestEntry``, or the accumulator dict the real path
carries.

Measured, linear across 100,000–1,500,000 objects:

* **≈1.5 KiB peak RSS per matched object** for the manifest path in
  isolation (the accumulator dict + the finalized ``ManifestEntry`` list +
  the serialized CSV string, all live simultaneously — the CSV buffer is
  already included, it is ~26% of that peak, so there is no additional
  "serialization doubling" on top).
* **≈1.9 KiB peak RSS per matched object** for a realistic full run, where
  the ``deduped_ops`` list (the ``TaggingOperation`` records read from the
  journal) stays alive in ``_process_bucket`` scope while the manifest is
  built and serialized. This is the figure the ceilings are anchored to,
  because that is the memory actually resident at the run's peak.

At 500,000 objects the manifest path measured ≈745 MiB (≈928 MiB with
``deduped_ops``); at 1,000,000 objects it measured ≈1,458 MiB
(≈1,841 MiB with ``deduped_ops``) — i.e. the previous ceiling for a
2,048 MiB Lambda (1,000,000) put the manifest alone at ~71% of the budget
before the interpreter, boto3/Athena buffers, or any headroom. The prior
figure of ~0.8 KiB/object (≈400 MiB at 500k) was never measured against
this code path and understates it by roughly 1.9–2.4x.

Ceiling model
~~~~~~~~~~~~~
Each ceiling is the largest row cap for which the modelled peak stays
comfortably below the memory budget:

    peak ≈ BASELINE_MIB + PER_OBJECT_KIB/1024 × (TIE_OVERSHOOT × row_cap)

with ``BASELINE_MIB ≈ 200`` (Python interpreter + boto3/Athena client +
per-invocation transients), ``PER_OBJECT_KIB ≈ 1.9`` (measured, full run),
and ``TIE_OVERSHOOT = 1.10``. The ``TIE_OVERSHOOT`` factor deliberately
builds in headroom for Finding 2 below.

The resulting table is the previous table halved — 500,000 safe rows per
1,024 MiB of allocated memory, landing on round numbers at every
CloudFormation-allowed ``LambdaMemoryMB`` value (``deploy/template.yaml``).
Under the model above this keeps peak at ≈52–69% of budget across every
tier *with* the 10% tie overshoot applied, and absorbs 44–66% overshoot
before reaching an 85%-of-budget red line — far more than the tie
mechanism can realistically produce.

The cap is approximate, not exact (Finding 2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``Journal_Read_Row_Cap`` bounds the number of rows a run *targets*, not the
exact number it reads. ``find_row_count_boundary`` locates the
``record_timestamp`` of the ``row_cap``-th row and ``read_journal`` reads
that timestamp **inclusively**, so every row sharing the boundary timestamp
is admitted — deliberately, so a tie is never split and no row silently
falls below the advancing watermark (see
``athena_journal_adapter.read_journal``). A large tied batch at the
boundary can therefore push the actual matched-object count modestly above
``row_cap`` (bounded to roughly 10% of the window in the worst realistic
case). The ``TIE_OVERSHOOT`` headroom in the ceiling model above absorbs
this; the orchestrator additionally emits a ``row_cap_overshoot`` audit log
when a run's actual matched count exceeds ``row_cap``, so the run-time
reality is visible rather than assumed (the config-load check here cannot
see it).

If a future re-run of ``benchmarks/real_manifest_benchmark.py`` measures
materially different figures, this table (and this docstring) should be
updated together.

The cap is divided between two ranges
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A run's read spans the lookback tail — ``(watermark - lookback, watermark]``,
re-scanned for late-arriving journal rows — and the rows above the watermark,
which are the only rows that can advance the checkpoint.
:func:`split_row_budget` divides ``Journal_Read_Row_Cap`` between the two and
reserves a floor for the new rows, so a tail larger than the cap cannot leave
the new-row range empty. What the ceiling table below bounds is the **sum** of
the two allowances, which is the whole read, so the ceilings and the measured
per-row figures they are anchored to are unaffected by the split:
:data:`IN_MEMORY_MEMORY_CEILING` and :func:`validate_row_cap` are unchanged by
it.
"""
from __future__ import annotations

from math import floor

from src.core.config_loader import ConfigError
from src.core.manifest_strategy import (
    MIN_NEW_ROW_BUDGET,
    TAIL_ROW_BUDGET_FRACTION,
)

# ---------------------------------------------------------------------------
# Supported LambdaMemoryMB values (deploy/template.yaml, LambdaMemoryMB
# AllowedValues) and their In_Memory_Memory_Ceiling — the largest
# Journal_Read_Row_Cap / Preflight_Count for which In_Memory_Generation
# completes within the memory safety margin at that memory size.
#
# See the module docstring for the derivation: anchored to the measured
# ~1.9 KiB/object real-path peak (benchmarks/real_manifest_benchmark.py),
# 250,000 safe rows per 1,024 MiB, with tie-overshoot headroom baked in.
# ---------------------------------------------------------------------------
IN_MEMORY_MEMORY_CEILING: dict[int, int] = {
    1024: 250_000,
    2048: 500_000,
    3072: 750_000,
    4096: 1_000_000,
    6144: 1_500_000,
    8192: 2_000_000,
    10240: 2_500_000,
}


def split_row_budget(row_cap: int, tail_rows: int) -> tuple[int, int]:
    """Divide *row_cap* between the lookback tail and the rows above the watermark.

    Returns ``(tail_allowance, new_row_budget)``. The two values sum to at most
    *row_cap*, so the total rows a run reads stays bounded by the cap exactly as
    it was before the budget was split. ``new_row_budget`` is never zero: it is
    floored at :data:`~src.core.manifest_strategy.MIN_NEW_ROW_BUDGET`, because a
    new-row budget of zero is what lets a bucket stop draining.

    Three cases, and the first two leave behavior unchanged:

    ==========================  ================  =====================
    Tail size                   ``tail_allowance``  ``new_row_budget``
    ==========================  ================  =====================
    0                           0                 ``row_cap``
    Fits the tail fraction      ``tail_rows``     ``row_cap - tail_rows``
    Exceeds the tail fraction   the fraction      the remainder
    ==========================  ================  =====================

    Pure: no AWS, no I/O.

    Parameters
    ----------
    row_cap:
        The configured ``Journal_Read_Row_Cap``. Must be a positive integer.
    tail_rows:
        Rows in the lookback tail — the range ``(watermark - lookback,
        watermark]``. Must be non-negative.

    Returns
    -------
    tuple[int, int]
        ``(tail_allowance, new_row_budget)``.

    Raises
    ------
    ValueError
        When ``row_cap`` is not positive, or ``tail_rows`` is negative.
    """
    if row_cap <= 0:
        raise ValueError(f"row_cap must be a positive integer, got {row_cap!r}")
    if tail_rows < 0:
        raise ValueError(f"tail_rows must be non-negative, got {tail_rows!r}")

    tail_allowance = min(tail_rows, floor(row_cap * TAIL_ROW_BUDGET_FRACTION))
    new_row_budget = max(MIN_NEW_ROW_BUDGET, row_cap - tail_allowance)
    return tail_allowance, new_row_budget


def _nearest_supported_memory_mb(lambda_memory_mb: int) -> int:
    """Return the supported ``LambdaMemoryMB`` value nearest to *lambda_memory_mb*.

    ``LambdaMemoryMB`` is a CloudFormation parameter constrained by
    ``AllowedValues`` (``deploy/template.yaml``), so in practice the value
    reaching this function is always an exact key of
    ``IN_MEMORY_MEMORY_CEILING``. Nearest-match lookup is implemented anyway
    so this pure function stays total and defensive for any caller that
    supplies a value outside that exact set (e.g. a future
    ``context.memory_limit_in_mb``-sourced value, or a unit test) — ties
    (equidistant between two supported sizes) resolve to the smaller size,
    the more conservative (safer) choice.
    """
    supported = sorted(IN_MEMORY_MEMORY_CEILING)
    return min(supported, key=lambda mb: (abs(mb - lambda_memory_mb), mb))


def validate_row_cap(row_cap: int, lambda_memory_mb: int) -> None:
    """Validate that *row_cap* fits the In_Memory_Memory_Ceiling for *lambda_memory_mb*.

    Parameters
    ----------
    row_cap:
        The configured ``Journal_Read_Row_Cap``.
    lambda_memory_mb:
        The Lambda's configured memory size in MiB (``LambdaMemoryMB``).
        Matched to the nearest supported size in ``IN_MEMORY_MEMORY_CEILING``
        (see :func:`_nearest_supported_memory_mb`).

    Raises
    ------
    ConfigError
        When ``row_cap`` exceeds the ceiling for the nearest supported memory
        size. The message names the offending ``row_cap`` value, the
        ceiling, and the configured memory size (Requirement 3.2).
    """
    nearest_mb = _nearest_supported_memory_mb(lambda_memory_mb)
    ceiling = IN_MEMORY_MEMORY_CEILING[nearest_mb]
    if row_cap > ceiling:
        raise ConfigError(
            f"journal_read_row_cap={row_cap} exceeds the In_Memory_Memory_Ceiling "
            f"of {ceiling} rows for a {lambda_memory_mb} MiB Lambda "
            f"(nearest supported LambdaMemoryMB: {nearest_mb}). Lower "
            f"journal_read_row_cap to at most {ceiling}, or raise LambdaMemoryMB, "
            f"to avoid an out-of-memory failure mid-run."
        )
