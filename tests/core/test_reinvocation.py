"""Tests for src/core/reinvocation.py — Task 5.1/5.2.

Properties 1, 2: reinvocation decision correctness and bounded chain.

Feature: scale-threshold-and-drain-throughput
Requirements: 4.1, 4.2, 4.3, 5.1, 5.2, 5.4
"""
from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.reinvocation import should_reinvoke


# ---------------------------------------------------------------------------
# Unit / edge-case tests
# ---------------------------------------------------------------------------


class TestShouldReinvoke:
    def test_all_conditions_true_reinvokes(self):
        assert should_reinvoke(True, True, 0, 20, True) is True

    def test_not_capped_never_reinvokes(self):
        """Req 4.2: a fully drained window must not reinvoke."""
        assert should_reinvoke(False, True, 0, 20, True) is False

    def test_not_progressed_never_reinvokes(self):
        """Req 4.3: a failing run must not storm reinvocations."""
        assert should_reinvoke(True, False, 0, 20, True) is False

    def test_bucket_inactive_never_reinvokes(self):
        """Req 5.4: disabled/circuit-broken bucket is a no-op."""
        assert should_reinvoke(True, True, 0, 20, False) is False

    def test_depth_at_limit_does_not_reinvoke(self):
        """Req 5.1: depth == chain_limit is already at the limit."""
        assert should_reinvoke(True, True, 20, 20, True) is False

    def test_depth_one_below_limit_reinvokes(self):
        assert should_reinvoke(True, True, 19, 20, True) is True

    def test_depth_beyond_limit_does_not_reinvoke(self):
        assert should_reinvoke(True, True, 21, 20, True) is False

    def test_chain_limit_zero_never_reinvokes(self):
        """A configured limit of 0 disables reinvocation entirely."""
        assert should_reinvoke(True, True, 0, 0, True) is False

    def test_all_false_does_not_reinvoke(self):
        assert should_reinvoke(False, False, 0, 0, False) is False

    def test_is_pure_and_total_no_exception(self):
        """Negative depth/limit values are still handled without raising."""
        assert should_reinvoke(True, True, -1, 20, True) is True
        assert should_reinvoke(True, True, 0, -1, True) is False


# ---------------------------------------------------------------------------
# Property 1: Reinvocation decision is total and correct
# Feature: scale-threshold-and-drain-throughput, Property 1: Reinvocation decision is total and correct
# Requirements: 4.1, 4.2, 4.3, 5.1
# ---------------------------------------------------------------------------


@given(
    capped=st.booleans(),
    progressed=st.booleans(),
    depth=st.integers(min_value=-5, max_value=50),
    chain_limit=st.integers(min_value=-5, max_value=50),
    bucket_active=st.booleans(),
)
@settings(max_examples=100)
def test_property_1_reinvocation_decision_total_and_correct(
    capped: bool,
    progressed: bool,
    depth: int,
    chain_limit: int,
    bucket_active: bool,
) -> None:
    """Property 1: should_reinvoke is True iff capped, progressed,
    bucket_active all hold and depth < chain_limit.

    # Feature: scale-threshold-and-drain-throughput, Property 1: Reinvocation decision is total and correct
    """
    result = should_reinvoke(capped, progressed, depth, chain_limit, bucket_active)

    expected = capped and progressed and bucket_active and depth < chain_limit
    assert result == expected
    assert isinstance(result, bool)

    # No storm on failure (Req 4.2, 4.3).
    if not progressed:
        assert result is False
    if not capped:
        assert result is False

    # Bounded chain (Req 5.1).
    if depth >= chain_limit:
        assert result is False


# ---------------------------------------------------------------------------
# Property 2: Reinvocation chain is strictly bounded
# Feature: scale-threshold-and-drain-throughput, Property 2: Reinvocation chain is strictly bounded
# Requirements: 5.1, 5.2
# ---------------------------------------------------------------------------


@given(
    chain_limit=st.integers(min_value=0, max_value=30),
    max_iterations=st.integers(min_value=0, max_value=100),
)
@settings(max_examples=100)
def test_property_2_reinvocation_chain_strictly_bounded(
    chain_limit: int,
    max_iterations: int,
) -> None:
    """Property 2: a chain starting at depth 0 with limit L strictly
    increases depth on each reinvocation, never reinvokes at depth >= L,
    and therefore never exceeds L reinvocations.

    Simulates the chain: every run in the simulation is capped and
    progressed and the bucket stays active, so the *only* thing that can
    stop the chain is the chain_limit itself. This isolates the
    depth/limit boundary behavior the property is about.

    # Feature: scale-threshold-and-drain-throughput, Property 2: Reinvocation chain is strictly bounded
    """
    depth = 0
    reinvocation_count = 0
    depths_seen = [depth]

    for _ in range(max_iterations):
        decision = should_reinvoke(
            capped=True,
            progressed=True,
            depth=depth,
            chain_limit=chain_limit,
            bucket_active=True,
        )
        if not decision:
            break
        # No reinvocation is issued at depth >= chain_limit.
        assert depth < chain_limit
        depth += 1
        reinvocation_count += 1
        depths_seen.append(depth)

    # The loop always terminates (within max_iterations, well before that in
    # practice) and the chain length never exceeds chain_limit.
    assert reinvocation_count <= chain_limit
    # Depth strictly increases across successive reinvocations.
    assert depths_seen == sorted(set(depths_seen))
    for i in range(1, len(depths_seen)):
        assert depths_seen[i] == depths_seen[i - 1] + 1
