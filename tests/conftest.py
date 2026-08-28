"""pytest + Hypothesis configuration for the tag-based S3 replication test suite.

Registers a Hypothesis settings profile that runs each property test with
max_examples=100, satisfying the design requirement that every property test
runs a minimum of 100 iterations.  Individual property tests may raise this
with @settings(max_examples=N).

Profiles:
  default  — 100 examples (standard development runs)
  ci       — 100 examples (same as default; explicit for CI pipelines)
  nightly  — 1000 examples (deep exploration / nightly runs)
"""

from unittest import mock

import pytest
from hypothesis import HealthCheck, Phase, settings

# Feature: tag-based-s3-replication — global Hypothesis profile
settings.register_profile(
    "default",
    max_examples=100,
    suppress_health_check=[HealthCheck.too_slow],
    phases=[
        Phase.explicit,
        Phase.reuse,
        Phase.generate,
        Phase.target,
        Phase.shrink,
    ],
)

settings.register_profile(
    "ci",
    max_examples=100,
    deadline=None,  # CI runners have variable latency; deadline flakes are not meaningful signal
    suppress_health_check=[HealthCheck.too_slow],
    phases=[
        Phase.explicit,
        Phase.reuse,
        Phase.generate,
        Phase.shrink,
    ],
)

settings.register_profile(
    "nightly",
    max_examples=1000,
    suppress_health_check=[HealthCheck.too_slow],
)

settings.load_profile("default")


# ---------------------------------------------------------------------------
# Default the lookback-tail row count to zero for orchestrator-level tests.
#
# `_read_journal_window` sizes the lookback tail before it finds the row-cap
# boundary, so any test driving it with a MagicMock Athena client would
# otherwise fall through to the real `find_tail_row_count` and hang: the poll
# loop sleeps two seconds per attempt against a response that never reaches a
# terminal state. The suite already patches `find_row_count_boundary` at every
# such call site for exactly this reason; a zero tail is patched centrally
# instead, because it is the value that leaves the budget split identical to an
# uncapped read and so keeps every pre-existing test's expectations intact.
#
# A test exercising a non-empty tail patches the same target itself; the inner
# patch wins for its duration.
#
# Patching the attribute on the adapter module leaves `from ... import
# find_tail_row_count` bindings alone, so the adapter's own tests, which call
# the function directly, are unaffected.
@pytest.fixture(autouse=True)
def _default_empty_lookback_tail(request):
    # Scoped to the tests that can reach the orchestrator. `tests/core` and
    # `tests/adapters` never do, and the adapter's own tests call the real
    # function by name, so patching for them would be noise at best.
    if "/tests/core/" in str(request.path) or "/tests/adapters/" in str(request.path):
        yield
        return
    with mock.patch(
        "src.adapters.athena_journal_adapter.find_tail_row_count",
        return_value=0,
    ):
        yield
