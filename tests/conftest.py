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
