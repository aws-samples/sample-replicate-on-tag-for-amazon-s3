"""Metrics_Publisher adapter — publishes run counters as CloudWatch custom metrics.

The feature is gated on the ``metrics_namespace`` runtime config key.  When the
key is absent, empty, or whitespace-only the publisher is a strict no-op: no
CloudWatch client is constructed, no boto3 call is made, and no IAM permission
is required (Requirements 1.1, 1.2, 1.3).

The conversion of a Run_Result into CloudWatch MetricDatum dicts is a pure
helper (``_build_metric_data``) so it can be unit-tested without any AWS
interaction.

The ``MetricsPublisher.publish`` method intentionally does **not** catch its own
exceptions.  The orchestrator wraps the call in a try/except and emits an error
log entry on failure, ensuring metric publishing never fails the run
(Requirement 5.1).

Requirements: 1.1, 1.2, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 3.1, 3.2,
             3.3, 4.3, 5.1, 9.2, 9.3, 9.4
"""
from __future__ import annotations

from typing import Any

from src.core.models import RunResult

# Maximum number of MetricDatum entries allowed per PutMetricData request.
_MAX_DATUMS_PER_CALL = 1000


# ---------------------------------------------------------------------------
# Pure metric-data builder
# ---------------------------------------------------------------------------


def _build_metric_data(
    run_result: RunResult,
    extra_dimensions: dict[str, str] | None = None,
) -> list[dict]:
    """Convert a RunResult into a list of CloudWatch MetricDatum dicts.

    Pure function — no I/O, no AWS calls.

    Per-bucket datums carry a ``SourceBucket`` base dimension (Req 2.5), plus
    any ``extra_dimensions`` entries after it (Req 9.3).

    The run-level ``DisabledBuckets`` datum carries no base dimension
    (Req 3.3), only any ``extra_dimensions`` (Req 9.3).

    Zero-suppression (Req 2.6)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~
    CloudWatch bills each unique metric-and-dimension combination per hour in
    which a datum is sent, so a per-bucket metric published with a zero value
    on every run costs the same as one carrying real activity. The three
    activity counters (``TaggingOperationsRead``, ``MatchedObjects``,
    ``BatchJobsSubmitted``) are therefore emitted only when at least one of
    them is non-zero — they are suppressed as a group, so a run with any
    activity still publishes all three including any genuine zeros among them.

    ``BucketErrors`` is always emitted, including its zero value. It is the
    per-bucket liveness signal: its presence means the bucket was enabled and
    processed this run, so an operator can alarm on missing data to detect a
    bucket that silently stopped being processed. A disabled bucket produces no
    ``BucketMetrics`` at all (see ``orchestrator.run_interval``), hence no
    datums, which is what makes that alarm meaningful.

    Gap semantics that follow:

    - activity counters absent, ``BucketErrors`` present → processed, idle
    - ``BucketErrors`` absent → disabled, removed from config, or not reached

    Parameters
    ----------
    run_result:
        Aggregated per-bucket counters and run-level duplicate count for one
        Processing_Interval.
    extra_dimensions:
        Optional mapping of additional dimension names to values applied to
        every datum (Req 9.3).  When absent or empty, only the base dimensions
        are emitted (Req 9.4).

    Returns
    -------
    list[dict]
        A flat list of CloudWatch MetricDatum dicts ready to pass directly to
        ``put_metric_data(Namespace=..., MetricData=...)``.
    """
    extra_dims: list[dict] = []
    if extra_dimensions:
        extra_dims = [{"Name": k, "Value": v} for k, v in extra_dimensions.items()]

    datums: list[dict] = []

    for bm in run_result.buckets:
        base_dims = [{"Name": "SourceBucket", "Value": bm.source_bucket}]
        dims = base_dims + extra_dims  # SourceBucket first, then extras (Req 9.3)

        # Activity counters are emitted as a group, and only when the bucket did
        # something this run — publishing flat zeros every run costs the full
        # per-metric monthly rate for no signal (Req 2.6).
        if bm.ops_read or bm.matched or bm.submitted:
            datums.extend([
                # Req 2.1
                {
                    "MetricName": "TaggingOperationsRead",
                    "Dimensions": dims,
                    "Value": float(bm.ops_read),
                    "Unit": "Count",
                },
                # Req 2.2
                {
                    "MetricName": "MatchedObjects",
                    "Dimensions": dims,
                    "Value": float(bm.matched),
                    "Unit": "Count",
                },
                # Req 2.3
                {
                    "MetricName": "BatchJobsSubmitted",
                    "Dimensions": dims,
                    "Value": float(bm.submitted),
                    "Unit": "Count",
                },
            ])

        # Emitted only when non-zero, unlike BucketErrors below. Objects in an
        # archived storage class are an occasional condition rather than a
        # liveness signal, so publishing a flat zero on every run for every
        # bucket would cost the full per-metric monthly rate to say nothing
        # (the same reasoning as the activity group above). An alarm on this
        # metric should treat missing data as not breaching.
        if bm.archived_excluded:
            datums.append({
                "MetricName": "ArchivedObjectsExcluded",
                "Dimensions": dims,
                "Value": float(bm.archived_excluded),
                "Unit": "Count",
            })

        # Always emitted, including zero — per-bucket liveness signal
        # (Req 2.4, 2.6, 7.3).
        datums.append({
            "MetricName": "BucketErrors",
            "Dimensions": dims,
            "Value": 1.0 if bm.errored else 0.0,
            "Unit": "Count",
        })

    # Run-level metric — no SourceBucket base dimension (Req 3.2, 9.4:
    # extra_dims is an empty list when no extras are configured).
    #
    # Always emitted, including zero, so `>= 1` is a usable alarm threshold
    # rather than a missing-data condition. Run-level rather than per-bucket
    # deliberately: one metric covers any number of buckets at a flat cost,
    # and the bucket's identity is already carried by the disable
    # notification, the per-run error log entry, and Solution_Config
    # (Req 3.3).
    datums.append({
        "MetricName": "DisabledBuckets",
        "Dimensions": extra_dims,
        "Value": float(run_result.disabled_buckets),
        "Unit": "Count",
    })

    return datums


# ---------------------------------------------------------------------------
# MetricsPublisher
# ---------------------------------------------------------------------------


class MetricsPublisher:
    """Publishes a RunResult as CloudWatch custom metrics.

    Gated on the optional ``namespace`` argument.  When the namespace is absent,
    empty, or whitespace-only, ``publish`` returns immediately without
    constructing a CloudWatch client or making any API call (Req 1.2).

    Parameters
    ----------
    namespace:
        CloudWatch namespace to publish into.  ``None``, empty string, or
        whitespace-only disables publishing entirely.
    dimensions:
        Optional mapping of extra dimension names to values applied to every
        datum (Req 9.2, 9.3).
    cloudwatch_client:
        Optional pre-constructed CloudWatch boto3 client.  When provided the
        publisher uses it directly (useful for testing without actual AWS
        calls).  When ``None`` (the default) the client is constructed lazily
        on the first ``publish`` call that actually needs it.
    """

    def __init__(
        self,
        namespace: str | None,
        dimensions: dict[str, str] | None = None,
        cloudwatch_client: Any | None = None,
    ) -> None:
        self._namespace: str = (namespace or "").strip()
        self._dimensions: dict[str, str] = dimensions or {}
        self._client = cloudwatch_client  # injected client (may be None)

    # ------------------------------------------------------------------
    # publish
    # ------------------------------------------------------------------

    def publish(self, run_result: RunResult) -> None:
        """Publish *run_result* as CloudWatch custom metrics.

        No-op when the namespace is falsy after stripping (Req 1.2).
        Does **not** catch exceptions — the orchestrator owns the try/except
        (Req 5.1).

        Parameters
        ----------
        run_result:
            The aggregated outcome of a Processing_Interval to publish.
        """
        # Req 1.2: strict no-op when namespace is absent / empty / whitespace
        if not self._namespace:
            return

        metric_data = _build_metric_data(run_result, self._dimensions or None)

        # Lazy client construction — only when publishing is actually needed
        client = self._client
        if client is None:
            import boto3
            client = boto3.client("cloudwatch")

        # Chunk into batches of at most _MAX_DATUMS_PER_CALL (Req 4.3 / design)
        for offset in range(0, len(metric_data), _MAX_DATUMS_PER_CALL):
            chunk = metric_data[offset : offset + _MAX_DATUMS_PER_CALL]
            client.put_metric_data(
                Namespace=self._namespace,
                MetricData=chunk,
            )
