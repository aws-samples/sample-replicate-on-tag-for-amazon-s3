"""Unit tests for src/adapters/metrics_publisher.py — task 7.1.

Covers:
  - No-op gate: absent/empty/whitespace namespace → no client, no PutMetricData
  - Configured namespace → correct metric names, values, units, dimensions
  - BucketErrors: 1 for errored bucket, 0 otherwise; always emitted (liveness)
  - Zero-suppression: the three activity counters are emitted as a group only
    when the bucket had activity this run
  - Batching: >1 000 datums yields multiple PutMetricData calls each ≤ 1 000
  - extra dimensions applied to every datum (per-bucket and run-level)
  - Pure _build_metric_data helper: exact datum shapes

Feature: cloudwatch-metrics
Requirements: 1.2, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 3.1, 3.2, 3.3, 7.3,
             9.2, 9.3, 9.4
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.adapters.metrics_publisher import MetricsPublisher, _build_metric_data
from src.core.models import BucketMetrics, RunResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bm(name: str, ops: int = 3, matched: int = 2, submitted: int = 1,
        errored: bool = False) -> BucketMetrics:
    return BucketMetrics(
        source_bucket=name,
        ops_read=ops,
        matched=matched,
        submitted=submitted,
        errored=errored,
    )


def _run(*buckets: BucketMetrics, disabled: int = 0) -> RunResult:
    return RunResult(buckets=list(buckets), disabled_buckets=disabled)


def _datum_names(datums: list[dict]) -> list[str]:
    return [d["MetricName"] for d in datums]


def _dims_for(datums: list[dict], metric: str) -> list[dict]:
    return next(d["Dimensions"] for d in datums if d["MetricName"] == metric)


def _value_for(datums: list[dict], metric: str) -> float:
    return next(d["Value"] for d in datums if d["MetricName"] == metric)


# ---------------------------------------------------------------------------
# No-op gate: absent / empty / whitespace namespace
# Requirement 1.2
# ---------------------------------------------------------------------------


class TestNoOpGate:
    @pytest.mark.parametrize("namespace", [None, "", "   ", "\t", "\n"])
    def test_publish_does_not_call_put_metric_data_when_namespace_falsy(
        self, namespace
    ):
        """No PutMetricData call when namespace is absent/empty/whitespace (Req 1.2)."""
        mock_client = MagicMock()
        pub = MetricsPublisher(namespace, cloudwatch_client=mock_client)
        pub.publish(_run(_bm("b1")))
        mock_client.put_metric_data.assert_not_called()

    @pytest.mark.parametrize("namespace", [None, "", "   "])
    def test_no_client_constructed_when_namespace_falsy(self, namespace):
        """No boto3 client constructed when namespace is falsy (Req 1.2).

        _client starts as None at __init__; after a no-op publish it must
        still be None — the lazy boto3.client() construction never ran.
        """
        pub = MetricsPublisher(namespace)  # no injected client
        pub.publish(_run(_bm("b1")))
        # If boto3.client() had been called, _client would no longer be None
        assert pub._client is None

    @pytest.mark.parametrize("namespace", [None, "", "  "])
    def test_no_client_publisher_does_not_raise(self, namespace):
        """Publisher with no injected client and falsy namespace must not raise (Req 1.2)."""
        pub = MetricsPublisher(namespace)  # no injected client
        # Should silently no-op
        pub.publish(_run(_bm("b1")))


# ---------------------------------------------------------------------------
# Configured namespace: correct Namespace, metric names, values, units, dims
# Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 3.1, 3.2
# ---------------------------------------------------------------------------


class TestConfiguredNamespace:
    def setup_method(self):
        self.mock_client = MagicMock()
        self.ns = "TestNS/Metrics"
        self.pub = MetricsPublisher(self.ns, cloudwatch_client=self.mock_client)

    def test_put_metric_data_called_once_for_small_run(self):
        """Single PutMetricData call for a run with one bucket (Req 4.3)."""
        self.pub.publish(_run(_bm("my-bucket")))
        assert self.mock_client.put_metric_data.call_count == 1

    def test_correct_namespace_passed(self):
        """Namespace argument matches the configured namespace (Req 1.4)."""
        self.pub.publish(_run(_bm("b1")))
        kwargs = self.mock_client.put_metric_data.call_args.kwargs
        assert kwargs["Namespace"] == self.ns

    def test_per_bucket_metric_names_present(self):
        """Four per-bucket metric names are present in the data (Req 2.1–2.4)."""
        self.pub.publish(_run(_bm("b1")))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        names = _datum_names(data)
        assert "TaggingOperationsRead" in names
        assert "MatchedObjects" in names
        assert "BatchJobsSubmitted" in names
        assert "BucketErrors" in names

    def test_run_level_metric_name_present(self):
        """DisabledBuckets present in metric data (Req 3.1)."""
        self.pub.publish(_run(_bm("b1")))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        assert "DisabledBuckets" in _datum_names(data)

    def test_per_bucket_metric_values(self):
        """TaggingOperationsRead, MatchedObjects, BatchJobsSubmitted values match (Req 2.1–2.3)."""
        self.pub.publish(_run(_bm("b1", ops=5, matched=4, submitted=1)))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        assert _value_for(data, "TaggingOperationsRead") == 5.0
        assert _value_for(data, "MatchedObjects") == 4.0
        assert _value_for(data, "BatchJobsSubmitted") == 1.0

    def test_disabled_buckets_value(self):
        """DisabledBuckets value equals run_result.disabled_buckets (Req 3.3)."""
        self.pub.publish(_run(_bm("b1"), disabled=13))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        assert _value_for(data, "DisabledBuckets") == 13.0

    def test_all_datums_have_count_unit(self):
        """Every datum uses Unit='Count' (Req 2.1–2.4, 3.1)."""
        self.pub.publish(_run(_bm("b1")))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        for d in data:
            assert d["Unit"] == "Count", f"Datum {d['MetricName']!r} missing Count unit"

    def test_per_bucket_datums_have_source_bucket_dimension(self):
        """SourceBucket dimension present on each per-bucket datum (Req 2.5)."""
        self.pub.publish(_run(_bm("my-bkt")))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        per_bucket_metrics = {
            "TaggingOperationsRead", "MatchedObjects",
            "BatchJobsSubmitted", "BucketErrors",
        }
        for d in data:
            if d["MetricName"] in per_bucket_metrics:
                dim_names = [dim["Name"] for dim in d["Dimensions"]]
                assert "SourceBucket" in dim_names, (
                    f"{d['MetricName']} missing SourceBucket dimension"
                )
                dim_map = {dim["Name"]: dim["Value"] for dim in d["Dimensions"]}
                assert dim_map["SourceBucket"] == "my-bkt"

    def test_run_level_metric_has_no_source_bucket_dimension(self):
        """DisabledBuckets must have no SourceBucket dimension (Req 3.2)."""
        self.pub.publish(_run(_bm("b1")))
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        run_level = next(d for d in data if d["MetricName"] == "DisabledBuckets")
        dim_names = [dim["Name"] for dim in run_level["Dimensions"]]
        assert "SourceBucket" not in dim_names


# ---------------------------------------------------------------------------
# BucketErrors: 1 for errored bucket, 0 otherwise (Req 2.4, 7.3)
# ---------------------------------------------------------------------------


class TestBucketErrors:
    def setup_method(self):
        self.mock_client = MagicMock()
        self.pub = MetricsPublisher("NS", cloudwatch_client=self.mock_client)

    def _get_bucket_errors(self, bucket_name: str) -> float:
        data = self.mock_client.put_metric_data.call_args.kwargs["MetricData"]
        for d in data:
            if d["MetricName"] == "BucketErrors":
                dim_map = {dim["Name"]: dim["Value"] for dim in d["Dimensions"]}
                if dim_map.get("SourceBucket") == bucket_name:
                    return d["Value"]
        raise AssertionError(f"BucketErrors not found for {bucket_name!r}")

    def test_bucket_errors_is_1_for_errored_bucket(self):
        """BucketErrors=1 when errored=True (Req 2.4, 7.3)."""
        self.pub.publish(_run(_bm("err-bucket", errored=True)))
        assert self._get_bucket_errors("err-bucket") == 1.0

    def test_bucket_errors_is_0_for_healthy_bucket(self):
        """BucketErrors=0 when errored=False (Req 2.4, 7.3)."""
        self.pub.publish(_run(_bm("ok-bucket", errored=False)))
        assert self._get_bucket_errors("ok-bucket") == 0.0

    def test_bucket_errors_emitted_for_idle_bucket(self):
        """BucketErrors is emitted even when the bucket had no activity (Req 2.8).

        Its presence is the per-bucket liveness signal, so it must survive
        zero-suppression of the activity counters.
        """
        self.pub.publish(_run(_bm("idle", ops=0, matched=0, submitted=0)))
        assert self._get_bucket_errors("idle") == 0.0

    def test_mixed_errored_and_healthy_buckets(self):
        """Both buckets appear; errored=1, healthy=0 (Req 2.4)."""
        mock_client = MagicMock()
        pub = MetricsPublisher("NS", cloudwatch_client=mock_client)
        pub.publish(_run(_bm("good", errored=False), _bm("bad", errored=True)))
        data = mock_client.put_metric_data.call_args.kwargs["MetricData"]
        errors_by_bucket = {}
        for d in data:
            if d["MetricName"] == "BucketErrors":
                dim_map = {dim["Name"]: dim["Value"] for dim in d["Dimensions"]}
                errors_by_bucket[dim_map["SourceBucket"]] = d["Value"]
        assert errors_by_bucket["good"] == 0.0
        assert errors_by_bucket["bad"] == 1.0


# ---------------------------------------------------------------------------
# Zero-suppression of the activity counters (Req 2.6, 2.7, 2.8)
# ---------------------------------------------------------------------------


_ACTIVITY_METRICS = {
    "TaggingOperationsRead", "MatchedObjects", "BatchJobsSubmitted",
}

# Run-level datums emitted once per run regardless of bucket count:
# DisabledBuckets and DisabledBuckets.
_RUN_LEVEL_DATUMS = 1


class TestActivityCounterSuppression:
    def test_idle_bucket_emits_only_bucket_errors(self):
        """A bucket with no activity emits BucketErrors and nothing else (Req 2.7)."""
        data = _build_metric_data(_run(_bm("idle", ops=0, matched=0, submitted=0)))
        per_bucket = [d["MetricName"] for d in data
                      if any(dim["Name"] == "SourceBucket" for dim in d["Dimensions"])]
        assert per_bucket == ["BucketErrors"]

    def test_idle_bucket_still_emits_run_level_metric(self):
        """The run-level heartbeat is unaffected by per-bucket suppression (Req 3.3)."""
        data = _build_metric_data(
            _run(_bm("idle", ops=0, matched=0, submitted=0), disabled=4)
        )
        assert _value_for(data, "DisabledBuckets") == 4.0

    @pytest.mark.parametrize(
        "ops,matched,submitted",
        [
            (1, 0, 0),  # journal activity only, nothing matched
            (0, 1, 0),  # matched without ops (defensive: shapes still grouped)
            (0, 0, 1),  # job submitted only
            (5, 3, 1),  # fully active
        ],
    )
    def test_any_activity_emits_all_three_counters(self, ops, matched, submitted):
        """Any non-zero counter emits all three, genuine zeros included (Req 2.6)."""
        data = _build_metric_data(
            _run(_bm("busy", ops=ops, matched=matched, submitted=submitted))
        )
        names = set(_datum_names(data))
        assert _ACTIVITY_METRICS <= names
        assert _value_for(data, "TaggingOperationsRead") == float(ops)
        assert _value_for(data, "MatchedObjects") == float(matched)
        assert _value_for(data, "BatchJobsSubmitted") == float(submitted)

    def test_errored_idle_bucket_emits_bucket_errors_only(self):
        """An errored bucket with no counters still reports the error (Req 2.4, 2.7)."""
        data = _build_metric_data(
            _run(_bm("bad", ops=0, matched=0, submitted=0, errored=True))
        )
        per_bucket = [d["MetricName"] for d in data
                      if any(dim["Name"] == "SourceBucket" for dim in d["Dimensions"])]
        assert per_bucket == ["BucketErrors"]
        assert _value_for(data, "BucketErrors") == 1.0

    def test_suppression_is_per_bucket_not_per_run(self):
        """An idle bucket is suppressed while an active one in the same run is not."""
        data = _build_metric_data(
            _run(
                _bm("busy", ops=7, matched=2, submitted=1),
                _bm("idle", ops=0, matched=0, submitted=0),
            )
        )
        by_bucket: dict[str, set[str]] = {}
        for d in data:
            dim_map = {dim["Name"]: dim["Value"] for dim in d["Dimensions"]}
            bkt = dim_map.get("SourceBucket")
            if bkt:
                by_bucket.setdefault(bkt, set()).add(d["MetricName"])
        assert by_bucket["busy"] == _ACTIVITY_METRICS | {"BucketErrors"}
        assert by_bucket["idle"] == {"BucketErrors"}

    def test_idle_bucket_costs_one_datum_instead_of_four(self):
        """Datum count drops from 4 to 1 per idle bucket (the cost property).

        Plus the two run-level datums, which are independent of bucket count.
        """
        idle = _build_metric_data(
            _run(*[_bm(f"b{i}", ops=0, matched=0, submitted=0) for i in range(10)])
        )
        busy = _build_metric_data(_run(*[_bm(f"b{i}") for i in range(10)]))
        assert len(idle) == 10 * 1 + _RUN_LEVEL_DATUMS
        assert len(busy) == 10 * 4 + _RUN_LEVEL_DATUMS


# ---------------------------------------------------------------------------
# DisabledBuckets — run-level auto-disable visibility (Req 3.3)
# ---------------------------------------------------------------------------


def _disabled_value(datums: list[dict]) -> float:
    return next(d["Value"] for d in datums if d["MetricName"] == "DisabledBuckets")


class TestDisabledBuckets:
    def test_emitted_as_zero_when_no_bucket_disabled(self):
        """Always published, so `>= 1` is an alarm threshold rather than a
        missing-data condition (Req 3.3)."""
        data = _build_metric_data(_run(_bm("b1")))
        assert _disabled_value(data) == 0.0

    def test_reports_the_disabled_count(self):
        rr = RunResult(
            buckets=[_bm("b1")], disabled_buckets=3
        )
        assert _disabled_value(_build_metric_data(rr)) == 3.0

    def test_emitted_even_when_no_bucket_produced_metrics(self):
        """The case that motivated the metric: a disabled bucket is skipped
        before any BucketMetrics exists, so with every bucket disabled there
        are no per-bucket datums at all — and this count is the only signal
        that the Solution is not replicating anything (Req 3.3)."""
        rr = RunResult(buckets=[], disabled_buckets=2)
        data = _build_metric_data(rr)
        assert _disabled_value(data) == 2.0
        assert not [
            d for d in data
            if any(dim["Name"] == "SourceBucket" for dim in d["Dimensions"])
        ]

    def test_carries_no_source_bucket_dimension(self):
        """Run-level, so its cost is flat regardless of bucket count (Req 3.3)."""
        data = _build_metric_data(_run(_bm("b1")))
        datum = next(d for d in data if d["MetricName"] == "DisabledBuckets")
        assert [dim["Name"] for dim in datum["Dimensions"]] == []

    def test_carries_extra_dimensions_only(self):
        rr = RunResult(
            buckets=[_bm("b1")], disabled_buckets=1
        )
        data = _build_metric_data(rr, extra_dimensions={"Deployment": "d1"})
        datum = next(d for d in data if d["MetricName"] == "DisabledBuckets")
        assert datum["Dimensions"] == [{"Name": "Deployment", "Value": "d1"}]

    def test_defaults_to_zero_when_field_omitted(self):
        """Back-compatible with a RunResult built without the field."""
        rr = RunResult(buckets=[_bm("b1")])
        assert _disabled_value(_build_metric_data(rr)) == 0.0


# ---------------------------------------------------------------------------
# Batching: >1 000 datums → multiple calls each ≤ 1 000 (Req 4.3 / design)
# ---------------------------------------------------------------------------


class TestBatching:
    def test_one_call_per_batch_of_1000(self):
        """250 buckets × 4 metrics + 2 run-level = 1 002 datums → 2 calls (Req design)."""
        mock_client = MagicMock()
        pub = MetricsPublisher("NS", cloudwatch_client=mock_client)
        buckets = [_bm(f"bucket-{i:04d}") for i in range(250)]
        pub.publish(_run(*buckets))
        # 250 buckets × 4 per-bucket datums = 1000 + 2 run-level = 1002 datums
        assert mock_client.put_metric_data.call_count == 2

    def test_each_batch_at_most_1000_datums(self):
        """Each individual PutMetricData call must contain ≤ 1 000 data points."""
        mock_client = MagicMock()
        pub = MetricsPublisher("NS", cloudwatch_client=mock_client)
        buckets = [_bm(f"b-{i:04d}") for i in range(500)]
        # 500 × 4 + 1 = 2001 datums → 3 calls
        pub.publish(_run(*buckets))
        for c in mock_client.put_metric_data.call_args_list:
            chunk = c.kwargs["MetricData"]
            assert len(chunk) <= 1000, f"Batch too large: {len(chunk)}"

    def test_total_datums_across_all_calls_matches_expected(self):
        """Total datums across all batched calls equals n_buckets×4 + run-level."""
        mock_client = MagicMock()
        pub = MetricsPublisher("NS", cloudwatch_client=mock_client)
        n = 300
        buckets = [_bm(f"b-{i:04d}") for i in range(n)]
        pub.publish(_run(*buckets))
        total = sum(
            len(c.kwargs["MetricData"])
            for c in mock_client.put_metric_data.call_args_list
        )
        assert total == n * 4 + _RUN_LEVEL_DATUMS


# ---------------------------------------------------------------------------
# Extra dimensions: per-bucket and run-level (Req 9.2, 9.3, 9.4)
# ---------------------------------------------------------------------------


class TestExtraDimensions:
    def _get_datums(self, extra_dims: dict | None) -> list[dict]:
        mock_client = MagicMock()
        pub = MetricsPublisher("NS", dimensions=extra_dims,
                               cloudwatch_client=mock_client)
        pub.publish(_run(_bm("bkt")))
        return mock_client.put_metric_data.call_args.kwargs["MetricData"]

    def test_per_bucket_datums_carry_extra_dimensions(self):
        """Per-bucket datums carry [SourceBucket, <extras>] when extras set (Req 9.3)."""
        data = self._get_datums({"Deployment": "stack-a"})
        per_bucket_names = {"TaggingOperationsRead", "MatchedObjects",
                            "BatchJobsSubmitted", "BucketErrors"}
        for d in data:
            if d["MetricName"] in per_bucket_names:
                dim_map = {dim["Name"]: dim["Value"] for dim in d["Dimensions"]}
                assert dim_map.get("SourceBucket") == "bkt"
                assert dim_map.get("Deployment") == "stack-a"

    def test_run_level_datum_carries_extra_dimensions_only(self):
        """DisabledBuckets carries only extra dims, no SourceBucket (Req 9.3)."""
        data = self._get_datums({"Deployment": "stack-a"})
        run_d = next(d for d in data if d["MetricName"] == "DisabledBuckets")
        dim_map = {dim["Name"]: dim["Value"] for dim in run_d["Dimensions"]}
        assert "SourceBucket" not in dim_map
        assert dim_map.get("Deployment") == "stack-a"

    def test_no_extra_dimensions_when_absent(self):
        """Without extra dims, per-bucket only has SourceBucket; run-level empty (Req 9.4)."""
        data = self._get_datums(None)
        per_bkt = next(d for d in data if d["MetricName"] == "TaggingOperationsRead")
        assert per_bkt["Dimensions"] == [{"Name": "SourceBucket", "Value": "bkt"}]
        run_d = next(d for d in data if d["MetricName"] == "DisabledBuckets")
        assert run_d["Dimensions"] == []

    def test_source_bucket_is_first_dimension_in_per_bucket_datums(self):
        """SourceBucket must be the first dimension in per-bucket datums (Req 9.3)."""
        data = self._get_datums({"Deployment": "d1", "Stage": "prod"})
        per_bkt = next(d for d in data if d["MetricName"] == "MatchedObjects")
        assert per_bkt["Dimensions"][0]["Name"] == "SourceBucket"


# ---------------------------------------------------------------------------
# Pure helper: _build_metric_data exact datum shapes
# Requirements: 2.1–2.6, 3.1–3.3, 9.2–9.4
# ---------------------------------------------------------------------------


class TestBuildMetricDataPure:
    def test_single_bucket_no_extra_dims(self):
        """Four per-bucket datums plus the run-level one; exact shape verified."""
        rr = _run(_bm("my-bucket", ops=10, matched=8, submitted=1, errored=False),
                  disabled=2)
        data = _build_metric_data(rr)
        assert len(data) == 4 + _RUN_LEVEL_DATUMS  # per-bucket + run-level

        by_name = {d["MetricName"]: d for d in data}
        assert by_name["TaggingOperationsRead"]["Value"] == 10.0
        assert by_name["MatchedObjects"]["Value"] == 8.0
        assert by_name["BatchJobsSubmitted"]["Value"] == 1.0
        assert by_name["BucketErrors"]["Value"] == 0.0
        assert by_name["DisabledBuckets"]["Value"] == 2.0

        # Dimension shapes
        src = [{"Name": "SourceBucket", "Value": "my-bucket"}]
        assert by_name["TaggingOperationsRead"]["Dimensions"] == src
        assert by_name["DisabledBuckets"]["Dimensions"] == []

    def test_errored_bucket_sets_bucket_errors_to_1(self):
        """BucketErrors=1.0 for errored bucket."""
        rr = _run(_bm("e-bucket", errored=True))
        data = _build_metric_data(rr)
        by_name = {d["MetricName"]: d for d in data}
        assert by_name["BucketErrors"]["Value"] == 1.0

    def test_two_buckets_produces_expected_datum_count(self):
        """Two buckets → 2×4 per-bucket + 2 run-level = 10 datums."""
        rr = _run(_bm("a"), _bm("b"))
        data = _build_metric_data(rr)
        assert len(data) == 2 * 4 + _RUN_LEVEL_DATUMS

    def test_extra_dimensions_appended_after_source_bucket(self):
        """Extra dims follow SourceBucket in per-bucket datums; run-level has them only."""
        rr = _run(_bm("bkt"))
        data = _build_metric_data(rr, extra_dimensions={"Env": "prod"})
        per_bkt = next(d for d in data if d["MetricName"] == "TaggingOperationsRead")
        assert per_bkt["Dimensions"] == [
            {"Name": "SourceBucket", "Value": "bkt"},
            {"Name": "Env", "Value": "prod"},
        ]
        run_d = next(d for d in data if d["MetricName"] == "DisabledBuckets")
        assert run_d["Dimensions"] == [{"Name": "Env", "Value": "prod"}]

    def test_all_datums_have_count_unit(self):
        """All datums from _build_metric_data have Unit='Count'."""
        rr = _run(_bm("x"), _bm("y"))
        data = _build_metric_data(rr)
        for d in data:
            assert d["Unit"] == "Count"

    def test_empty_extra_dimensions_equiv_to_none(self):
        """Passing {} extra_dimensions is equivalent to passing None."""
        rr = _run(_bm("b"))
        data_none = _build_metric_data(rr, extra_dimensions=None)
        data_empty = _build_metric_data(rr, extra_dimensions={})
        assert data_none == data_empty


# ---------------------------------------------------------------------------
# Property: all metric names and values are consistent for any RunResult shape
# Feature: cloudwatch-metrics, Property: metric_data_completeness
# Requirements: 2.1–2.6, 3.1
# ---------------------------------------------------------------------------


@given(
    bucket_count=st.integers(min_value=0, max_value=20),
    ops=st.integers(min_value=0, max_value=1000),
    matched=st.integers(min_value=0, max_value=1000),
    submitted=st.integers(min_value=0, max_value=1),
    errored=st.booleans(),
    disabled=st.integers(min_value=0, max_value=500),
)
@settings(max_examples=100)
def test_build_metric_data_completeness(
    bucket_count, ops, matched, submitted, errored, disabled
):
    """For any RunResult, _build_metric_data returns the expected datum count.

    A bucket with activity contributes 4 datums; an idle bucket contributes
    only ``BucketErrors`` (Req 2.6, 2.7, 2.8). The run-level datum is always
    present. Every datum carries Unit='Count' and a float value.

    # Feature: cloudwatch-metrics, Property: metric_data_completeness
    """
    buckets = [
        _bm(f"bucket-{i:04d}", ops=ops, matched=matched,
            submitted=submitted, errored=errored)
        for i in range(bucket_count)
    ]
    rr = RunResult(buckets=buckets, disabled_buckets=disabled)
    data = _build_metric_data(rr)
    per_bucket_datums = 4 if (ops or matched or submitted) else 1
    assert len(data) == bucket_count * per_bucket_datums + _RUN_LEVEL_DATUMS
    for d in data:
        assert d["Unit"] == "Count"
        assert isinstance(d["Value"], float)
