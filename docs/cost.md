# Cost Detail

A worked example for the cost drivers listed in [Cost](../README.md#cost).

## Example workload

Approximate monthly cost in `us-east-1`, pricing as of 2026-06-24, for one source bucket:

- 10,000 objects tagged per day, about 300,000 per month.
- 20% of daily tagging straddles a check boundary, about 36 jobs per month.
- Lambda runs about 28s on a busy journal.
- The SNS row assumes `CompletionNotificationEmail` is set. One report covers a bucket's resolved outcomes for a run, about one per job at this volume.
- Athena rows assume the default `JournalLookbackSeconds` of 7,200. Every run re-scans the whole lookback window, so bytes scanned per run scale with that parameter. At this tagging volume the window stays small enough that the Athena charge is rounding error either way.

Every row is per source bucket, with two exceptions. Lambda is one invocation per run covering all buckets, and one of the six CloudWatch metrics is run-level. Adding a bucket adds five metrics, not six.

### Checking every 15 minutes

`CheckFrequencyMinutes = 15`, about 2,880 runs per month.

| Component | Basis | Monthly |
|---|---|---|
| S3 Batch Operations — jobs | ~36 jobs × $0.25 | ~$9 |
| S3 Batch Operations — objects | 0.3M × $1.00 / million | ~$0.30 |
| Athena | well under 1 GB/month scanned | ~$0.05 |
| Lambda | ~2,880 runs × ~28s (arm64, 2 GB) | ~$2.15 |
| S3 State Bucket | manifests + completion reports, expired after 30 days | ~$1 |
| SNS completion reports (optional) | ~36 publishes and email deliveries | <$0.01 |
| CloudWatch metrics (optional) | 6 metrics × $0.30, activity in most hours | ~$1.80 |
| **Total** | | **~$14.30** |

### Checking hourly

`CheckFrequencyMinutes = 60`, about 730 runs per month.

| Component | Basis | Monthly |
|---|---|---|
| S3 Batch Operations — jobs | ~36 jobs × $0.25 | ~$9 |
| S3 Batch Operations — objects | 0.3M × $1.00 / million | ~$0.30 |
| Athena | well under 1 GB/month scanned | ~$0.05 |
| Lambda | ~730 runs × ~28s (arm64, 2 GB) | ~$0.55 |
| S3 State Bucket | manifests + completion reports, expired after 30 days | ~$1 |
| SNS completion reports (optional) | ~36 publishes and email deliveries | <$0.01 |
| CloudWatch metrics (optional) | 6 metrics × $0.30, activity in most hours | ~$1.80 |
| **Total** | | **~$12.70** |

## What the two intervals show

The job charge dominates and does not change with `CheckFrequencyMinutes`. Only Lambda scales with run count, which is why quadrupling the run rate adds about $1.60 a month at this volume.

## The per-job charge and concurrency

The job charge is per job, so it is set by how many jobs the Solution submits rather than by how long they run. At most one job is submitted per source bucket per run, whatever `MaxConcurrentJobsPerBucket` is set to, so that parameter does not change how many jobs are submitted over a month.

What it does bound is how many jobs may be *outstanding* at once for one bucket, and therefore the ceiling on concurrent per-job charges for that bucket. At the default of 3, a bucket whose destination is slow accumulates at most three jobs before the Solution stops submitting and waits. Raising the limit raises that ceiling; lowering it to 1 serializes the bucket, which costs throughput rather than money.

The bound matters most where a job's duration is set by replication throughput rather than object count. Without it, a bandwidth-bound bucket submitting every 15 minutes would accumulate jobs at roughly four an hour for as long as its destination stayed behind, each one billable. See [Bounded Concurrent Jobs per Bucket](../README.md#bounded-concurrent-jobs-per-bucket).

## Athena queries per run

Athena is billed per query on bytes scanned, so the query count per run matters as well as the window size. The queries governing the journal read window, per source bucket per run, are:

| Query | Issued |
|---|---|
| Journal read | Every run that reaches the journal |
| Row-cap boundary | Every run that reaches the journal. If this query fails, the bucket is skipped for that interval, so the journal read is not issued either |
| Lookback re-scan window row count | Every run, except a bucket's first run and any run with `JournalLookbackSeconds` set to 0 |
| Lookback re-scan window lower bound | When that window holds more rows than its share of `JournalReadRowCap`, or when the row count above could not be established. If this query itself fails, the bucket is skipped for that interval, so the journal read is not issued either |

A run also issues a preflight count and a permanent-delete scan, which this table omits because neither is affected by the window arithmetic.

The last two rows size and bound the part of the read below the Solution's journal position. Both return a single row whatever the window holds, so neither scales with the window's size the way the journal read does. A bucket's first run has nothing below its position and a zero lookback has no window, so both are skipped rather than issued and discarded. A bucket not in a backlog pays the third query and never the fourth.

## CloudWatch metric charges

Both tables assume a bucket active in most hours. A bucket that sees occasional bursts pays closer to $0.65, because only `BucketErrors` and the run-level metric are published on every run.

CloudWatch bills each custom metric for every hour in which a data point is sent, per unique metric-and-dimension combination. Publishing a flat zero series would therefore cost the same as publishing real activity, so the three per-bucket activity metrics are withheld for an idle bucket, and `ArchivedObjectsExcluded` is withheld unless at least one object was excluded. See [Interpreting missing metric data](monitoring.md#interpreting-missing-metric-data) for what a missing data point means.

Log ingestion for the structured JSON logs applies whether or not `MetricsNamespace` is set.

## Not included

S3 Replication charges, meaning PUT requests and cross-Region data transfer at the destination. S3 Metadata journal table write and storage charges. Both are prerequisites of this Solution rather than costs it introduces.
