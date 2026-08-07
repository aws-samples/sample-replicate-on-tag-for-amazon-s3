# Cost Detail

A worked example for the cost drivers listed in [Cost](../README.md#cost).

## Example workload

Approximate monthly cost in `us-east-1`, pricing as of 2026-06-24, for one source bucket:

- 10,000 objects tagged per day, about 300,000 per month.
- 20% of daily tagging straddles a check boundary, about 36 jobs per month.
- Lambda runs about 28s on a busy journal.
- `HeadObject` rows assume `CompletionNotificationEmail` is set and replication completes within 10 minutes at p90, so most objects resolve on their first eligible check.

Every row is per source bucket, with two exceptions. Lambda is one invocation per run covering all buckets, and one of the five CloudWatch metrics is run-level. Adding a bucket adds four metrics, not five.

### Checking every 15 minutes

`CheckFrequencyMinutes = 15`, about 2,880 runs per month.

| Component | Basis | Monthly |
|---|---|---|
| S3 Batch Operations — jobs | ~36 jobs × $0.25 | ~$9 |
| S3 Batch Operations — objects | 0.3M × $1.00 / million | ~$0.30 |
| Athena | well under 1 GB/month scanned | ~$0.05 |
| Lambda | ~2,880 runs × ~28s (arm64, 2 GB) | ~$2.15 |
| `HeadObject` checks (completion tracking, optional) | ~0.345M checks × $0.0004/1,000 | ~$0.14 |
| S3 State Bucket | manifests + results, expired after 30 days | ~$1 |
| CloudWatch metrics (optional) | 5 metrics × $0.30, activity in most hours | ~$1.50 |
| **Total** | | **~$14** |

### Checking hourly

`CheckFrequencyMinutes = 60`, about 730 runs per month.

| Component | Basis | Monthly |
|---|---|---|
| S3 Batch Operations — jobs | ~36 jobs × $0.25 | ~$9 |
| S3 Batch Operations — objects | 0.3M × $1.00 / million | ~$0.30 |
| Athena | well under 1 GB/month scanned | ~$0.05 |
| Lambda | ~730 runs × ~28s (arm64, 2 GB) | ~$0.55 |
| `HeadObject` checks (completion tracking, optional) | ~0.306M checks × $0.0004/1,000 | ~$0.12 |
| S3 State Bucket | manifests + results, expired after 30 days | ~$1 |
| CloudWatch metrics (optional) | 5 metrics × $0.30, activity in most hours | ~$1.50 |
| **Total** | | **~$13** |

## What the two intervals show

The job charge dominates and does not change with `CheckFrequencyMinutes`. Only Lambda scales with run count, which is why quadrupling the run rate adds about $1.60 a month at this volume.

## CloudWatch metric charges

Both tables assume a bucket active in most hours. A bucket that sees occasional bursts pays closer to $0.65, because only `BucketErrors` and the run-level metric are published on every run.

CloudWatch bills each custom metric for every hour in which a data point is sent, per unique metric-and-dimension combination. Publishing a flat zero series would therefore cost the same as publishing real activity, so the three per-bucket activity metrics are withheld for an idle bucket. See [Interpreting missing metric data](monitoring.md#interpreting-missing-metric-data) for what a missing data point means.

Log ingestion for the structured JSON logs applies whether or not `MetricsNamespace` is set.

## Not included

S3 Replication charges, meaning PUT requests and cross-Region data transfer at the destination. S3 Metadata journal table write and storage charges. Both are prerequisites of this Solution rather than costs it introduces.
