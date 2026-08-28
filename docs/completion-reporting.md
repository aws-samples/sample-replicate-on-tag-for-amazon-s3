# Completion Reporting

This page describes the Amazon Simple Storage Service (Amazon S3) Batch Operations completion reports and Amazon Simple Notification Service (Amazon SNS) messages available when `CompletionNotificationEmail` is set. Leave that parameter empty to keep Batch Operations completion-report diagnostics without email notifications.

Amazon S3 documents that a Batch Replication task's status depends on the parent object's replication status and its annotation replication status. The Solution resolves each tracked object from that per-task result in the completion report. See [S3 Batch Replication considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-batch-replication-batch.html).

## Report readiness

The Solution processes a terminal job's report only after its top-level `job-<job-id>/manifest.json` is available and valid. The manifest is the readiness boundary because it names the result files and their Message-Digest Algorithm 5 (MD5) checksums. For a job with one or more invoked tasks, the Solution requires all of the following before it changes tracking state:

- The manifest parses and each declared result object is readable.
- Every result object's body matches its declared MD5 checksum.
- Every result row parses, contains a unique `(bucket, key, version ID)` identity, and the total row count equals `NumberOfTasksSucceeded + NumberOfTasksFailed` from `DescribeJob`.

A missing, partial, malformed, duplicate, or checksum-invalid report leaves the job unprocessed for a later interval. A terminal job with zero invoked tasks needs no report because S3 does not create one in that case.

The scheduled completion-report check alerts when the top-level manifest has not appeared one hour after a job finishes. It checks report availability only; the main Lambda validates and resolves the report.

## Outcomes

A ready report row resolves its tracked object in the same conditional state write that records the job as processed. Newly processed rows have one of these outcomes:

| Outcome | Report row meaning | Email treatment |
|---|---|---|
| `COMPLETE` | `task_status` is `succeeded` | Non-actionable |
| `FAILED` | `task_status` is `failed`, with or without an error code | Actionable |
| `UNKNOWN` | `task_status` is absent or unrecognized | Actionable |

The grouped email is `format_version: 3` and carries outcome counts per group. Per-task error codes and result messages are in the Batch Operations completion-report comma-separated values (CSV) file, and the Solution logs a diagnostic entry for each recognized failure code.

## Report group fields

The report body is a `groups` list. Objects are aggregated by `(source_bucket, matched_rules, destinations)` rather than listed individually, so the shared values appear once per group.

| Field | Meaning |
|---|---|
| `source_bucket` | Bucket containing the source objects |
| `matched_rules` | IDs of replication rules that matched the objects |
| `destinations` | Destination buckets associated with those rules |
| `count` | Number of objects in the group |
| `outcome_counts` | Aggregate outcomes in the group |
| `tagged_at_range` | `[earliest, latest]` tag timestamps for objects that have them |
| `last_modified_range` | `[earliest, latest]` last-modified timestamps for objects that have them |

`matched_rules` and `destinations` are empty lists when the Solution has no routing data for the objects. A timestamp range is omitted when no object in the group has that timestamp.

Object keys and version IDs are not included in the email. Use the completion-report CSV under `completion-reports/` in the State Bucket to identify an individual object.

### Example report

A 563-object all-success report to one destination produces:

```json
{
  "summary": "my-source-bucket-eu-west-1: 563 objects replicated successfully. No action needed. No replication jobs remain outstanding.",
  "format_version": 3,
  "source_bucket": "my-source-bucket-eu-west-1",
  "item_count": 563,
  "outstanding_jobs": 0,
  "submission_deferred": false,
  "outcome_counts": {"COMPLETE": 563},
  "groups": [
    {
      "source_bucket": "my-source-bucket-eu-west-1",
      "matched_rules": ["replicate-to-us-west-2"],
      "destinations": ["my-destination-bucket-us-west-2"],
      "count": 563,
      "outcome_counts": {"COMPLETE": 563},
      "tagged_at_range": ["2026-08-08T21:24:28.335000+00:00", "2026-08-08T21:24:30.737000+00:00"],
      "last_modified_range": ["2025-11-18T20:13:09+00:00", "2025-11-18T21:15:16+00:00"]
    }
  ]
}
```

## Multi-destination results

A completion-report row is the aggregate result for one Batch Replication task. A group can name several destinations, but `FAILED` and `UNKNOWN` do not identify which destination caused the aggregate result. Diagnose a destination-specific failure from the S3 replication failure event or by inspecting the destination. S3 documents that destination-side replication failure reasons appear in the completion report; see [Amazon S3 replication failure reasons](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-metrics-events.html#replication-failure-codes).

## Delivery and tracking state

One SNS message per source bucket covers the report groups that fit within the Amazon SNS 256 KB message limit. Larger reports split between groups and never split a group. Every message from one split report carries the same values for `outstanding_jobs` and `submission_deferred`: the messages are one logical report split to fit the size limit, and they carry no ordering.

### Outstanding work fields

| Field | Meaning |
|---|---|
| `outstanding_jobs` | Batch Operations jobs outstanding for the source bucket when the report was built, including any the run just submitted. `null` when the count is unknown |
| `submission_deferred` | Whether the most recent run skipped this bucket because its outstanding job count had reached `MaxConcurrentJobsPerBucket` |

`outstanding_jobs` is `null` when the run did not get as far as checking the bucket's jobs, or skipped the bucket because its `disabled` flag is set. Treat `null` as "still in progress, or not known", never as zero: a bucket is disabled precisely because its jobs kept failing. The key is always present, so `null` and `0` are distinguishable. 

`format_version: 2`'s `outstanding` field is removed, and nothing takes its name. It counted objects still awaiting a terminal answer, and `outstanding: 0` was the single field that answered "has everything I tagged arrived?".

No count of stored completion items can carry that meaning. An object enters tracking only once its job's completion report has been read, so by the time it is counted the question is already settled for it. `outstanding_jobs` answers it instead, at the level where the work is actually pending.

A subscriber migrating from `format_version: 2` therefore cannot repoint `outstanding` at anything: no field replaces it one-for-one. Use the two-field test below instead.

Two kinds of pending work remain invisible to both counts:

| Pending work | Why it is not counted |
|---|---|
| Tagging read from the journal but not yet submitted | No completion item is created until a job's report is read, and no job exists yet |
| Tagging that has not been read from the journal | The Solution holds no count of it |

`submission_deferred: true` covers the third case that used to appear here: tagging waiting because the bucket is at its concurrency limit. See [Bounded Concurrent Jobs per Bucket](../README.md#bounded-concurrent-jobs-per-bucket).

Both fields are per source bucket, and neither is attributed to a tag, rule, or destination. One job covers every matched object across all of a bucket's tag-scoped rules, so no job belongs to a single rule. A per-tag count would need one job per rule, which multiplies the per-job charge by the number of rules and is why the Solution submits one job per bucket instead.

To answer "has everything I tagged replicated", use `outstanding_jobs: 0` together with `submission_deferred: false`. Both are required, and `outstanding_jobs` must be `0` rather than `null`. The summary sentence applies the same rule: it says `No replication jobs remain outstanding.` only when the count is a known zero. For the objects themselves, use the Batch Operations completion reports under `completion-reports/` on the State Bucket.

Email delivery is at least once. The Solution deletes covered tracking state only after Amazon SNS accepts the message. If publishing succeeds but the subsequent state deletion fails, a later interval can deliver the same report again. Subscribers must tolerate duplicate messages.

The summary and subject flag `FAILED` and `UNKNOWN` as action needed. `COMPLETE` is non-actionable.

## Permissions

The Batch Operations role created by the stack has the `s3:PutObject` permission required to write the report. See [Completion reporting](permissions.md#completion-reporting).
