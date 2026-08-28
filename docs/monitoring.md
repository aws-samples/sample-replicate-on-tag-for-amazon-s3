# Monitoring Reference

Reference detail for the log entries, metrics, and job-failure diagnosis behind [Monitoring](../README.md#monitoring). That section covers what you set up: enabling metrics, the auto-disable circuit breaker and its recovery step, and the email alerts. This page is the lookup for what you then see.

## Log entries

The Solution emits structured JSON log entries. All entries include a `timestamp` field in ISO 8601 format. Object keys are never logged; error messages reference a SHA-256 fingerprint instead.

| Event | When emitted |
|---|---|
| `interval_summary` | Once per run: `Tagging_Operations`, `Matched_Objects`, `Batch_Replication_Job_submissions`, `duplicate_records_discarded` |
| `job_submitted` | Each successful Batch Operations job: `job_id`, `source_bucket` |
| `error` | Per-bucket error or skip: `component`, `bucket`, `cause` |
| `audit` | Security-critical mutations and bounded-processing decisions. One `action` field per entry, listed below |
| `reinvocation_triggered` | A capped run that progressed triggers the next run immediately: `chain_position` |
| `reinvocation_chain_limit_reached` | `ReinvocationChainLimit` reached with backlog remaining: `chain_limit`, `depth` |
| `deleted_versions_excluded` | When versions are filtered: `excluded_count` |
| `archived_objects_excluded` | When objects in an archived storage class are excluded: `excluded_count`, `by_storage_class` |
| `manifest_strategy_selected` | The manifest format chosen for a run: `bucket`, `preflight_count`, `manifest_format`, `generation_mode` |
| `bucket_disabled` | A bucket's `disabled` flag is set after repeated failures: `source_bucket`, `cause`, `recovery` |
| `submission_failure_permanent` | `CreateJob` is rejected before the request is sent: `source_bucket`, `operation`, `cause` |
| `journal_unavailable` | A bucket's S3 Metadata journal table or namespace does not exist: `source_bucket`, `cause`, `recovery` |
| `completion_report_missing` | A terminal job's outcomes cannot be confirmed from its completion report: `source_bucket`, `replication_config_id`, `job_id`, `reason`, `cause`. `reason` is `missing` when no report appeared within an hour of the job terminating, or `present but unconsumed` when a report has existed for 48 hours without the Solution recording the job as processed. `replication_config_id` carries the job ID, because alert suppression is per job: a bucket can have several jobs outstanding, and suppressing one must not hide another |

A run that fails before the handler executes emits none of these. An init failure such as `Runtime.ImportModuleError` from a code package with the wrong layout appears only as a Lambda runtime error in the log group, with no structured entry, so the absence of `interval_summary` is the signal. `ReplicationLambdaErrorAlarm` covers that case (see [Run failure alerts](../README.md#run-failure-alerts)).

### Journal read budget errors

`JournalReadRowCap` covers the whole journal read: the `JournalLookbackSeconds` window below the Solution's journal position that each run re-scans for late-arriving records, and the new rows above it. Four `error` entries report how a run divided that budget. Each carries the ordinary `component`, `bucket`, and `cause` fields; the `cause` text distinguishes them.

| `cause` begins | Meaning | What to do |
|---|---|---|
| `Lookback tail shortened to fit the row budget` | The re-scan window held more rows than its 80% share of the cap, so the read's lower bound was raised and the oldest part of the window was skipped this run. Names the row count, the allowance, the bound used, and the configured lookback | Nothing, if occasional. If sustained, the bucket has a backlog: see the `JournalTailShortened` metric below |
| `Lookback-tail row count failed` | The Athena query sizing the re-scan window failed, so the run assumed the window was exactly at its allowance. The run still progressed | Nothing. Transient Athena failures clear on the next run |
| `Lookback-tail floor lookup failed` | The Athena query locating the raised lower bound failed, so the bucket was skipped for the interval with its checkpoint unchanged. Neither reading the window unbounded nor skipping it whole is safe, so the run declines to read. Raises `BucketErrors` | Nothing if isolated: the next interval picks up the whole backlog. If it repeats, the bucket is not draining — investigate Athena |
| `Row-cap boundary did not advance` | A regression tripwire. The bucket was skipped for the interval. The Solution's read window is constructed so this cannot happen | Report it. No configuration change fixes it |

A shortened re-scan window can lose a late-arriving journal record, not only delay
it. The scope is worth being precise about, because it is narrow.

The window is truncated from its oldest end, so the rows skipped are the oldest in
it and the rows nearest the Solution's journal position — the likeliest genuine
late arrivals, given S3 Metadata delivers within about 15 minutes against a default
two-hour window — are still read. Only a record in the skipped part is at risk, and
only if it was a genuine late arrival: a record already submitted on an earlier run
is suppressed by the Solution's own tracking and loses nothing.

Such a record is unreachable once the Solution's journal position passes its
timestamp plus `JournalLookbackSeconds`. For the oldest rows in the window that is
the very next advance, so treat the skipped part as a last chance rather than a
delay. The lower bound is recomputed each run, so a bucket that stops shortening
resumes re-scanning its whole window immediately.

Raise `JournalReadRowCap`, within the ceiling for the configured `LambdaMemoryMB`,
if `JournalTailShortened` is sustained. A higher cap gives the window a larger
share, which is what shrinks the skipped part toward nothing.

## Audit actions

Every `audit` log entry carries an `action` naming what happened, a `source_bucket`, and action-specific fields.

| `audit` action | When emitted | Fields |
|---|---|---|
| `lease_acquired` | A run takes the per-bucket processing lease | `lease_id`, `candidate_max_watermark`, `lease_status` |
| `lease_released` | The lease is released and the checkpoint written | `checkpoint_from`, `checkpoint_to`, `checkpoint_advanced`, `submitted_operations` |
| `batch_job_created` | A Batch Operations job is created | `job_id`, `config_id`, `batch_operations_role_arn`, `object_count` |
| `journal_read_capped` | A run hits `JournalReadRowCap` and bounds itself | `row_cap`, `until_timestamp`, `since_timestamp`, `tail_rows`, `new_row_budget`, `tail_shortened` |
| `row_cap_overshoot` | A capped run read past the cap because many operations shared the boundary timestamp | `row_cap`, `rows_read`, `matched`, `overshoot_rows` |
| `batch_job_failure_readmit` | A failed job's operations are readmitted for reprocessing | `job_id`, `config_id`, `watermark_low`, `watermark_high` |
| `bucket_disabled` | A bucket's `disabled` flag is set after repeated failures | `reason`, `state_key` |
| `journal_unavailable` | A bucket's journal read fails because the journal table or namespace does not exist | `cause` |
| `completion_report_published` | An SNS completion report publishes successfully | `item_count` |
| `submission_deferred_job_in_flight` | A bucket is skipped because its outstanding Batch Operations job count has reached `MaxConcurrentJobsPerBucket` | `outstanding_count`, `limit`, `job_id`, `job_status`, `job_age_seconds` (the last three describe the oldest outstanding job) |

## CloudWatch metrics

Set `MetricsNamespace` to publish these after each run. The namespace is the name you look for in the CloudWatch console and needs no setup beyond the parameter. `MetricsDeploymentId`, if set, adds a `Deployment` dimension to every metric below, so multiple stacks can publish to one namespace without colliding.

| Metric | Dimension | Published | Description |
|---|---|---|---|
| `TaggingOperationsRead` | `SourceBucket` | Only when the bucket had activity | Distinct tagging operations matched |
| `MatchedObjects` | `SourceBucket` | Only when the bucket had activity | Objects matched against replication rules |
| `BatchJobsSubmitted` | `SourceBucket` | Only when the bucket had activity | 0 or 1, whether a job was submitted for the bucket this run. Never more than 1, because one job covers every matched object across all of the bucket's tag-scoped rules |
| `ArchivedObjectsExcluded` | `SourceBucket` | Only when at least 1 | Tagged objects excluded because they are in an archived storage class (see [Objects That Are Not Replicated](../README.md#objects-that-are-not-replicated)) |
| `SubmissionDeferred` | `SourceBucket` | Only when it happened | 1 when the bucket was skipped because its outstanding Batch Operations job count had reached `MaxConcurrentJobsPerBucket` (see [Bounded Concurrent Jobs per Bucket](../README.md#bounded-concurrent-jobs-per-bucket)). Not an error, and no work is lost. A sustained run of these means the bucket's replication throughput sets the pace, not this Solution |
| `JournalTailShortened` | `SourceBucket` | Only when it happened | 1 when the run raised its journal-read lower bound above the configured `JournalLookbackSeconds` window start, because the window held more rows than its share of `JournalReadRowCap`. The run read every row it could afford and made progress, so nothing above the Solution's journal position is affected. The oldest part of the re-scan window was skipped, though, so a journal record delivered late into that part can be missed rather than merely delayed — see below. A sustained run of these means either a backlog worth investigating or a `JournalReadRowCap` too low for the workload |
| `BucketErrors` | `SourceBucket` | Every run, including the 0 | 1 if the bucket was skipped due to an error, 0 otherwise |
| `DisabledBuckets` | _(none)_ | Every run, including the 0 | How many buckets were skipped because their `disabled` flag is set |

## Interpreting missing metric data

`TaggingOperationsRead`, `MatchedObjects`, and `BatchJobsSubmitted` are published as a group, so a run with any activity carries any genuine zeros among them. They are withheld for an idle bucket to avoid paying for a flat zero series (see [CloudWatch metric charges](cost.md#cloudwatch-metric-charges)). A missing data point therefore carries meaning:

| Observation | Meaning |
|---|---|
| No activity metrics, `BucketErrors` present | Bucket was processed and had nothing to do |
| `BucketErrors` present and 1 | Bucket errored, either during processing or before it started because its state object could not be read |
| No `BucketErrors` for a bucket | Bucket is disabled, absent from `SourceBucketNames`, or the run never reached it |

Alarm on the last row with `treatMissingData: breaching` to catch a bucket that silently stopped being processed. Alarm on `ArchivedObjectsExcluded` with `treatMissingData: notBreaching`, since it is published only when at least one object was excluded.

For the auto-disable case, alarm on `DisabledBuckets >= 1` instead. It is a plain threshold on a metric published every run, so it does not depend on missing-data handling, and unlike the disable notification email it reports the condition for as long as it persists rather than once when it happens. It is run-level, so it tells you how many buckets are disabled. Which ones is in the notification email, the per-run `error` log entry, and the `disabled` flag in each bucket's state object at `s3://<state-bucket>/state/<bucket-name>.json`.

## Diagnosing task failures

A Batch Operations job can reach `Complete` while individual tasks in it failed, and the job's status does not reveal that. Every task failure in a job's completion report raises an `error` log entry naming the error code, how many tasks carried it, and the message S3 reported for it. Two codes get cause-specific guidance, described below; the rest are reported with the service's own wording, because a code this Solution does not recognize still means the object was not replicated.

The error code alone is often not enough to identify a cause. An object in an archived storage class is reported only as `SrcObjectNotEligible` with the message "Object is not eligible for replication", which names no storage class and also covers other conditions, so the reported message is worth reading. See [Objects That Are Not Replicated](../README.md#objects-that-are-not-replicated).

### Permission failures

Two job failures are permission gaps rather than replication problems. Both point at the Batch Operations role the stack creates, so both indicate a defect in the deployment rather than a condition in your account. Compare the `BatchOperationsRoleArn` output's policy against [Batch Operations job role](permissions.md#batch-operations-job-role).

**The job fails with `Reading the manifest is forbidden`.** It processed no objects. Batch Operations reads the manifest as the job's own role, and the stack grants that role `s3:GetObject` on the State Bucket's `manifests/` prefix, so this reason means the created role's grant did not apply.

**Every task fails with `InitiateReplicationNotPermitted`.** The role is missing `s3:InitiateReplication` on the source bucket. The stack grants that action on every bucket in `SourceBucketNames`, so confirm the bucket is named there and that the role named by the `BatchOperationsRoleArn` output carries the action for it.

Job status alone does not identify the second case. A job of fewer than 1,000 objects reaches `Complete`, which reports that every object in the manifest was attempted, not that any succeeded. At 1,000 objects or more, the [task-failure threshold](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-job-status.html#batch-ops-job-status-failure) trips at a 50 percent failure rate and the job reaches `Failed`. Two log signals cover both job sizes.

| Signal | When | Always emitted |
|---|---|---|
| An `error` entry naming the job, its `NumberOfTasksSucceeded` and `NumberOfTasksFailed`, and pointing at the completion-report entry for the error code | A job reached `Complete` with zero tasks succeeded and at least one failed | Yes |
| An `error` entry naming the actual task `ErrorCode` and how many rows carried it | The job's completion report contains any non-empty `ErrorCode`, one entry per distinct code | Yes |

The first signal is deliberately narrow: it requires zero successes, because `NumberOfTasksFailed` is not reliable on its own at high object counts. A job where one task succeeded and the rest failed on a permission error therefore does not raise it, and the second signal is what catches that case.

The first signal also counts the job as a failure, so a bucket whose every task fails is disabled after `MaxBatchJobFailures` runs rather than resubmitting indefinitely. The second is diagnostic only and does not move the counter.
