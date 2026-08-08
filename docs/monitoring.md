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
| `completion_report_missing` | A terminal job's completion report never appeared: `source_bucket`, `replication_config_id`, `job_id`, `cause` |

A run that fails before the handler executes emits none of these. An init failure such as `Runtime.ImportModuleError` from a code package with the wrong layout appears only as a Lambda runtime error in the log group, with no structured entry, so the absence of `interval_summary` is the signal. `ReplicationLambdaErrorAlarm` covers that case (see [Run failure alerts](../README.md#run-failure-alerts)).

## Audit actions

Every `audit` log entry carries an `action` naming what happened, a `source_bucket`, and action-specific fields.

| `audit` action | When emitted | Fields |
|---|---|---|
| `lease_acquired` | A run takes the per-bucket processing lease | `lease_id`, `candidate_max_watermark`, `lease_status` |
| `lease_released` | The lease is released and the checkpoint written | `checkpoint_from`, `checkpoint_to`, `checkpoint_advanced`, `submitted_operations` |
| `batch_job_created` | A Batch Operations job is created | `job_id`, `config_id`, `batch_operations_role_arn`, `object_count` |
| `journal_read_capped` | A run hits `JournalReadRowCap` and bounds itself | `row_cap`, `until_timestamp`, `since_timestamp` |
| `row_cap_overshoot` | A capped run read past the cap because many operations shared the boundary timestamp | `row_cap`, `rows_read`, `matched`, `overshoot_rows` |
| `batch_job_failure_readmit` | A failed job's operations are readmitted for reprocessing | `job_id`, `config_id`, `watermark_low`, `watermark_high` |
| `bucket_disabled` | A bucket's `disabled` flag is set after repeated failures | `reason`, `config_key` |
| `completion_report_published` | An SNS completion report publishes successfully | `item_count` |
| `completion_item_expired` | A tracked object passed `CompletionItemTtlHours` and was abandoned | `job_ids`, `age_seconds`, `ttl_seconds` |

## CloudWatch metrics

Set `MetricsNamespace` to publish these after each run. The namespace is the name you look for in the CloudWatch console and needs no setup beyond the parameter. `MetricsDeploymentId`, if set, adds a `Deployment` dimension to every metric below, so multiple stacks can publish to one namespace without colliding.

| Metric | Dimension | Published | Description |
|---|---|---|---|
| `TaggingOperationsRead` | `SourceBucket` | Only when the bucket had activity | Distinct tagging operations matched |
| `MatchedObjects` | `SourceBucket` | Only when the bucket had activity | Objects matched against replication rules |
| `BatchJobsSubmitted` | `SourceBucket` | Only when the bucket had activity | 0 or 1, whether a job was submitted for the bucket this run. Never more than 1, because one job covers every matched object across all of the bucket's tag-scoped rules |
| `ArchivedObjectsExcluded` | `SourceBucket` | Only when at least 1 | Tagged objects excluded because they are in an archived storage class (see [Objects That Are Not Replicated](../README.md#objects-that-are-not-replicated)) |
| `BucketErrors` | `SourceBucket` | Every run, including the 0 | 1 if the bucket was skipped due to an error, 0 otherwise |
| `DisabledBuckets` | _(none)_ | Every run, including the 0 | How many buckets were skipped because their `disabled` flag is set |

## Interpreting missing metric data

`TaggingOperationsRead`, `MatchedObjects`, and `BatchJobsSubmitted` are published as a group, so a run with any activity carries any genuine zeros among them. They are withheld for an idle bucket to avoid paying for a flat zero series (see [CloudWatch metric charges](cost.md#cloudwatch-metric-charges)). A missing data point therefore carries meaning:

| Observation | Meaning |
|---|---|
| No activity metrics, `BucketErrors` present | Bucket was processed and had nothing to do |
| `BucketErrors` present and 1 | Bucket was processed and errored |
| No `BucketErrors` for a bucket | Bucket is disabled, absent from `SourceBucketNames`, or the run never reached it |

Alarm on the last row with `treatMissingData: breaching` to catch a bucket that silently stopped being processed. Alarm on `ArchivedObjectsExcluded` with `treatMissingData: notBreaching`, since it is published only when at least one object was excluded.

For the auto-disable case, alarm on `DisabledBuckets >= 1` instead. It is a plain threshold on a metric published every run, so it does not depend on missing-data handling, and unlike the disable notification email it reports the condition for as long as it persists rather than once when it happens. It is run-level, so it tells you how many buckets are disabled. Which ones is in the notification email, the per-run `error` log entry, and the `disabled` flags in `solution-config.json`.

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
