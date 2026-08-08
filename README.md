# Automatic replication after tagging for Amazon S3

Automates Amazon S3 replication for objects tagged after creation. S3 Replication evaluates tag-scoped rules only at object-creation time, so adding a tag to an object that already exists does not trigger it. This Solution runs on a recurring schedule, reads new tagging operations from the S3 Metadata journal, and submits S3 Batch Operations replication jobs for matched objects.

The Solution operates entirely on the source side. It never accesses the destination account or Region.

[Use Cases](docs/use-cases.md) covers two patterns: gating replication on a post-upload check such as a GuardDuty malware scan, and copying a selected dataset to another Region. Either can fan out to several destination buckets based on the tag's value. To apply a matching tag across a large set of objects that already exist, see [Tagging Objects at Scale](docs/tagging-at-scale.md).

> **This is sample code.** It is provided for demonstration and educational purposes, and is not intended for production use without your own testing, security review, and validation against your requirements and environment. Review [Required AWS Permissions](docs/permissions.md), the [Hardening Options](docs/hardening.md) that are off by default, and the S3 Batch Operations cost implications of your check interval before deploying it into production.

## Documentation

This README is the operator reference. Detail lives alongside it:

| Document | Covers |
|---|---|
| [Deployment Guide](deploy/README.md) | Deployment walkthrough, the full parameter reference, IAM and KMS setup, multiple accounts and Regions |
| [Use Cases](docs/use-cases.md) | The patterns this Solution suits, and how tag values fan out to several destinations |
| [Tagging Objects at Scale](docs/tagging-at-scale.md) | Applying a matching tag across objects that already exist |
| [Backfilling After a Replication-Rule Change](docs/backfill.md) | The Athena query and Batch Operations recipe for a manual catch-up |
| [Monitoring Reference](docs/monitoring.md) | Audit actions, metric semantics and alarm recipes, task-failure diagnosis |
| [Completion Reporting](docs/completion-reporting.md) | Tracking mechanics, outcomes, and report entry fields |
| [Cost Detail](docs/cost.md) | A worked example, and what is excluded from it |
| [Required AWS Permissions](docs/permissions.md) | What each grant is for, the Batch Operations job role, Lake Formation |
| [Hardening Options](docs/hardening.md) | The three protections that are off by default |
| [Customer-Managed KMS Keys](docs/kms.md) | All three KMS parameters, key-policy grants, verification |
| [Testing](docs/testing.md) | The end-to-end test against real AWS resources |

## How It Works

One Lambda function, on a schedule, doing five things per source bucket:

1. Read the bucket's replication configuration and keep the tag-scoped rules.
2. Query the S3 Metadata journal through Athena for tagging operations since the last checkpoint.
3. Match those objects against the tag filters, and drop versions that have been permanently deleted.
4. Build one manifest in memory and submit one S3 Batch Operations replication job for the bucket.
5. Advance the checkpoint, but only once the job is submitted.

S3 performs the replication itself, using the bucket's own rules and destinations. This Solution decides which object versions to hand it.

## Prerequisites

Per source account and Region. Everything here is a condition on your account or your buckets; the IAM permissions you need to run the deployment itself are in [`deploy/README.md`](deploy/README.md#prerequisites).

- **Versioning enabled** on every source and destination bucket, as S3 replication requires regardless of this Solution.
- **An S3 replication configuration** on each source bucket with at least one tag-scoped rule. Rules with no tag filter are ignored, since S3 already applies those at upload.
- **A replication role** attached to each of those configurations, carrying the permissions S3 replication itself needs. The Solution requires nothing added to it: the stack creates its own role for the Batch Operations jobs it submits.
- **The S3 Metadata journal table** enabled on each source bucket.
- **The S3 Tables analytics-services integration** enabled in the Region, which creates the `s3tablescatalog` Glue catalog that Athena reads the journal through. One-time per account and Region, via S3 console → Table buckets → Enable integration. [`deploy/README.md`](deploy/README.md#prerequisites) has the `aws glue create-catalog` equivalent.
- **An active CloudTrail trail** capturing management events in the Region, required only if you set `AlarmEmail`. Batch Operations job-status events reach EventBridge through CloudTrail, so without a trail those alerts never fire. Replication itself is unaffected.
- **Customer-managed KMS keys**, optional. See [Customer-Managed KMS Keys](docs/kms.md).
- **VPC connectivity**, only when deploying the Lambda into a VPC. The subnets must route to S3, Athena, and CloudWatch (via gateway or interface VPC endpoints, or a NAT gateway). Leave the VPC parameters empty to deploy outside a VPC, which needs no additional networking setup.

## Getting Started

Complete the [Prerequisites](#prerequisites), then per source account and Region:

1. Download `template.yaml`, `package-<version>.zip`, and `package-<version>.zip.sha256` from the project's [Releases](https://github.com/aws-samples/sample-replicate-on-tag-for-amazon-s3/releases) page.
2. Verify the zip against its checksum: `shasum -a 256 -c package-<version>.zip.sha256` on macOS, or `sha256sum -c package-<version>.zip.sha256` on Linux.
3. Upload the zip to an S3 bucket in the same Region as the stack, keeping the version in the key. Its S3 URI, for example `s3://<your-code-bucket>/package-<version>.zip`, is the `CodeLocation` parameter.
4. Create the CloudFormation stack from `template.yaml`, set the parameters, and acknowledge `CAPABILITY_IAM`. Use the console, or the AWS CLI with the template staged in S3 (see [`deploy/README.md`](deploy/README.md#4-create-the-stack)).

One stack covers one account and one Region, and every bucket in `SourceBucketNames` must be co-regional with it. [Multiple Accounts and Regions](deploy/README.md#multiple-accounts-and-regions) covers what to repeat for each stack, how to aggregate metrics across them, and what to watch for when two stacks share an account and Region.

## Parameters

**Required** (no default, you must provide a value):

| CloudFormation parameter | Description |
|---|---|
| `CodeLocation` | S3 URI of the Lambda zip, e.g. `s3://<your-code-bucket>/package-<version>.zip` (bucket co-regional with the stack). Must change on upgrade (see [Upgrading](#upgrading)) |
| `SourceBucketNames` | Comma-separated source bucket names to monitor. At least one required |

The role that each S3 Batch Operations job runs as is not a parameter. The stack creates it, grants it `s3:InitiateReplication` on the buckets in `SourceBucketNames` plus manifest and completion-report access on the State Bucket, and publishes its ARN as the `BatchOperationsRoleArn` output. See [Required AWS Permissions](docs/permissions.md#batch-operations-job-role).

**The decisions worth making before you deploy.** Every other parameter has a default that works; the [full reference](deploy/README.md#parameter-reference) covers scale tuning, encryption, VPC placement, and Lake Formation.

| CloudFormation parameter | Default | Description |
|---|---|---|
| `CheckFrequencyMinutes` | 15 | How often the Solution runs, in minutes (15–1440). Because S3 Batch Operations charges per job, smaller values can raise cost, most of all when tagging activity is spread over time rather than arriving in a single batch |
| `CompletionNotificationEmail` | _(empty)_ | Email address for per-object replication tracking and completion email reports. The S3 Batch Operations completion report CSV is always written to the State Bucket; this parameter gates the per-object `x-amz-replication-status` tracking, the SNS email, and the report-missing alert only (see [Completion Reporting](#completion-reporting)) |
| `AlarmEmail` | _(empty)_ | Email address for run-failure, Batch Operations job-failure, and bucket-disabled alerts. Leave empty to disable alerting; no SNS topic is provisioned (see [Monitoring](#monitoring)) |
| `MetricsNamespace` | _(empty)_ | CloudWatch namespace to publish metrics under, any name you choose, e.g. `S3ReplicateOnTag`. CloudWatch creates it on first publish; names starting with `AWS/` are reserved. Leave empty to disable metrics |
| `KmsKeyArn` | _(empty)_ | Customer-managed KMS key ARN; leave empty for SSE-S3. See [Customer-Managed KMS Keys](docs/kms.md) |
| `JournalReadRowCap` | 500,000 | Max tagging operations processed in one interval, and the single scale knob bounding in-memory manifest size. Raise it only alongside `LambdaMemoryMB` (see [Configuration](#configuration)) |

The threshold parameters can also be changed on the Lambda function's environment variables without a stack update, which is useful for tuning against a live workload. [Tuning without a stack update](deploy/README.md#tuning-without-a-stack-update) lists the variables and the one case where this needs care.

## Configuration

The Solution is configured entirely through CloudFormation stack parameters. There is no file to author before deploying. On deploy, the stack writes an internal config object (`solution-config.json`) to the State Bucket that the Lambda reads at startup. The only supported manual edit to that object is clearing a bucket's `disabled` flag after an auto-disable (see [Monitoring](#monitoring)). Every other value in it is derived from stack parameters and is overwritten on the next deploy.

**Monitored buckets.** The bucket list comes from the `SourceBucketNames` parameter. To add or remove a bucket, update `SourceBucketNames` and deploy a stack update. This is the only supported method. The execution role's per-bucket permissions, and in Lake Formation accounts the journal grants, are scoped to this parameter at deploy time, so adding a bucket by hand-editing the config object would leave it without journal or bucket access.

**Replication rules.** Tag filters, key prefixes, and destinations are read from each bucket's existing replication configuration, not set on this Solution. The Solution acts only on **tag-scoped** rules; rules without a tag filter, whether prefix-only or unfiltered, are ignored. A bucket may have multiple tag-scoped rules. All of a bucket's tag-scoped rules are evaluated together, and every matched object across every rule goes into one Batch Operations job per bucket per interval.

**Journal read cap.** `JournalReadRowCap` (default 500,000) caps how many tagging operations one run processes, and is the Solution's single scale knob. A run that finds more than the cap processes the oldest `JournalReadRowCap` operations and picks up the rest automatically, so no tagging operation is ever dropped, only delayed. A capped run reinvokes itself immediately rather than waiting for the next scheduled interval, so a temporary burst clears faster than the schedule alone allows.

The default suits most workloads. Its ceiling for each `LambdaMemoryMB`, what happens when a value exceeds that ceiling, and the sustained tagging rate the cap supports are in the [deployment parameter reference](deploy/README.md#parameter-reference).

## Completion Reporting

Answers the question "did the objects I tagged actually arrive?". S3 Batch Operations confirms only that it initiated replication for each object, not that the bytes landed at the destination: a job reaches `Complete` while replication may still be in flight, or may have failed.

Set `CompletionNotificationEmail` to turn it on. The Solution then polls each tagged object's `x-amz-replication-status`, which S3 sets to `COMPLETED` only once the object has reached every destination its rules target, and emails one report per source bucket covering everything that reached a terminal answer since the last one:

> my-bucket: 150 objects replicated successfully. No action needed. No objects remain in tracking.

That closing count is the batch-level answer. A report covers what confirmed by the time it was sent, which is not the same as a set of objects you tagged together: if some replicate quickly and others lag, one tagging batch is reported across several emails. `No objects remain in tracking` means none are left awaiting confirmation for that bucket, so the wave has landed. A non-zero count means more reports are coming, and it appears in the email subject too, so an inbox can be triaged without opening anything.

Object keys and version IDs appear in the report unredacted, unlike the structured logs described in [Monitoring](#monitoring).

Two limits on what a successful outcome means. It is one aggregate across every destination, so for an object bound for two buckets it means both succeeded, and a failure means at least one did not, without saying which. And it confirms S3 reported `COMPLETED` at the time of the check, not that the replica is still there now: per [AWS's S3 Batch Replication considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-batch-replication-batch.html#batch-replication-considerations), a destination version deleted by specifying its version ID is not re-replicated, and no check detects that.

Leave `CompletionNotificationEmail` empty and none of the above runs. The Batch Operations completion report CSV is written to the State Bucket either way, and the Solution reads it either way to diagnose permission-shaped failures (`InitiateReplicationNotPermitted`, `AccessDenied`).

[Completion Reporting](docs/completion-reporting.md) has the outcomes an object can reach, the fields each entry carries, and the tracking mechanics.

## Deleted-Version Filtering

Before writing any manifest, the Solution removes object versions that have been permanently deleted, since enough failed tasks will cause S3 Batch Operations to cancel the whole job.

- **Permanently deleted** (DELETE with `is_delete_marker = false`, or lifecycle expiration): excluded from the manifest.
- **Delete marker placed** (`is_delete_marker = true`): the tagged version still exists as a noncurrent version, so it is kept in the manifest.

**A delete marker is never itself replicated.** Two independent reasons, so this is not something a configuration change can alter.

A delete marker carries no tags. The journal records a null tag set for a `DELETE` record, so a delete marker cannot satisfy a tag filter and never becomes a candidate for a manifest. See [S3 Metadata journal tables schema](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-schema.html) for the column behavior, and [Working with delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html) for what a delete marker is.

S3 also does not support delete marker replication for tag-based rules at all. A rule whose `Filter` includes a `Tag` must set `DeleteMarkerReplication` to `Disabled`, and S3 rejects the configuration otherwise, so the tag-scoped rules this Solution reads could not replicate a delete marker even if one could be selected. See [DeleteMarkerReplication](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-s3-bucket-deletemarkerreplication.html).

The practical consequence: deleting an object in a source bucket is not propagated to any destination, by this Solution or by the rules it reads. The delete marker stays on the source side, and the replica that was already delivered remains. Propagating deletes needs a prefix-scoped or bucket-wide replication rule with delete marker replication enabled, which is outside what this Solution drives.

## Objects That Are Not Replicated

S3 does not replicate objects in an archived storage class. They must be restored and copied to another storage class first. This is a property of S3 Replication, not of this Solution: see [What isn't replicated with replication configurations?](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-what-is-isnot-replicated.html).

Tagging such an object matches a rule as normal, so the Solution handles the object rather than ignoring it.

| Storage class or tier | Replicated | Behavior |
|---|---|---|
| `GLACIER` (S3 Glacier Flexible Retrieval) | No | Excluded before the manifest is written. No Batch Operations task is submitted and nothing is billed for it |
| `DEEP_ARCHIVE` (S3 Glacier Deep Archive) | No | Excluded before the manifest is written, as above |
| S3 Intelligent-Tiering Archive Access or Deep Archive Access tier | No | Submitted, then the task fails at S3 with `SrcObjectNotEligible`. The journal reports the storage class as `INTELLIGENT_TIERING` whatever tier the object occupies, so the Solution cannot tell this object apart from a replicable one |
| `GLACIER_IR` (S3 Glacier Instant Retrieval) | Yes | Replicated normally. Despite the name this is not an asynchronous class and needs no restore |
| Every other storage class | Yes | Replicated normally |

An excluded object raises an `archived_objects_excluded` log entry and the `ArchivedObjectsExcluded` metric.

To replicate a restored object, copy it to a non-archived storage class and tag it again. Restoring alone does not cause another attempt, because the checkpoint has already advanced past the original tagging event.

A rejected task does not interfere with the object's lifecycle rules: S3 never initiates replication, so the object never acquires `x-amz-replication-status` and is never left `PENDING` or `FAILED` (either of which would block lifecycle transitions).

## Repeat Tagging

Tagging an object again is a new tagging operation, whether or not the object has already been replicated.

| Condition | Behavior |
|---|---|
| An already-replicated object is tagged again and its new tags match a rule | S3 replicates the tag change itself, with no involvement from this Solution: `x-amz-replication-status` on the source object returns to `PENDING` on the tagging call, and reaches `COMPLETED` again once the new tag set is on the replica. The version separately enters the next Batch Operations job and is billed for one manifest entry. If the destination already holds that version, S3 Replication treats it as delivered: no transfer and no destination-side charge |
| Two matching tag states on one object version inside one interval | One manifest entry. The job evaluates the object's live tags, so a destination selected only by the superseded tag state is not reached: not by this run, and not by any later run, because the tag that selected it no longer exists |
| Two or more versions of the same key tagged inside one interval | One version per run, oldest tagging event first, until all are replicated. Each of those runs submits a job and is billed for its manifest entries. Versions still waiting must stay within `JournalLookbackSeconds` of the checkpoint, which they do unless newer tagging activity advances the watermark past them |
| An object version permanently deleted at the destination | Not restored, by this or any other job (see [Completion Reporting](#completion-reporting)) |

## Backfilling After a Replication-Rule Change

Adding or widening a tag-scoped rule does not replicate objects tagged before the change. [Backfilling After a Replication-Rule Change](docs/backfill.md) has the Athena query and Batch Operations recipe for a manual catch-up.

## Monitoring

Tells you whether the Solution is running, keeping up, and still covering every bucket you gave it. Each run emits a structured JSON `interval_summary`, plus an entry for anything that needed a decision or went wrong. Object keys are never logged; error messages reference a SHA-256 fingerprint instead. A per-bucket error is logged and the run continues for the remaining buckets, so one bad bucket does not stop the others.

[Log entries](docs/monitoring.md#log-entries) lists every event type and the fields it carries, and [Audit actions](docs/monitoring.md#audit-actions) does the same for the nine `audit` actions.

**Auto-disable.** When consecutive Batch Operations job failures for a bucket reach `MaxBatchJobFailures` (default 4), the Solution sets that bucket's `disabled` flag to `true` in `solution-config.json` and clears its stored failure history. The other buckets keep running. This circuit breaker prevents runaway per-job costs from a bucket whose job keeps failing. When `AlarmEmail` is set, an email names the disabled bucket and the recovery step.

The same threshold applies to permanent submission failures, where `create_job` is rejected by botocore's own parameter validation before the request is sent. Such a request fails identically on every retry, unlike a terminal-job failure, which may be transient, so it needs a change deployed rather than another interval. Service-side errors such as throttling and permission issues do not count toward the threshold.

To re-enable a bucket, address the cause of the job failures, then set `"disabled": false` for its entry in `solution-config.json` on the State Bucket (`s3://<state-bucket>/config/solution-config.json`) and wait for the next scheduled run. No redeploy or manual state edit is needed. A bucket disabled by a rejected request needs a code fix deployed first, or the failure reproduces.

### CloudWatch metrics

Set `MetricsNamespace` and the Solution publishes six per-bucket and run-level counters after each run: `TaggingOperationsRead`, `MatchedObjects`, `BatchJobsSubmitted`, `ArchivedObjectsExcluded`, `BucketErrors`, and `DisabledBuckets`. Leave it empty to disable this entirely; no CloudWatch permission is then required.

Some are withheld for an idle bucket rather than published as a zero, to avoid paying for a flat series (see [CloudWatch metric charges](docs/cost.md#cloudwatch-metric-charges)), so a missing data point carries meaning. [Monitoring Reference](docs/monitoring.md#cloudwatch-metrics) has each metric's dimension and publish condition, followed by the alarm recipes that depend on them, including how to catch a bucket that silently stopped being processed.

### Run failure alerts

A `ReplicationLambdaErrorAlarm` alarm covers the run itself failing, as opposed to a job failing after a run submitted it. It watches the function's CloudWatch `Errors` metric, so it catches an unhandled exception, a timeout, and an init failure such as a code package with the wrong layout. While it is in alarm, no tagging activity is being processed at all.

The alarm is always created, and emails the `AlarmEmail` address when one is set, including when the runs recover. It reads the native Lambda metric rather than the Solution's own log entries, because an init failure produces none: the module never imports. Its ARN is the `ReplicationLambdaErrorAlarmArn` output.

### Batch job failure alerts

Set `AlarmEmail` to be notified when an S3 Batch Operations job fails or is cancelled. The stack creates an SNS topic, an email subscription, and an EventBridge rule that sends one readable email per failed or cancelled job, carrying the job ID, its status, and a console link. This requires an active CloudTrail trail capturing management events in the stack's Region (see [Verifying the CloudTrail trail](deploy/README.md#verifying-the-cloudtrail-trail)). Leave `AlarmEmail` empty to disable alerting; no SNS topic is provisioned. A CloudWatch alarm on the same event exists for console and dashboard visibility and does not send its own email.

Three other alerts go to the same address. The run failure alarm above is one. The bucket-disabled notification names the bucket, the cause, and the exact recovery step. The submission-failure alert names the bucket, the operation (`CreateJob`), and the validation error; it fires once per episode and is suppressed while the same failure persists, and a successful submission clears the suppression so a recurrence after a fix is reported again. Both are always written to the `BatchJobFailureLogGroup` CloudWatch log group even when `AlarmEmail` is not set.

### Diagnosing task failures

A Batch Operations job can reach `Complete` while individual tasks in it failed, and the job's status does not reveal that. Every task failure in a job's completion report raises an `error` log entry naming the error code, how many tasks carried it, and the message S3 reported for it.

[Diagnosing task failures](docs/monitoring.md#diagnosing-task-failures) covers the two permission-shaped failures, why the reported message matters more than the error code, and the two log signals that catch a job whose tasks all failed at either job size.

## Checkpoint and Recovery

A run that fails part way through does not skip the objects it was working on. Each bucket's progress advances only once its Batch Operations job has been submitted, so a run that dies before that point leaves the checkpoint where it was and the next run covers the same tagging operations again. The checkpoint lives in the State Bucket at `state/<bucket-name>.json`.

To reset a bucket to the beginning of the journal, delete its state object at `s3://<state-bucket>/state/<bucket-name>.json`.

## Cost

This Solution uses only pay-per-use services. There are no fixed or idle charges. Cost scales with how many objects are tagged, how often the Solution runs, and how many source buckets are monitored.

| Component | Driver |
|---|---|
| S3 Batch Operations | Per-job plus per-object charge. One job per source bucket per run that has matches. Usually the dominant cost. |
| Amazon Athena | Per-TB scanned, with a 10 MB per-query minimum. The journal is an Apache Iceberg table, so the `record_timestamp > <checkpoint>` predicate lets Athena skip data files that fall entirely below the checkpoint, keeping scanned volume roughly proportional to new activity rather than total journal size. |
| AWS Lambda | Per invocation and GB-second. One invocation per run, scaled by `LambdaMemoryMB`. |
| Amazon S3 (State Bucket) | Storage for manifests and Athena results, expired after `LifecycleExpirationDays`, plus request charges. |
| Source-side `HeadObject` checks (optional) | GET-class requests. Only when `CompletionNotificationEmail` is set: one `HeadObject` per still-`PENDING` tracked object per run, capped at `CompletionCheckBatchSize`, until `x-amz-replication-status` resolves. |
| Amazon CloudWatch (optional) | Only when `MetricsNamespace` is set: per custom metric per month, billed per unique metric-and-dimension combination. Log ingestion for the structured JSON logs applies regardless. |
| Amazon SNS (optional) | Per-notification charges when `CompletionNotificationEmail` or `AlarmEmail` is set. |
| AWS KMS (optional) | Per-request charges when `KmsKeyArn` or `JournalKmsKeyArn` is set. |

The biggest lever is `CheckFrequencyMinutes`. A job is submitted only on runs that find matches, so shorter intervals raise cost most when tagging activity is spread over time, and least when it arrives in a single batch. Choose the largest interval that meets your replication-latency needs.

Rule count does not affect cost. There is one job and one journal query per bucket per run, covering every matched object across all of that bucket's tag-scoped rules.

For one source bucket tagging 10,000 objects a day, the worked example in [Cost Detail](docs/cost.md) comes to roughly $13 a month checking hourly and $14 checking every 15 minutes. That page also lists what is excluded, chiefly S3 Replication and journal-table charges.

## Hardening Options

State Bucket access logging, State Bucket versioning, and SNS topic encryption are off by default to keep deployment simple. [Hardening Options](docs/hardening.md) covers what to change for each. [Customer-Managed KMS Keys](docs/kms.md) covers all three KMS parameters, key-policy grants, and the verification procedure.

## Required AWS Permissions

The execution role holds source-side actions only. [`deploy/iam-policy.json`](deploy/iam-policy.json) is the full least-privilege policy.

The stack also creates the role its S3 Batch Operations jobs run as, separate from the execution role and from your buckets' replication roles.

[Required AWS Permissions](docs/permissions.md) explains what each grant is for, the Batch Operations job role, and the optional customer-managed KMS grants.

## Upgrading

Upgrade by repeating the Getting Started steps with the new release's assets and
updating the stack.

`CodeLocation` must change for the Lambda code to be replaced. Give each release
its own S3 key, which the versioned asset filename does for you if you keep the
version in the key.

Overwriting one fixed key leaves `CodeLocation` unchanged. CloudFormation
replaces the function code only when that parameter changes, so the stack applies
the new template while the Lambda keeps running the previous release's code, and
reports `UPDATE_COMPLETE` either way. The result is a new template against old
code, with no error to indicate it.

## Development

`pip install -e ".[dev]"` installs the package with its test dependencies; `pytest` runs the unit suite.

[Testing](docs/testing.md) covers the end-to-end test against real AWS resources, its environment variables, and the manual verification procedures.
