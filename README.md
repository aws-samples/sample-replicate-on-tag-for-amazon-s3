# Automatic replication after tagging for Amazon S3

Automates Amazon S3 replication for objects tagged after creation. S3 Replication evaluates tag-scoped rules only at object-creation time, so adding a tag to an object that already exists does not trigger it. This Solution runs on a recurring schedule, reads new tagging operations from the S3 Metadata journal, and submits S3 Batch Operations replication jobs for matched objects.

The Solution operates entirely on the source side. It never accesses the destination account or region.

[Use cases](docs/use-cases.md) covers two worked patterns: copying an existing dataset to another Region, and replicating only GuardDuty-scanned clean objects.

## Prerequisites

- S3 Metadata Tables journal table enabled on each source bucket.
- An existing S3 replication configuration on each source bucket with at least one tag-scoped rule.
- An existing IAM replication role attached to each source bucket's replication configuration.
- The S3 Tables analytics-services integration enabled in the Region. This is one-time per account and Region, via S3 console → Table buckets → Enable integration. It creates the `s3tablescatalog` Glue catalog that Athena uses to read the journal.

[`deploy/README.md`](deploy/README.md) has the full deployment procedure, the complete parameter reference, and the `aws glue create-catalog` alternative to enabling the integration from the console.

## Getting Started

Complete the [Prerequisites](#prerequisites), then per source account and region:

1. Download `template.yaml`, `package.zip`, and `package.zip.sha256` from the project's Releases page.
2. Verify `package.zip` against `package.zip.sha256`.
3. Upload `package.zip` to an S3 bucket in the same region as the stack. Its S3 URI, for example `s3://my-code-bucket/package.zip`, is the `CodeLocation` parameter.
4. Create the CloudFormation stack from `template.yaml`, set the parameters, and acknowledge `CAPABILITY_IAM`. Use the console, or the AWS CLI with the template staged in S3 (see [`deploy/README.md`](deploy/README.md#4-create-the-stack)).

## Required AWS Permissions

The execution role holds source-side actions only. [`deploy/iam-policy.json`](deploy/iam-policy.json) is the full least-privilege policy.

[Required AWS Permissions](docs/permissions.md) explains what each grant is for, the optional customer-managed KMS grants, and the one extra grant completion reporting needs.

## Parameters

**Required** (no default, you must provide a value):

| CloudFormation parameter | Description |
|---|---|
| `CodeLocation` | S3 URI of the Lambda zip, e.g. `s3://my-code-bucket/package.zip` (bucket co-regional with the stack) |
| `SourceBucketNames` | Comma-separated source bucket names to monitor |

The replication role passed to S3 Batch Operations is not a parameter. At deploy time the stack reads each source bucket's replication configuration and scopes the `iam:PassRole` grant to exactly those roles.

**Optional** (commonly tuned; full list in [`deploy/README.md`](deploy/README.md#parameter-reference)):

| CloudFormation parameter | Default | Description |
|---|---|---|
| `CheckFrequencyMinutes` | 15 | How often the Solution runs, in minutes (15–1440). Because S3 Batch Operations charges per job, smaller values can raise cost, most of all when tagging activity is spread over time rather than arriving in a single batch |
| `LambdaMemoryMB` | 2,048 | Memory in MiB for the main ReplicationLambda function |
| `JournalReadRowCap` | 500,000 | Max tagging operations processed in one interval, and the single scale knob bounding in-memory manifest size. A burst above this is processed over multiple intervals instead of risking the Lambda timeout (see [Configuration](#configuration)) |
| `ReinvocationChainLimit` | 20 | Max consecutive self-reinvocations draining a capped run's backlog before deferring to the next scheduled trigger |
| `CompletionNotificationEmail` | _(empty)_ | Email address for per-object-version replication completion reports. Setting it enables completion reporting; leave empty to disable it entirely (see [Completion Reporting](#completion-reporting)) |
| `CompletionCheckBatchSize` | 2,000 | Max replication-status checks issued per run when completion reporting is enabled |
| `CompletionItemTtlHours` | 168 | Hours an object may await replication confirmation before it is abandoned as `EXPIRED`. A backstop that bounds state growth; minimum 24 |
| `AlarmEmail` | _(empty)_ | Email address for Batch Operations job-failure and bucket-disabled alerts. Leave empty to disable alerting; no SNS topic is provisioned (see [Monitoring](#monitoring)) |
| `MaxBatchJobFailures` | 4 | Consecutive Batch Operations job failures for a bucket before the Solution disables that bucket |
| `KmsKeyArn` | _(empty)_ | Customer-managed KMS key ARN; leave empty for SSE-S3 |
| `MetricsNamespace` | _(empty)_ | CloudWatch namespace to publish metrics under, any name you choose, e.g. `S3ReplicateOnTag`. CloudWatch creates it on first publish; names starting with `AWS/` are reserved. Leave empty to disable metrics |
| `VpcId` | _(empty)_ | Deploy Lambda into this VPC; requires S3 and Athena VPC endpoints |

All threshold parameters are also settable as Lambda environment variables without redeploying the stack.

## Configuration

The Solution is configured entirely through CloudFormation stack parameters. There is no file to author before deploying. On deploy, the stack writes an internal config object (`solution-config.json`) to the State Bucket that the Lambda reads at startup. The only supported manual edit to that object is clearing a bucket's `disabled` flag after an auto-disable (see [Monitoring](#monitoring)). Every other value in it is derived from stack parameters and is overwritten on the next deploy.

**Monitored buckets.** The bucket list comes from the `SourceBucketNames` parameter. To add or remove a bucket, update `SourceBucketNames` and deploy a stack update. This is the only supported method. The execution role's per-bucket permissions, and in Lake Formation accounts the journal grants, are scoped to this parameter at deploy time, so adding a bucket by hand-editing the config object would leave it without journal or bucket access.

**Replication rules.** Tag filters, key prefixes, and destinations are read from each bucket's existing replication configuration, not set on this Solution. The Solution acts only on **tag-scoped** rules; rules without a tag filter, whether prefix-only or unfiltered, are ignored. A bucket may have multiple tag-scoped rules. All of a bucket's tag-scoped rules are evaluated together, and every matched object across every rule goes into one Batch Operations job per bucket per interval.

**Journal read cap.** `JournalReadRowCap` (default 500,000) is the Solution's single scale knob. It caps how many tagging operations one run processes and how many matched objects the manifest holds. The Lambda always builds the manifest in memory, in the S3 Inventory Report format, under either SSE-S3 or SSE-KMS. A run that finds more than the cap processes the oldest `JournalReadRowCap` operations and picks up the rest automatically, so no tagging operation is ever dropped, only delayed. A capped run reinvokes itself immediately rather than waiting for the next scheduled interval, so a temporary burst clears faster than the schedule alone allows.

Tagging faster than `JournalReadRowCap × (1440 / CheckFrequencyMinutes)` per day, which is 48,000,000/day at the defaults, relies on that immediate rerun to catch up. Sustained volume above it builds backlog. Raising `JournalReadRowCap` lets each run clear more before capping, at the cost of Lambda timeout and memory headroom, so the Solution enforces a safe maximum for the configured `LambdaMemoryMB`. Those limits are tabulated in the [deployment parameter reference](deploy/README.md#parameter-reference).

The cap is a target, not an exact count. A run reads slightly more when many tagging operations share the boundary timestamp, never fewer, so no operation is dropped.

## Completion Reporting

Completion reporting confirms, per object version, that replication actually finished, not just that S3 Batch Operations ran. It reads the source object's native `x-amz-replication-status` header and reports the outcome once per source bucket over SNS.

Set `CompletionNotificationEmail` to enable it. Leave it empty and none of the behaviour below runs.

When enabled:

- A completion report is requested for every S3 Batch Operations job and written to the State Bucket under `completion-reports/`.
- Once a job's completion report confirms processing, each object version is tracked with one `x-amz-replication-status` check per run, capped at `CompletionCheckBatchSize` (default 2,000/run).
- One SNS message per source bucket per run covers every object that resolved and passed the tag-quiescence check. Reports split across as many messages as needed to stay within SNS's 256 KB limit, so a bucket resolving tens of thousands of objects in one interval still reports. An object leaves tracking only once the message covering it publishes successfully.
- An alert fires if a job's completion report has not appeared within 1 hour of that job finishing.

Every tracked object reaches a terminal outcome. `GONE` and `EXPIRED` exist so that an object cannot be tracked indefinitely.

| Outcome | Meaning |
|---|---|
| `COMPLETE` | S3 reported `x-amz-replication-status: COMPLETED` on the source object |
| `PENDING`, `FAILED` | Verbatim `x-amz-replication-status` value |
| `UNKNOWN` | The check succeeded but the object carried no replication-status header |
| `GONE` | The object version no longer exists, so replication can never be confirmed |
| `EXPIRED` | The object stayed unresolved past `CompletionItemTtlHours` and was abandoned |

An entry carries the object's key, version ID, one aggregate outcome from the table above, and a `destinations` list of the routing configuration identifiers that matched it. The destinations list is context only. One job covers every matched rule, so there is no per-destination outcome breakdown. Object keys and version IDs are not redacted in this report, unlike the structured logs described in [Monitoring](#monitoring).

Each report opens with a plain-English `summary` line stating the bucket, the object count, what happened to those objects, and whether anything needs attention. Outcomes are listed most severe first, so failures lead even when they are the minority. The email subject carries the same verdict, so an inbox of reports can be triaged without opening them.

A `COMPLETE` outcome confirms S3 reported `x-amz-replication-status: COMPLETED` on the source object at some point. It does not confirm the object is still present at the destination now. Per [AWS's S3 Batch Replication considerations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-batch-replication-batch.html#batch-replication-considerations), S3 Batch Replication does not re-replicate a destination version that was deleted by specifying its version ID, so a `COMPLETE` outcome cannot detect that case.

Completion reporting needs one IAM grant beyond the baseline policy. See [Required AWS Permissions](docs/permissions.md#completion-reporting).

## Deleted-Version Filtering

Before writing any manifest, the Solution removes object versions that have been permanently deleted, since enough failed tasks will cause S3 Batch Operations to cancel the whole job.

- **Permanently deleted** (DELETE with `is_delete_marker = false`, or lifecycle expiration): excluded from the manifest.
- **Delete marker placed** (`is_delete_marker = true`): the tagged version still exists as a noncurrent version, so it is kept in the manifest.

## Repeat Tagging

Tagging an object again is a new tagging operation, whether or not the object has already been replicated.

| Condition | Behaviour |
|---|---|
| An already-replicated object is tagged again and its new tags match a rule | Its object version goes into the next Batch Operations job for the bucket, and the source object's `x-amz-replication-status` returns to `PENDING` |
| A destination already holds that object version | Batch Operations treats it as delivered: no transfer and no destination-side charge |
| Cost of the resubmission | One manifest entry, billed like any other manifest entry |
| Two matching tag states on one object version inside one interval | One manifest entry. The job evaluates the object's live tags, so a destination selected only by the superseded tag state is not reached — not by this run, and not by any later run, because the tag that selected it no longer exists |
| Two or more versions of the same key tagged inside one interval | One version per run, oldest tagging event first, until all are replicated. Each of those runs submits a job and is billed for its manifest entries. Versions still waiting must stay within `JournalLookbackSeconds` of the checkpoint, which they do unless newer tagging activity advances the watermark past them |
| An object version permanently deleted at the destination | Not restored, by this or any other job (see [Completion Reporting](#completion-reporting)) |

### Backfilling after a replication-rule change

Adding or widening a tag-scoped rule does not replicate objects that were tagged before the change. Their journal records sit below the checkpoint watermark and outside the lookback window, so no run picks them up. Matching them from the source side would need a `HeadObject` per object in scope, so the Solution does not do it. Backfill manually instead.

The recipe below queries the **S3 Metadata live inventory table** for the objects the new rule selects and writes a CSV manifest for an `S3ReplicateObject` Batch Operations job, submitted with the source bucket's existing replication role.

Read this first:

- The live inventory table is not part of this Solution, which reads only the `journal` table. Enable it separately on the source bucket's metadata configuration. Enabling it triggers a backfill that you are charged for, and a bucket over one billion objects also carries a monthly inventory-table fee. See [the inventory table schema](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-inventory-schema.html).
- S3 Batch Operations bills every object listed in the manifest, whether or not it is already replicated. The inventory table has no replication-status column, so the manifest cannot be pre-filtered to unreplicated objects. Narrow it with the tag and prefix predicates you actually need.
- Object versions permanently deleted at the destination cannot be restored by any manifest.

Run this in Athena, substituting the source bucket name (the namespace is `b_` plus the bucket name with `.` replaced by `_`), the tag the new rule filters on, and a destination for the manifest:

```sql
UNLOAD (
  SELECT bucket || ',' ||
         replace(url_encode(key), '%2F', '/') || ',' ||
         COALESCE(NULLIF(version_id, ''), 'null') AS manifest_row
  FROM "s3tablescatalog/aws-s3"."b_amzn_s3_demo_bucket"."inventory"
  WHERE bucket = 'amzn-s3-demo-bucket'
    AND COALESCE(is_delete_marker, FALSE) = FALSE
    AND element_at(object_tags, 'replicate') = 'yes'
)
TO 's3://my-manifest-bucket/backfill-manifest/'
WITH (format = 'TEXTFILE', compression = 'NONE')
```

`url_encode` percent-encodes the key so that a comma or newline in a key cannot break the row, and `replace` restores `/`, which Trino encodes as `%2F`. This matches the encoding the Solution's own manifests use. `null` is the literal third field for an object with no version ID; an empty field fails the task with `SrcObjectNotFound`.

`UNLOAD` writes one or more objects under the prefix. A Batch Operations manifest is a single object, so submit one job per written object, or concatenate them first.

Create each job with the CSV manifest, operation `S3ReplicateObject`, and the replication role already attached to the source bucket's replication configuration. Job progress is visible in the Batch Operations console; the Solution's completion reporting does not track manually created jobs.

## Monitoring

The Solution emits structured JSON log entries. All entries include a `timestamp` field in ISO 8601 format.

| Event | When emitted |
|---|---|
| `interval_summary` | Once per run: `Tagging_Operations`, `Matched_Objects`, `Batch_Replication_Job_submissions`, `duplicate_records_discarded` |
| `job_submitted` | Each successful Batch Operations job: `job_id`, `source_bucket` |
| `error` | Per-bucket error or skip: `component`, `bucket`, `cause` |
| `audit` | Security-critical mutations (lease acquire/release, job creation), including `journal_read_capped` when a run hits `JournalReadRowCap`: `row_cap`, `until_timestamp`, `since_timestamp` |
| `reinvocation_triggered` | A capped run that progressed triggers the next run immediately: `chain_position` |
| `reinvocation_chain_limit_reached` | `ReinvocationChainLimit` reached with backlog remaining: `chain_limit`, `depth` |
| `deleted_versions_excluded` | When versions are filtered: `excluded_count` |
| `completion_item_expired` | An `audit` action: a tracked object passed `CompletionItemTtlHours` and was abandoned: `job_ids`, `age_seconds`, `ttl_seconds` |

Per-bucket errors are logged and the run continues for the remaining buckets. Object keys are never logged; error messages reference a SHA-256 fingerprint instead.

**Auto-disable.** When consecutive Batch Operations job failures for a bucket reach `MaxBatchJobFailures` (default 4), the Solution sets that bucket's `disabled` flag to `true` in `solution-config.json` and clears its stored failure history. The other buckets keep running. This circuit breaker prevents runaway per-job costs from a bucket whose job keeps failing. When `AlarmEmail` is set, an email names the disabled bucket and the recovery step.

The same threshold applies to permanent submission failures, where `create_job` is rejected by botocore's own parameter validation before the request is sent. A rejected request is a code defect in the Solution and will never self-heal, unlike a terminal-job failure, which may be transient. Service-side errors such as throttling and permission issues do not count toward the threshold.

To re-enable a bucket, address the cause of the job failures, then set `"disabled": false` for its entry in `solution-config.json` on the State Bucket (`s3://<state-bucket>/config/solution-config.json`) and wait for the next scheduled run. No redeploy or manual state edit is needed. A bucket disabled by a rejected request needs a code fix deployed first, or the failure reproduces.

### CloudWatch Metrics

When `MetricsNamespace` is set, the Solution publishes per-bucket and run-level counters as CloudWatch custom metrics after each run. Leave it empty to disable this entirely; no CloudWatch permission is then required.

The namespace is the name you look for in the CloudWatch console and needs no setup beyond this parameter. `MetricsDeploymentId`, if set, adds a `Deployment` dimension to every metric below, so multiple stacks can publish to one namespace without colliding.

| Metric | Dimension | Published | Description |
|---|---|---|---|
| `TaggingOperationsRead` | `SourceBucket` | Only when the bucket had activity | Distinct tagging operations matched |
| `MatchedObjects` | `SourceBucket` | Only when the bucket had activity | Objects matched against replication rules |
| `BatchJobsSubmitted` | `SourceBucket` | Only when the bucket had activity | 0 or 1, whether a job was submitted for the bucket this run. Never more than 1, because one job covers every matched object across all of the bucket's tag-scoped rules |
| `BucketErrors` | `SourceBucket` | Every run, including the 0 | 1 if the bucket was skipped due to an error, 0 otherwise |
| `DisabledBuckets` | _(none)_ | Every run, including the 0 | How many buckets were skipped because their `disabled` flag is set |

The three activity metrics are published as a group, so a run with any activity carries any genuine zeros among them. They are withheld for an idle bucket to avoid paying for a flat zero series (see [Cost detail](docs/cost.md#cloudwatch-metric-charges)). A missing data point therefore carries meaning:

| Observation | Meaning |
|---|---|
| No activity metrics, `BucketErrors` present | Bucket was processed and had nothing to do |
| `BucketErrors` present and 1 | Bucket was processed and errored |
| No `BucketErrors` for a bucket | Bucket is disabled, absent from `SourceBucketNames`, or the run never reached it |

Alarm on the last row with `treatMissingData: breaching` to catch a bucket that silently stopped being processed.

For the auto-disable case, alarm on `DisabledBuckets >= 1` instead. It is a plain threshold on a metric published every run, so it does not depend on missing-data handling, and unlike the disable notification email it reports the condition for as long as it persists rather than once when it happens. It is run-level, so it tells you how many buckets are disabled. Which ones is in the notification email, the per-run `error` log entry, and the `disabled` flags in `solution-config.json`.

### Batch Job Failure Alerts

Set `AlarmEmail` to be notified when an S3 Batch Operations job fails or is cancelled. The stack creates an SNS topic, an email subscription, and an EventBridge rule that sends one readable email per failed or cancelled job, carrying the job ID, its status, and a console link. This requires an active CloudTrail trail capturing management events in the stack's Region (see [`deploy/README.md`](deploy/README.md#parameter-reference)). Leave `AlarmEmail` empty to disable alerting; no SNS topic is provisioned. A CloudWatch alarm on the same event exists for console and dashboard visibility and does not send its own email.

Two other alerts go to the same address. The bucket-disabled notification names the bucket, the cause, and the exact recovery step. The submission-failure alert names the bucket, the operation (`CreateJob`), and the validation error; it fires once per episode and is suppressed while the same failure persists, and a successful submission clears the suppression so a recurrence after a fix is reported again. Both are always written to the `BatchJobFailureLogGroup` CloudWatch log group even when `AlarmEmail` is not set.

## Checkpoint and Recovery

The Solution persists a per-bucket checkpoint in the State Bucket at `state/<bucket-name>.json`. The checkpoint advances only when a Batch Operations job is successfully submitted. If a run fails mid-way, the next run resumes from the last successful checkpoint.

To reset a bucket to the beginning of the journal, delete its state object at `s3://<state-bucket>/state/<bucket-name>.json`.

## Cost

This Solution uses only pay-per-use services. There are no fixed or idle charges. Cost scales with how many objects are tagged, how often the Solution runs, and how many source buckets are monitored.

| Component | Driver |
|---|---|
| S3 Batch Operations | Per-job plus per-object charge. One job per source bucket per run that has matches. Usually the dominant cost. |
| Amazon Athena | Per-TB scanned, with a 10 MB per-query minimum. Queries filter on `record_timestamp` against the last checkpoint, so a run scans only the journal data added since the previous run. |
| AWS Lambda | Per invocation and GB-second. One invocation per run, scaled by `LambdaMemoryMB`. |
| Amazon S3 (State Bucket) | Storage for manifests and Athena results, expired after `LifecycleExpirationDays`, plus request charges. |
| Source-side `HeadObject` checks (optional) | GET-class requests. Only when `CompletionNotificationEmail` is set: one `HeadObject` per still-`PENDING` tracked object per run, capped at `CompletionCheckBatchSize`, until `x-amz-replication-status` resolves. |
| Amazon CloudWatch (optional) | Only when `MetricsNamespace` is set: per custom metric per month, billed per unique metric-and-dimension combination. Log ingestion for the structured JSON logs applies regardless. |
| Amazon SNS (optional) | Per-notification charges when `CompletionNotificationEmail` or `AlarmEmail` is set. |
| AWS KMS (optional) | Per-request charges when `KmsKeyArn` or `JournalKmsKeyArn` is set. |

The biggest lever is `CheckFrequencyMinutes`. A job is submitted only on runs that find matches, so shorter intervals raise cost most when tagging activity is spread over time, and least when it arrives in a single batch. Choose the largest interval that meets your replication-latency needs.

Rule count does not affect cost. There is one job and one journal query per bucket per run, covering every matched object across all of that bucket's tag-scoped rules.

For one source bucket tagging 10,000 objects a day, the worked example in [Cost detail](docs/cost.md) comes to roughly $13 a month checking hourly and $14 checking every 15 minutes. That page also lists what is excluded, chiefly S3 Replication and journal-table charges.

## Hardening Options

State Bucket access logging, State Bucket versioning, and SNS topic encryption are off by default to keep deployment simple. [Hardening options](docs/hardening.md) covers what to change for each.

## Development

`pip install -e ".[dev]"` installs the package with its test dependencies; `pytest` runs the unit suite.

The end-to-end tests (`tests/test_e2e_aws.py`) require AWS credentials and several environment variables. See [End-to-End Test Environment Variables](deploy/README.md#end-to-end-test-environment-variables).
