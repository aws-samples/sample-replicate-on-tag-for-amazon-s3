# Deployment Guide

Deploy **Automatic replication after tagging for Amazon S3** as a scheduled
Lambda function using CloudFormation. The stack is created through the AWS
CloudFormation console or the AWS CLI. No CLI build or code checkout is required.

## Prerequisites

The conditions your account and buckets must meet are listed once, in
[Prerequisites](../README.md#prerequisites). This section covers only what is
specific to running the deployment.

### Permissions to deploy

IAM permissions to create CloudFormation stacks, Lambda functions, S3 buckets,
Athena workgroups, IAM roles, and EventBridge Scheduler schedules.

### The Batch Operations job role

The stack creates the role its batch jobs run as and scopes the Lambda's
`iam:PassRole` grant to that one ARN. You pass no role ARN and edit no role of
your own. Its permissions are listed in
[Batch Operations job role](../docs/permissions.md#batch-operations-job-role),
and its ARN is the `BatchOperationsRoleArn` stack output.

The `iam:PassRole` grant carries no `iam:PassedToService` condition, because
`s3control:CreateJob` does not populate that context key and a statement using it
would always deny. The single role ARN is the boundary instead. To monitor the
grant, filter CloudTrail for `PassRole` events naming that role and check that
the consuming service is S3 Batch Operations.

### Registering the S3 Metadata catalog from the CLI

The Solution queries the journal through Athena's `s3tablescatalog/aws-s3`
connector, which needs the `s3tablescatalog` federated Glue catalog to exist in
the Region. The console route (**S3 → Table buckets → Enable integration**) is in
the main Prerequisites. The CLI equivalent:

```bash
cat > /tmp/s3tablescatalog.json <<'EOF'
{
  "Name": "s3tablescatalog",
  "CatalogInput": {
    "FederatedCatalog": {
      "Identifier": "arn:aws:s3tables:REGION:ACCOUNT_ID:bucket/*",
      "ConnectionName": "aws:s3tables"
    },
    "CreateDatabaseDefaultPermissions": [{
      "Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
      "Permissions": ["ALL"]
    }],
    "CreateTableDefaultPermissions": [{
      "Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
      "Permissions": ["ALL"]
    }],
    "AllowFullTableExternalDataAccess": "True"
  }
}
EOF
aws glue create-catalog --region REGION --cli-input-json file:///tmp/s3tablescatalog.json  # register catalog
```

Replace `REGION` and `ACCOUNT_ID` with your values. If the catalog already
exists, use `aws glue update-catalog` instead.

**IAM mode versus Lake Formation mode.** The command above registers the catalog in IAM mode
(`IAM_ALLOWED_PRINCIPALS` present). If your account uses AWS Lake Formation as the
access-control layer and `IAM_ALLOWED_PRINCIPALS` is absent, the Solution detects
this at deploy time and grants the Lambda execution role the necessary Lake
Formation permissions. See
[Lake Formation accounts](../docs/permissions.md#lake-formation-accounts).

### Verifying the CloudTrail trail

Required only when `AlarmEmail` is set. The `BatchJobFailureRule` EventBridge rule
captures S3 Batch Operations job status changes delivered via CloudTrail (`AWS
Service Event via CloudTrail`, `eventName: JobStatusChanged`). Those events reach
the default event bus only when a trail recording management events is active in
the same Region as the stack, so without one those alerts never fire even though
batch jobs are failing. Replication processing is unaffected either way, and the
run-failure alarm needs no trail because it reads a CloudWatch metric.

Check in the CloudTrail console: **Trails → select trail → Management events →
All**. If no trail exists, create one scoped to the stack Region.

## Parameter Reference

### Required

These have no default. You must provide a value.

| Parameter | Description |
|-----------|-------------|
| `CodeLocation` | S3 URI of the Lambda zip, e.g. `s3://<your-code-bucket>/package-<version>.zip` (bucket co-regional with the stack). Must differ per release, or the Lambda keeps the old code (see [step 3](#3-create-a-co-regional-code-bucket-and-upload-the-zip)) |
| `SourceBucketNames` | Comma-separated source S3 bucket names monitored by the Solution. At least one required; each must be a valid S3 bucket name |

### Optional

**Processing config**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CheckFrequencyMinutes` | `15` | How often the Solution runs, in minutes (15–1440). Smaller values run more often; because S3 Batch Operations charges per job, this can submit more jobs and increase cost, especially when tagging activity is spread over time rather than arriving in a single batch |
| `LifecycleExpirationDays` | `30` | Days before manifest and Athena result objects expire in the State Bucket |
| `JournalLookbackSeconds` | `3600` | Seconds to re-scan the journal below the watermark for late-arriving records |

**Scale & performance**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LambdaMemoryMB` | `2048` | Memory in MiB for the main ReplicationLambda function. One of 1024, 2048, 3072, 4096, 6144, 8192, 10240; each carries its own `JournalReadRowCap` ceiling, tabulated below |
| `CompletionCheckMemoryMB` | `256` | Memory in MiB for CompletionReportCheckLambda (report-missing detection, only created when `CompletionNotificationEmail` is set). One of 128, 256, 512, 1024, 2048. Sized independently, since this function does not read the journal or generate manifests |
| `LambdaTimeoutSeconds` | `900` | Lambda function timeout in seconds (max 900), for ReplicationLambda |
| `JournalReadRowCap` | `500000` | Max journal rows read in one interval, and the single scale knob bounding in-memory manifest size; a larger single-interval tagging burst is processed in bounded partial reads across multiple intervals |
| `ReinvocationChainLimit` | `20` | Max consecutive self-reinvocations draining a capped run's backlog before deferring to the next scheduled trigger |

Raising `JournalReadRowCap` lets each run clear more before capping, at the cost
of Lambda timeout and memory headroom. The Solution enforces a safe maximum for
the configured `LambdaMemoryMB`:

| `LambdaMemoryMB` | Max safe `JournalReadRowCap` |
|---|---|
| 1,024 | 250,000 |
| 2,048 (default) | 500,000 |
| 3,072 | 750,000 |
| 4,096 | 1,000,000 |
| 6,144 | 1,500,000 |
| 8,192 | 2,000,000 |
| 10,240 | 2,500,000 |

A `JournalReadRowCap` above the ceiling for the configured `LambdaMemoryMB` is
refused rather than clamped: the run fails at startup with a configuration error
naming the offending value, the ceiling, and the memory size. This is deliberate,
since silently processing fewer rows than configured would misreport progress,
and honouring the value would risk an out-of-memory failure part way through a
run. Raise `LambdaMemoryMB` in the same change.

The cap is a target, not an exact count. A run reads slightly more when many
tagging operations share the boundary timestamp, never fewer, so no operation is
dropped. These limits reserve headroom for that.

Sustained tagging above `JournalReadRowCap × (1440 / CheckFrequencyMinutes)` per
day, which is 48,000,000/day at the defaults, builds backlog faster than the
Solution clears it. See [Configuration](../README.md#configuration) for how a
capped run drains a temporary burst.

**Encryption**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `KmsKeyArn` | _(empty)_ | ARN of a customer-managed KMS key for encrypting state objects and Athena results. Leave empty for SSE-S3 |
| `JournalKmsKeyArn` | _(empty)_ | ARN of a customer-managed KMS key encrypting the S3 Metadata journal tables. Leave empty if the journal uses SSE-S3 or an AWS-managed key |
| `SnsKmsKeyArn` | _(empty)_ | ARN of a symmetric customer-managed KMS key for encrypting the completion-report and batch-job-failure SNS topics. Leave empty for unencrypted topics. Requires key-policy statements for `sns.amazonaws.com` and `events.amazonaws.com` or every publish fails (see [SNS topic encryption](../docs/kms.md#sns-topic-encryption)) |

When either KMS parameter is set, the key policy must grant the `ExecutionRole` the required permissions (see [Customer-Managed KMS Keys](../docs/kms.md)).

**Networking (VPC)**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `VpcId` | _(empty)_ | VPC ID to deploy the Lambda into. Leave empty to deploy outside any VPC |
| `SubnetIds` | _(empty)_ | (Required when `VpcId` is set) Comma-separated subnet IDs |
| `SecurityGroupIds` | _(empty)_ | (Required when `VpcId` is set) Comma-separated security group IDs |

**Metrics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MetricsNamespace` | _(empty)_ | CloudWatch custom metrics namespace, any name you choose, e.g. `S3ReplicateOnTag`; created on first publish. Names starting with `AWS/` are reserved. Leave empty to disable metrics |
| `MetricsDeploymentId` | _(empty)_ | Label added as a `Deployment` dimension on every published metric |

**Monitoring**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `AlarmEmail` | _(empty)_ | Email address for run-failure and S3 Batch Operations job-failure notifications. When set, creates an SNS topic and an email subscription; a failed or cancelled batch job sends one readable email with the job ID, status, and a console link. A CloudWatch alarm on the same event exists for console/dashboard visibility but does not itself send email. The separate `ReplicationLambdaErrorAlarm` does email this address when a scheduled run fails outright, and again when runs recover. Requires an active CloudTrail trail in the stack Region for the batch job events only (see [Verifying the CloudTrail trail](#verifying-the-cloudtrail-trail)) |
| `MaxBatchJobFailures` | `4` | Consecutive S3 Batch Operations job failures (`Failed` or `Cancelled`) for a bucket's job before the Solution disables that bucket in `solution-config.json`. Prevents runaway per-job costs from a bucket whose job keeps failing; the failure counter resets on the first successful (`Complete`) job |

**Completion tracking**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CompletionNotificationEmail` | _(empty)_ | Email address for per-object replication tracking and completion email reports. The S3 Batch Operations completion report CSV is always written to the State Bucket; this parameter gates the per-object `x-amz-replication-status` tracking, the SNS email, and the report-missing alert only |
| `CompletionCheckBatchSize` | `2000` | Maximum number of replication-status checks issued per run when completion tracking is enabled |
| `CompletionItemTtlHours` | `168` | Hours a tracked object may await replication confirmation before it is abandoned with outcome `EXPIRED` and removed from tracking. A backstop bounding state-object growth for objects that can never resolve; minimum 24, since it must exceed both real replication time and several run intervals |

Every S3 Batch Operations job writes a completion report CSV to the State Bucket
under `completion-reports/`, regardless of whether `CompletionNotificationEmail`
is set. The Solution reads it to diagnose permission-shaped error codes. Setting
`CompletionNotificationEmail` additionally enables per-object
`x-amz-replication-status` tracking, the SNS email report, and the
report-missing alert.

The report is written by the job itself, using the `s3:PutObject` grant on
`completion-reports/*` that the stack's
[Batch Operations job role](../docs/permissions.md#batch-operations-job-role)
carries. The stack's `ExecutionRole` reads the report back through its existing
`ScratchBucketReadWrite` grant, so no additional statement is needed on either
role. Completion tracking never accesses the destination account or Region: it
reads only the source object's own replication-status header.

**Lake Formation**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LFAdminRoleArn` | _(empty)_ | ARN of an existing Lake Formation data lake administrator role. Leave empty to let the Solution self-elevate when the catalog is in LF mode |

The `solution-config.json` `processing_interval` is not a parameter. It is derived automatically from `CheckFrequencyMinutes` (e.g. `30` → `30m`, `60` → `1h`).

### Tuning without a stack update

The threshold parameters reach the Lambda as environment variables, so they can
be changed on the function directly to try a value against a live workload
without a stack update. Everything else, including `CheckFrequencyMinutes`, the
memory sizes, and the encryption and networking parameters, must go through the
stack.

| Parameter | Environment variable |
|---|---|
| `JournalReadRowCap` | `JOURNAL_READ_ROW_CAP` |
| `JournalLookbackSeconds` | `JOURNAL_LOOKBACK_SECONDS` |
| `ReinvocationChainLimit` | `REINVOCATION_CHAIN_LIMIT` |
| `MaxBatchJobFailures` | `MAX_BATCH_JOB_FAILURES` |
| `CompletionCheckBatchSize` | `COMPLETION_CHECK_BATCH_SIZE` |
| `CompletionItemTtlHours` | `COMPLETION_ITEM_TTL_HOURS` |
| `MetricsNamespace` | `METRICS_NAMESPACE` |
| `MetricsDeploymentId` | `METRICS_DEPLOYMENT_ID` |

Two things to know before using this. The next stack update overwrites every one
of these values from the stack parameters, so a change worth keeping belongs in
the parameter. And `JOURNAL_READ_ROW_CAP` is still checked against the ceiling
for the deployed `LambdaMemoryMB`, so raising it past that ceiling this way fails
every run at startup until the value comes back down or the stack is updated to a
larger memory size.

## 1. Download Release Artifacts

Download all three files from the project [Releases](https://github.com/aws-samples/sample-replicate-on-tag-for-amazon-s3/releases) page:

- `template.yaml` — the CloudFormation template
- `package-<version>.zip` — the Lambda code package
- `package-<version>.zip.sha256` — the SHA256 checksum for that zip

## 2. Verify the Checksum

The checksum file names the zip, so verify it directly from the directory you
downloaded into:

```bash
sha256sum -c package-<version>.zip.sha256      # Linux
shasum -a 256 -c package-<version>.zip.sha256  # macOS
```

The command prints `OK` on success. A mismatch indicates a corrupted or tampered
download; do not proceed.

## 3. Create a Co-Regional Code Bucket and Upload the Zip

The Lambda code package must be stored in a bucket that is **in the same AWS
Region as the target stack**. A cross-Region code source causes stack creation to
fail.

1. Create or select an S3 bucket in the same Region where you will deploy the
   stack.
2. Upload the zip to the bucket, keeping the version in the key. The resulting S3
   URI (for example, `s3://<your-code-bucket>/releases/<version>/package-<version>.zip`) is
   the `CodeLocation` parameter.

Any key works, with or without a prefix, but **each release must go to a
different key**. `CodeLocation` is what tells CloudFormation the code changed:
`ReplicationLambda`'s `Code.S3Key` is derived from it by a custom resource, and
CloudFormation replaces the function code only when that parameter's value
changes. Overwriting one fixed key across releases leaves `CodeLocation`
identical, so the stack applies the new template while the Lambda keeps running
the previous release's code and still reports `UPDATE_COMPLETE`. Keeping the
version in the key avoids this; `build-package.sh`'s upload mode achieves the
same by content-hashing the key.

## 4. Create the Stack

Deploy with either the CloudFormation console or the AWS CLI.

### Option A — CloudFormation console

1. Open the CloudFormation console in the target Region.
2. Choose **Create stack** → **With new resources (standard)**.
3. Under **Specify template**, select **Upload a template file** and upload
   `template.yaml`.
4. Fill in the stack parameters (see [Parameter Reference](#parameter-reference)
   above).
5. On the **Configure stack options** page, acknowledge **CAPABILITY_IAM**.
6. Create the stack. The stack provisions the Lambda function, State Bucket,
   Athena workgroup, EventBridge schedule, IAM roles, and automatically writes the
   `solution-config.json` object to the State Bucket.

### Option B — AWS CLI

`template.yaml` is close to the 51,200-byte `--template-body` limit, so stage
it in S3 and reference it with `--template-url` instead. This also raises
the limit to 1 MB, giving headroom for future growth. Use the same
co-regional bucket from step 3.

Upload the template:

```bash
aws s3 cp template.yaml s3://<your-code-bucket>/template.yaml --region us-east-1 --no-cli-pager  # stage template in S3
```

Create the stack, pointing at the staged template. Escape the commas inside
`SourceBucketNames` with `\,` so the shell passes a single parameter value:

```bash
aws cloudformation create-stack \
  --stack-name s3-replicate-on-tag \
  --template-url https://<your-code-bucket>.s3.us-east-1.amazonaws.com/template.yaml \
  --parameters \
    ParameterKey=CodeLocation,ParameterValue=s3://<your-code-bucket>/package-<version>.zip \
    ParameterKey=SourceBucketNames,ParameterValue=bucket-a\,bucket-b \
  --capabilities CAPABILITY_IAM \
  --region us-east-1 \
  --no-cli-pager  # add ParameterKey=... entries for any optional overrides
```

Wait for completion:

```bash
aws cloudformation wait stack-create-complete --stack-name s3-replicate-on-tag --region us-east-1 --no-cli-pager
```

## 5. Post-Deploy Verification

After the stack reaches `CREATE_COMPLETE`:

1. **Confirm solution-config.json was written.** The stack's `SolutionConfigUri` output
   shows the S3 URI. Check the object exists:
   ```bash
   aws s3 ls "$(aws cloudformation describe-stacks \
     --stack-name <stack-name> \
     --query "Stacks[0].Outputs[?OutputKey=='SolutionConfigUri'].OutputValue" \
     --output text --no-cli-pager)" --no-cli-pager
   ```
2. **Confirm the EventBridge schedule is enabled** in the AWS Console under
   EventBridge → Schedules.
3. (Optional) Invoke the Lambda once manually to confirm it processes the journal
   without errors.

To update the monitored bucket list, update the `SourceBucketNames` stack
parameter and deploy a stack update. The custom resource rewrites the
`solution-config.json` and seeds a checkpoint for each newly added bucket so
processing starts from the update timestamp, not from the beginning of the
journal.

## Operational Notes

- **Single-Region deployment:** the stack covers one account and one Region, and
  every source bucket in `SourceBucketNames` must be co-regional with it. To
  cover buckets in another account or Region, deploy a separate stack there. See
  [Multiple Accounts and Regions](#multiple-accounts-and-regions).

- **Check frequency and cost:** `CheckFrequencyMinutes` controls how often the
  Lambda runs (default 15, range 15–1440). The stack converts it to an
  EventBridge `rate(N minutes)` schedule and derives the `solution-config.json`
  `processing_interval` from it (e.g. `30` → `30m`, `60` → `1h`). Smaller values
  run the Solution more often; because S3 Batch Operations charges per job, this
  can submit more jobs and increase cost, especially when tagging activity is
  spread over time rather than arriving in a single batch.

- **Lambda timeout (900s):** this is the maximum Lambda allows. It bounds a
  single invocation's work across Athena polling and multi-bucket processing.

- **Journal watermark and lookback:** the Solution checkpoints journal progress
  on the S3 Metadata `record_timestamp`. Each run re-scans the journal from
  `watermark - JournalLookbackSeconds` so records delivered late are still
  picked up. A bounded processed-operation window suppresses re-submission of
  already-processed records, so a larger lookback only scans more rows. It
  never replicates an object twice.

- **Code package dependencies:** the package relies on the Lambda runtime's
  built-in boto3 and does not bundle third-party dependencies. The runtime
  boto3 version may be newer than the development pin; this is an accepted
  tradeoff for simpler packaging and automatic security patches.

- **Bucket disabled for rejected request:** when a bucket is disabled with a
  reason stating "rejected by the AWS API before it was sent", the request was
  malformed for the botocore version in the Lambda runtime and fails identically
  on every retry. Usually a defect in the Solution, occasionally a runtime boto3
  that predates a parameter this release sends (see Code package dependencies
  above). Neither is a condition in your account, so re-enabling the bucket
  without deploying a change reproduces the failure. The `disabled_reason` field
  in `solution-config.json` carries this distinction. For all other disable
  reasons (consecutive terminal-job failures), re-enabling after addressing the
  underlying cause is sufficient.

- **Why the code is in S3, not embedded in the template:** the Lambda code is
  delivered as an S3 zip referenced by `CodeLocation`, not inlined in
  `template.yaml`. CloudFormation's only inline-code mechanism,
  `AWS::Lambda::Function` `Code.ZipFile`, places the source in a single file
  named `index`, so a package split across modules cannot be inlined at any
  size. Staging the template in S3 raises the template size limit from 51,200
  bytes to 1 MiB but does not change that. The template uses `ZipFile` only for
  the small single-file custom-resource Lambdas. Keeping the application code in
  S3 also lets you update or version it independently of the template and verify
  it against the release's `.sha256` asset.

## Multiple Accounts and Regions

One stack covers one account and one Region. Deploy a separate stack in every
account and Region holding source buckets, each naming only its local buckets in
`SourceBucketNames`.

Four things pin a stack to the pair it is deployed into:

| What | Why it is fixed to the stack's account and Region |
|---|---|
| `solution-config.json` | The stack writes a single Region into it, taken from `AWS::Region`, and applies that Region to every name in `SourceBucketNames` |
| Journal access | The execution role reads the journal through the `s3tablescatalog` catalog and the `aws-s3` table bucket in the stack's own account and Region |
| Job submission | S3 Batch Operations jobs are created in the stack's Region, against buckets in the stack's account |
| Athena | The workgroup and its query results live in the stack's Region, in that stack's State Bucket |

A source bucket that is not co-regional with the stack fails at runtime rather
than at deploy time. The Solution builds a client for whatever Region the config
names, but the journal, batch job, and Athena resources the execution role is
scoped to exist only in the stack's Region. Keeping `SourceBucketNames`
co-regional is the operator's responsibility.

### What to repeat per pair

- The [Prerequisites](../README.md#prerequisites), in full. The S3 Tables
  analytics-services integration in particular is one-time per account and
  Region, not per bucket.
- A co-regional code bucket holding the Lambda zip, per
  [step 3](#3-create-a-co-regional-code-bucket-and-upload-the-zip). A stack
  cannot read its code from a bucket in another Region.
- A CloudTrail trail recording management events, for any stack that sets
  `AlarmEmail`. See
  [Verifying the CloudTrail trail](#verifying-the-cloudtrail-trail).
- Keys in the stack's own Region, if `KmsKeyArn`, `JournalKmsKeyArn`, or
  `SnsKmsKeyArn` is used. A KMS key is regional.

Each stack keeps its own State Bucket, checkpoints, Athena workgroup, IAM roles,
and schedule. No state is shared between stacks, and no stack is aware the others
exist.

### Seeing every deployment in one place

Metrics are the one thing that aggregates. Set the same `MetricsNamespace` on
every stack and give each a distinct `MetricsDeploymentId`, which adds a
`Deployment` dimension to every metric so stacks sharing a namespace do not
collide. A Region-qualified value such as `us-west-2-prod` reads well in the
CloudWatch console.

CloudWatch metrics are themselves regional, so viewing several Regions together
still needs a cross-account, cross-Region dashboard or a metric stream into one
account. This template creates neither.

Email needs no aggregation. The same address works for `AlarmEmail` on every
stack, since each batch job failure email names the stack, account, and Region it
came from.

### More than one stack in the same account and Region

This works, and is occasionally useful for separating buckets that need different
check frequencies or different alert recipients. Every physical name the template
creates is either derived from the stack name or generated by CloudFormation, so
two stacks with different stack names do not collide on the State Bucket, Athena
workgroup, IAM roles, log groups, or SNS topics.

Two behaviors are worth weighing against simply extending one stack's
`SourceBucketNames`:

| Behavior | Consequence |
|---|---|
| The journal belongs to the bucket, not the stack, and each stack keeps its own checkpoint | Two stacks naming the same source bucket both read that bucket's journal and both submit jobs for the same objects. Give co-located stacks disjoint bucket sets |
| `BatchJobFailureRule` matches every S3 Batch Operations job failure in the account and Region, not only jobs its own stack submitted | Each co-located stack also reports the other's failures, labeled with its own stack name, and counts them toward its own `FailedBatchJobs` alarm |

The second is harmless for replication but misleading when diagnosing, so check
the job ID in the email against the stack you expect before acting on it. One
stack per account and Region avoids both.

## KMS Key Setup

When you set `KmsKeyArn`, `JournalKmsKeyArn`, or `SnsKmsKeyArn`, the key policy
must grant the appropriate principals access to the key. See
[Customer-Managed KMS Keys](../docs/kms.md) for the full key-policy statements,
SNS topic encryption detail, automatic key rotation guidance, and the six-check
verification procedure.

## Lake Formation Mode

When the `s3tablescatalog/aws-s3` Glue catalog is governed by AWS Lake Formation,
the Solution detects this automatically and grants the execution role the
necessary Lake Formation permissions. No manual Lake Formation configuration is
needed. See [Lake Formation accounts](../docs/permissions.md#lake-formation-accounts)
for the full detection logic, administrator elevation, `LFAdminRoleArn`
permissions table, and manual integration test.

## Testing

Deploying from a release does not involve the test suite. For working on the
code, [Testing](../docs/testing.md) covers the unit suite, the end-to-end test
against real AWS resources and its environment variables, and the manual
verification procedures.
