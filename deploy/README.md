# Deployment Guide

Deploy **Automatic replication after tagging for Amazon S3** as a scheduled
Lambda function using CloudFormation. The stack is created through the AWS
CloudFormation console or the AWS CLI — no CLI build or code checkout is required.

## Prerequisites

- IAM permissions to create CloudFormation stacks, Lambda functions, S3 buckets,
  Athena workgroups, IAM roles, and EventBridge Scheduler schedules.
- Source S3 buckets with existing replication configurations and S3 Metadata
  journal enabled on each bucket.
- An existing IAM replication role attached to each source bucket's replication
  configuration. You do not pass its ARN — at deploy time the stack reads each
  bucket's replication configuration, collects the distinct role ARNs, and scopes
  the Lambda's `iam:PassRole` grant to exactly those roles. Each ARN must be a
  well-formed IAM role ARN in the deploying account; deployment fails if a listed
  bucket has no replication configuration, or if its replication role ARN is
  malformed or belongs to another account.

  The `iam:PassRole` grant carries no `iam:PassedToService` condition, because
  `s3control:CreateJob` does not populate that context key and a statement using
  it would always deny. The specific role ARNs above are the boundary instead. To
  monitor the grant, filter CloudTrail for `PassRole` events naming these roles
  and check that the consuming service is S3 Batch Operations.
- **S3 Metadata catalog integration.** The solution queries the journal through
  Athena's `s3tablescatalog/aws-s3` connector, which requires the `s3tablescatalog`
  federated Glue catalog to exist in the Region. This is a one-time-per-account+region
  setup, not repeated per stack.

  The simplest way is the S3 console: **S3 → Table buckets → Enable integration**
  with AWS analytics services. This registers the `s3tablescatalog` catalog for you.

  Alternatively, register it via the CLI:

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

  **IAM mode vs LF mode:** The command above registers the catalog in IAM mode
  (`IAM_ALLOWED_PRINCIPALS` present). If your account uses AWS Lake Formation as
  the access-control layer and `IAM_ALLOWED_PRINCIPALS` is absent, the solution
  automatically detects this at deploy time and grants the Lambda execution role
  the necessary Lake Formation permissions. See the [LF Mode](#lf-mode) section
  for details.

- **Active CloudTrail trail (management events) in the stack region.** The
  `BatchJobFailureRule` EventBridge rule captures S3 Batch Operations job status
  changes delivered via CloudTrail (`AWS Service Event via CloudTrail`,
  `eventName: JobStatusChanged`). These events only reach the default event bus
  when a trail that records management events is active in the same region as the
  stack. Without an active trail the alarm will never fire, even if batch jobs
  fail. Verify in the CloudTrail console: **Trails → select trail → Management
  events → All**. If no trail exists, create one scoped to the stack region.
  This is required only when `AlarmEmail` is set and the failure alarm is in use;
  the Lambda's replication processing works regardless.

- (Optional) Customer-managed KMS keys. Two independent KMS parameters are
  supported:
  - `KmsKeyArn` — encrypts state objects and Athena query results in the
    State_Bucket.
  - `JournalKmsKeyArn` — encrypts Athena reads of the S3 Metadata journal tables.
    Only needed when the journal tables use SSE-KMS with a customer-managed key.
    To find the key ARN: AWS Console → S3 → Tables → select the table bucket →
    Properties → Default encryption.

  For each key supplied, the key policy must grant the stack's `ExecutionRole`
  the required permissions — see [KMS Key Setup](#kms-key-setup).

## Parameter Reference

### Required

These have no default — you must provide a value.

| Parameter | Description |
|-----------|-------------|
| `CodeLocation` | S3 URI of the Lambda zip, e.g. `s3://my-code-bucket/package.zip` (bucket co-regional with the stack) |
| `SourceBucketNames` | Comma-separated source S3 bucket names monitored by the solution|

### Optional

**Processing config**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CheckFrequencyMinutes` | `15` | How often the solution runs, in minutes (15–1440). Smaller values run more often; because S3 Batch Operations charges per job, this can submit more jobs and increase cost, especially when tagging activity is spread over time rather than arriving in a single batch |
| `LifecycleExpirationDays` | `30` | Days before manifest and Athena result objects expire in the State_Bucket |
| `JournalLookbackSeconds` | `3600` | Seconds to re-scan the journal below the watermark for late-arriving records |

**Scale & performance**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LambdaMemoryMB` | `2048` | Memory in MiB for the main ReplicationLambda function |
| `CompletionCheckMemoryMB` | `256` | Memory in MiB for CompletionReportCheckLambda (report-missing detection, only created when `CompletionNotificationEmail` is set) — sized independently since this function does not read the journal or generate manifests |
| `LambdaTimeoutSeconds` | `900` | Lambda function timeout in seconds (max 900), for ReplicationLambda |
| `JournalReadRowCap` | `500000` | Max journal rows read in one interval, and the single scale knob bounding in-memory manifest size; a larger single-interval tagging burst is processed in bounded partial reads across multiple intervals |
| `ReinvocationChainLimit` | `20` | Max consecutive self-reinvocations draining a capped run's backlog before deferring to the next scheduled trigger |

Raising `JournalReadRowCap` lets each run clear more before capping, at the cost
of Lambda timeout and memory headroom. The solution enforces a safe maximum for
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

The cap is a target, not an exact count. A run reads slightly more when many
tagging operations share the boundary timestamp, never fewer, so no operation is
dropped. These limits reserve headroom for that. See
[Configuration](../README.md#configuration) for how a capped run drains its
backlog.

**Encryption**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `KmsKeyArn` | _(empty)_ | ARN of a customer-managed KMS key for encrypting state objects and Athena results. Leave empty for SSE-S3 |
| `JournalKmsKeyArn` | _(empty)_ | ARN of a customer-managed KMS key encrypting the S3 Metadata journal tables. Leave empty if the journal uses SSE-S3 or an AWS-managed key |
| `SnsKmsKeyArn` | _(empty)_ | ARN of a symmetric customer-managed KMS key for encrypting the completion-report and batch-job-failure SNS topics. Leave empty for unencrypted topics. Requires key-policy statements for `sns.amazonaws.com` and `events.amazonaws.com` or every publish fails — see [SNS Topic Encryption](#sns-topic-encryption) |

When either KMS parameter is set, the key policy must grant the `ExecutionRole` the required permissions — see [KMS Key Setup](#kms-key-setup).

**Networking (VPC)**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `VpcId` | _(empty)_ | VPC ID to deploy the Lambda into. Leave empty to deploy outside any VPC |
| `SubnetIds` | _(empty)_ | (Required when `VpcId` is set) Comma-separated subnet IDs |
| `SecurityGroupIds` | _(empty)_ | (Required when `VpcId` is set) Comma-separated security group IDs |

**Metrics**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MetricsNamespace` | _(empty)_ | CloudWatch custom metrics namespace — any name you choose, e.g. `S3ReplicateOnTag`; created on first publish. Names starting with `AWS/` are reserved. Leave empty to disable metrics |
| `MetricsDeploymentId` | _(empty)_ | Label added as a `Deployment` dimension on every published metric |

**Monitoring**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `AlarmEmail` | _(empty)_ | Email address for S3 Batch Operations job failure notifications. When set, creates an SNS topic and an email subscription; a failed or cancelled batch job sends one readable email with the job ID, status, and a console link. A CloudWatch alarm on the same event exists for console/dashboard visibility but does not itself send email. Requires an active CloudTrail trail in the stack region — see [Prerequisites](#prerequisites) |
| `MaxBatchJobFailures` | `4` | Consecutive S3 Batch Operations job failures (`Failed` or `Cancelled`) for a bucket's job before the solution disables that bucket in `Solution_Config`. Prevents runaway per-job costs from a bucket whose job keeps failing; the failure counter resets on the first successful (`Complete`) job |

**Completion tracking**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `CompletionNotificationEmail` | _(empty)_ | Email address for per-object-version replication completion reports. When set, creates an SNS topic and an email subscription, enables a completion report on every S3 Batch Operations job, and the solution tracks each object version's replication outcome via its native `x-amz-replication-status` header. Leave empty to disable completion tracking entirely — no completion report is requested and no additional IAM setup is needed |
| `CompletionCheckBatchSize` | `2000` | Maximum number of replication-status checks issued per run when completion tracking is enabled |
| `CompletionItemTtlHours` | `168` | Hours a tracked object may await replication confirmation before it is abandoned with outcome `EXPIRED` and removed from tracking. A backstop bounding state-object growth for objects that can never resolve; minimum 24, since it must exceed both real replication time and several run intervals |

Setting `CompletionNotificationEmail` requires one additional IAM grant not
covered by [`deploy/iam-policy.json`](iam-policy.json): the replication role
attached to each source bucket's Replication_Configuration (`RoleArn` passed
to S3 Batch Operations) needs `s3:PutObject` to the State_Bucket under the
`completion-reports/` prefix, so S3 Batch Operations can write the job
completion report there. This is opt-in — required only when
`CompletionNotificationEmail` is set, not a baseline grant on every
deployment. The stack's `ExecutionRole` already reads the report back
through its existing `ScratchBucketReadWrite` grant; no additional
`ExecutionRole` statement is needed. Completion tracking never accesses the
destination account or region — it reads only the source object's own
replication-status header.

**Lake Formation**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `LFAdminRoleArn` | _(empty)_ | ARN of an existing Lake Formation data lake administrator role. Leave empty to let the solution self-elevate when the catalog is in LF mode |

The `Solution_Config` `processing_interval` is not a parameter — it is derived automatically from `CheckFrequencyMinutes` (e.g. `30` → `30m`, `60` → `1h`).

## 1. Download Release Artifacts

Download all three files from the project [release](../releases) (GitLab Releases for internal
releases; GitHub Releases for public distribution):

- `template.yaml` — the CloudFormation template
- `package.zip` — the Lambda code package
- `package.zip.sha256` — the SHA256 checksum for `package.zip`

## 2. Verify the Checksum

Compute the SHA256 checksum of the downloaded `package.zip`:

```bash
sha256sum package.zip      # Linux
shasum -a 256 package.zip  # macOS
```

Compare the output against the contents of `package.zip.sha256`. The hex digests
must match before you proceed. A mismatch indicates a corrupted or tampered
download.

## 3. Create a Co-Regional Code Bucket and Upload the Zip

The Lambda code package must be stored in a bucket that is **in the same AWS
region as the target stack**. A cross-region code source causes stack creation to
fail.

1. Create or select an S3 bucket in the same region where you will deploy the
   stack.
2. Upload `package.zip` to the bucket (any key, with or without a prefix). The
   resulting S3 URI (for example, `s3://my-code-bucket/releases/v1.2.3/package.zip`)
   is the `CodeLocation` parameter.

## 4. Create the Stack

Deploy with either the CloudFormation console or the AWS CLI.

### Option A — CloudFormation console

1. Open the CloudFormation console in the target region.
2. Choose **Create stack** → **With new resources (standard)**.
3. Under **Specify template**, select **Upload a template file** and upload
   `template.yaml`.
4. Fill in the stack parameters (see [Parameter Reference](#parameter-reference)
   above).
5. On the **Configure stack options** page, acknowledge **CAPABILITY_IAM**.
6. Create the stack. The stack provisions the Lambda function, State_Bucket,
   Athena workgroup, EventBridge schedule, IAM roles, and automatically writes the
   `Solution_Config` object to the State_Bucket.

### Option B — AWS CLI

`template.yaml` is close to the 51,200-byte `--template-body` limit, so stage
it in S3 and reference it with `--template-url` instead — this also raises
the limit to 1 MB, giving headroom for future growth. Use the same
co-regional bucket from step 3.

Upload the template:

```bash
aws s3 cp template.yaml s3://my-code-bucket/template.yaml --region us-east-1 --no-cli-pager  # stage template in S3
```

Create the stack, pointing at the staged template. Escape the commas inside
`SourceBucketNames` with `\,` so the shell passes a single parameter value:

```bash
aws cloudformation create-stack \
  --stack-name s3-replicate-on-tag \
  --template-url https://my-code-bucket.s3.us-east-1.amazonaws.com/template.yaml \
  --parameters \
    ParameterKey=CodeLocation,ParameterValue=s3://my-code-bucket/package.zip \
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

1. **Confirm Solution_Config was written.** The stack's `SolutionConfigUri` output
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
`Solution_Config` automatically.

## Operational Notes

- **Single-region deployment:** the stack is deployed once per account+region.
  ALL source buckets in `SourceBucketNames` must be co-regional with the stack.
  To monitor buckets in another region, deploy a separate stack in that region.

- **Check frequency and cost:** `CheckFrequencyMinutes` controls how often the
  Lambda runs (default 15, range 15–1440). The stack converts it to an
  EventBridge `rate(N minutes)` schedule and derives the `Solution_Config`
  `processing_interval` from it (e.g. `30` → `30m`, `60` → `1h`). Smaller values
  run the solution more often; because S3 Batch Operations charges per job, this
  can submit more jobs and increase cost, especially when tagging activity is
  spread over time rather than arriving in a single batch.

- **Lambda timeout (900s):** this is the maximum Lambda allows. It bounds a
  single invocation's work across Athena polling and multi-bucket processing.

- **Journal watermark and lookback:** the solution checkpoints journal progress
  on the S3 Metadata `record_timestamp`. Each run re-scans the journal from
  `watermark - JournalLookbackSeconds` so records delivered late are still
  picked up. A bounded processed-operation window suppresses re-submission of
  already-processed records, so a larger lookback only scans more rows — it
  never replicates an object twice.

- **Code_Package dependencies:** the package relies on the Lambda runtime's
  built-in boto3 and does NOT bundle third-party dependencies. The runtime
  boto3 version may be newer than the development pin; this is an accepted
  tradeoff for simpler packaging and automatic security patches.

- **Bucket disabled for rejected request:** when a bucket is disabled with a
  reason stating "rejected by the AWS API before it was sent", the cause is a
  code defect in the Solution, not a condition in your account. Re-enabling
  the bucket without deploying a fix will reproduce the failure on the next
  interval. The `disabled_reason` field in `solution-config.json` carries this
  distinction. For all other disable reasons (consecutive terminal-job
  failures), re-enabling after addressing the underlying cause is sufficient.

- **Why the code is in S3, not embedded in the template:** the Lambda code is
  delivered as an S3 zip referenced by `CodeLocation`, not inlined in
  `template.yaml`. CloudFormation's only inline-code mechanism,
  `AWS::Lambda::Function` `Code.ZipFile`, is capped at 4,096 characters and a
  single file, so a multi-module package cannot be inlined. Staging the template
  in S3 raises the template size limit to 1 MiB but does not change this — inline
  code is still limited to one 4,096-character file. The template uses `ZipFile`
  only for the small single-file custom-resource Lambdas. Keeping the application
  code in S3 also lets you update or version it independently of the template and
  verify it against `package.zip.sha256`.

## KMS Key Setup

When you set `KmsKeyArn` or `JournalKmsKeyArn`, the key policy must grant the
stack's `ExecutionRole` (see the `ExecutionRoleArn` stack output) these actions
on the key:

- `kms:GenerateDataKey`
- `kms:Decrypt`
- `kms:DescribeKey`

Add these grants after the stack creates the role. For `KmsKeyArn`, the S3
replication role used by S3 Batch Operations must also read KMS-encrypted
manifests — grant it `kms:Decrypt` (and `kms:GenerateDataKey` for SSE-KMS
writes) on the same key.

To find a journal table's KMS key ARN: AWS Console → S3 → Table buckets → select
the table bucket → Properties → Default encryption.

### Enable automatic key rotation

Enable annual automatic rotation on any symmetric customer-managed key you
create for `KmsKeyArn`, `JournalKmsKeyArn`, or `SnsKmsKeyArn`. Rotation replaces
the key's backing cryptographic material each year while the key ARN, key ID,
and any policy referencing it stay the same, so nothing in this Solution needs
to change and previously encrypted objects remain readable.

In CloudFormation, set `EnableKeyRotation: true` on the `AWS::KMS::Key`. For an
existing key: AWS Console → KMS → select the key → Key rotation → Automatically
rotate this KMS key every year. Or:

```bash
aws kms enable-key-rotation --key-id <key-id> --region <region> --no-cli-pager  # Enable annual rotation
```

See [Rotating AWS KMS keys](https://docs.aws.amazon.com/kms/latest/developerguide/rotate-keys.html)
for what rotation does and does not cover.

## SNS Topic Encryption

The completion-report and batch-job-failure SNS topics are unencrypted by
default. SNS has no keyless server-side encryption option (there is no
equivalent of S3's `SSEAlgorithm: AES256`), so encrypting them requires a
symmetric KMS key and adds a KMS request charge per publish.

Completion report bodies carry object keys and version IDs — see
[Completion Reporting](../README.md#completion-reporting). If those keys are
sensitive in your environment, set `SnsKmsKeyArn` to a symmetric
customer-managed key ARN. The stack then sets `KmsMasterKeyId` on both topics
and grants the `ExecutionRole` the KMS actions needed to publish.

**The key policy is your responsibility, and publishes fail without it.** Add
both statements to the key referenced by `SnsKmsKeyArn`:

```json
{
  "Effect": "Allow",
  "Principal": { "Service": "sns.amazonaws.com" },
  "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": "*"
}
```

```json
{
  "Effect": "Allow",
  "Principal": { "Service": "events.amazonaws.com" },
  "Action": ["kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": "*"
}
```

The first lets SNS encrypt and decrypt message bodies. The second is required
because the batch-job-failure notification is published by an EventBridge rule
target rather than by the Lambda, so EventBridge needs its own access to the
key. Omitting either statement does not fail the stack update — it fails each
publish at runtime, so the notification silently stops arriving.

Verify by triggering a notification after enabling the key, then checking two
metrics for that window:

| Metric | Correct key policy | Missing `events.amazonaws.com` |
|---|---|---|
| `AWS/Events` `FailedInvocations`, dimension `RuleName` = the batch-job-failure rule | 0 | 1 per event |
| `AWS/SNS` `NumberOfMessagesPublished`, dimension `TopicName` = the failure topic | 1 per event | 0 |

The rule's CloudWatch Logs target keeps succeeding either way, so the event
still appears in the batch-job-failures log group when the SNS publish is
failing. Presence of the log entry is not evidence the email was sent.

Leaving `SnsKmsKeyArn` empty keeps the current behaviour and needs no key
policy changes.

## Verifying a KMS-enabled deployment

Every KMS-dependent path is a branch that a deployment with the KMS parameters
empty never executes, so a defect there survives both the unit tests and a
default-configuration deployment. Two shipped defects sat in exactly that blind
spot. Verify all six checks after enabling a key.

Checks 1–3 are automated: set `S3ROT_TEST_KMS_KEY_ARN` and run
`tests/test_e2e_aws.py` (see [End-to-End Test Environment
Variables](#end-to-end-test-environment-variables)). Checks 4–6 are manual.

| # | Check | How |
|---|---|---|
| 1 | State object is SSE-KMS | `HeadObject` on `state/<bucket>.json` returns `ServerSideEncryption: aws:kms` and your key ARN |
| 2 | Manifest is SSE-KMS and readable by the job | The manifest object is `aws:kms`, and the batch job reaches `Complete` with tasks succeeded — proving the replication role's `kms:Decrypt` works. Nothing is declared to `CreateJob`; the job decrypts using its `RoleArn` |
| 3 | Athena results are SSE-KMS | `HeadObject` on the newest `athena-results/` object. This reflects the workgroup, not the client |
| 4 | The workgroup enforces rather than defers | Run a query against the stack's workgroup passing a deliberately conflicting per-query `ResultConfiguration.EncryptionConfiguration` of `SSE_S3`. The result object must still be `aws:kms` |
| 5 | Both SNS topics are encrypted, and a publish arrives | `get-topic-attributes` reports `KmsMasterKeyId`. Then confirm a real publish: `AWS/SNS` `NumberOfMessagesPublished` increments and `NumberOfNotificationsFailed` stays 0 |
| 6 | The EventBridge publish arrives | Deliberately fail a batch job, then check `AWS/Events` `FailedInvocations` on the batch-job-failure rule is 0 — see [SNS Topic Encryption](#sns-topic-encryption). This is the check the key policy most easily breaks |

Two notes on forcing the awkward cases.

**A deliberately failed job** (checks 5 and 6): create a job whose CSV manifest
declares `Fields: [Bucket, Key, VersionId]` but whose rows carry one field. The
job reaches `Failed` with `InvalidManifestContent` within about a minute, which
is a real `JobStatusChanged` event. A job whose *tasks* all fail is not
equivalent — it reaches `Complete`, and the rule only matches `Failed` or
`Cancelled`.

**The report-missing handler's state write** is the one path that needs state
crafted by hand, because it writes only when a terminal job's completion report
is genuinely missing. Point the bucket's `submission_records` entry at a
terminal job whose `Report.Enabled` was `false` and which terminated more than
an hour ago, set `manifest_key` to a path with no report under it, and write that
state object under SSE-S3 first. Then invoke `CompletionReportCheckLambda`: the
object must come back `aws:kms`. Restore the original state object afterwards.
This is the exact regression fixed in 0.1.13 — the handler had no key and
silently downgraded every state object it touched.

## LF Mode

When the `s3tablescatalog/aws-s3` Glue catalog is governed by AWS Lake Formation
(i.e. `IAM_ALLOWED_PRINCIPALS` is absent from `CreateTableDefaultPermissions`),
the Lambda execution role requires explicit Lake Formation grants on each journal
namespace before Athena can query it.

The solution detects catalog mode automatically at stack CREATE and UPDATE time
using an inline Lambda custom resource (`LFPermissionsGranterFunction`). No
manual LF configuration is needed.

**How it works:**

1. At deploy time, the `ExecuteLFPermissionsGranter` custom resource calls
   `glue:GetCatalog` to check whether `IAM_ALLOWED_PRINCIPALS` is present.
2. **IAM mode** (default): the custom resource returns immediately with no LF
   calls.
3. **LF mode**: the custom resource grants the execution role `SELECT` +
   `DESCRIBE` on the `b_<bucket-name>` namespace for each bucket in
   `SourceBucketNames`. These grants are re-issued on stack UPDATE, so adding a
   bucket and updating the stack automatically provisions the new grant. On stack
   DELETE, grants are revoked (best-effort).

**LF admin elevation:**

To issue Lake Formation grants, the `LFGranterRole` Lambda role must be an LF
data lake administrator.

| Scenario | What to do |
|---|---|
| You manage LF admins centrally | Set `LFAdminRoleArn` to an existing LF admin role ARN. |
| No dedicated LF admin role | Leave `LFAdminRoleArn` empty. The solution elevates `LFGranterRole` automatically with exponential backoff retry. The elevation is reversed on stack DELETE. |

**Manual integration test (LF-mode account):**

1. Register the catalog without `IAM_ALLOWED_PRINCIPALS`.
2. Deploy the stack; confirm the `ExecuteLFPermissionsGranter` custom resource
   succeeds and `grant_permissions` calls appear in the LFGranterRole Lambda logs.
3. Run an Athena query against the journal namespace and confirm no
   `INSUFFICIENT_PRIVILEGES` error.

## End-to-End Test Environment Variables

The repository includes an end-to-end integration test (`tests/test_e2e_aws.py`)
that runs the full pipeline against real AWS resources. This test requires
environment variables to avoid hardcoding account-specific values:

| Variable | Description | Example |
|----------|-------------|---------|
| `S3ROT_TEST_ACCOUNT` | AWS account ID | `"123456789012"` |
| `S3ROT_TEST_REGION` | AWS region | `"us-west-2"` |
| `S3ROT_TEST_SOURCE_BUCKET` | Source bucket name | `"s3rot-test-source-123456789012"` |
| `S3ROT_TEST_DEST_BUCKET` | Destination bucket name | `"s3rot-test-dest-123456789012"` |
| `S3ROT_TEST_STATE_BUCKET` | State bucket name | `"s3rot-test-state-123456789012"` |
| `S3ROT_TEST_ROLE_ARN` | IAM role ARN for replication | `"arn:aws:iam::123456789012:role/s3rot-test-replication-role"` |
| `S3ROT_TEST_WORKGROUP` | Athena workgroup name | `"s3rot-test-workgroup"` |
| `S3ROT_TEST_OUTPUT_LOCATION` | S3 URI for Athena query results | `"s3://s3rot-test-state-123456789012/athena-results/"` |

One optional variable:

| Variable | Description | Example |
|----------|-------------|---------|
| `S3ROT_TEST_KMS_KEY_ARN` | Symmetric CMK ARN. When set, the state object and the manifest are written with SSE-KMS and verified with `HeadObject`, as is the Athena result the workgroup produced | `"arn:aws:kms:us-west-2:123456789012:key/<key-id>"` |

Set it only against a stack deployed with the same key as `KmsKeyArn`. The
Athena check reads the workgroup's own encryption, so a run against a stack with
`KmsKeyArn` empty fails that assertion by design. The key policy must include
the statements from [KMS Key Setup](#kms-key-setup), including `kms:Decrypt` for
the replication role, or the batch job cannot read the manifest.

When any required environment variable is unset, the test skips with exit code 3
and an explanatory message. This allows the test suite to collect and pass in
environments where the test resources are not provisioned.

To run the test with all variables set:

```bash
export S3ROT_TEST_ACCOUNT="123456789012"
export S3ROT_TEST_REGION="us-west-2"
export S3ROT_TEST_SOURCE_BUCKET="s3rot-test-source-123456789012"
export S3ROT_TEST_DEST_BUCKET="s3rot-test-dest-123456789012"
export S3ROT_TEST_STATE_BUCKET="s3rot-test-state-123456789012"
export S3ROT_TEST_ROLE_ARN="arn:aws:iam::123456789012:role/s3rot-test-replication-role"
export S3ROT_TEST_WORKGROUP="s3rot-test-workgroup"
export S3ROT_TEST_OUTPUT_LOCATION="s3://s3rot-test-state-123456789012/athena-results/"
python -m pytest tests/test_e2e_aws.py -xvs
```

The test exercises the full pipeline from journal query through manifest
generation to batch job submission, and verifies checkpoint advancement.

