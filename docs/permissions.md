# Required AWS Permissions

The full least-privilege policy for the Lambda execution role is
[`deploy/iam-policy.json`](../deploy/iam-policy.json). It grants only source-side
actions. Nothing in this Solution accesses the destination account or Region.

## Execution role permissions

| Permission | Purpose |
|---|---|
| `s3:GetReplicationConfiguration` on source buckets | Derive tag-scoped replication rules |
| Athena query submission and result reads, Glue catalog reads, S3 Tables metadata reads, `lakeformation:GetDataAccess` | Query the S3 Metadata journal |
| `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the State Bucket | Read and write checkpoints, manifests, Athena results, and completion reports |
| `s3:CreateJob`, `s3:PutJobTagging`, `iam:PassRole` | Submit and tag S3 Batch Operations replication jobs |
| `s3:DescribeJob` | Read a job's status, termination date, and invoked-task count, for resolving completion outcomes and for failed-job recovery |
| `lambda:InvokeFunction` on the Solution's own function | Self-reinvoke when a run approaches its timeout |
| `cloudwatch:PutMetricData` in `MetricsNamespace` when that parameter is set | Publish the Solution's custom metrics |
| `sns:Publish` on the report topic when `CompletionNotificationEmail` is set | Send the completion report email |
| `sns:Publish` on the failure topic when `AlarmEmail` is set | Send the batch-job-failure and bucket-disabled alerts |
| `logs:PutLogEvents`, `logs:CreateLogStream` on the failure log group, always | Write those same alerts to CloudWatch Logs regardless of `AlarmEmail` |

`iam:PassRole` is scoped to the ARN of the one role the stack creates for its
batch jobs, described next. It carries no `iam:PassedToService` condition,
because `s3control:CreateJob` does not populate that context key and a statement
using it would always deny.

## Batch Operations job role

S3 Batch Operations runs a job as the job's own IAM role, not as the execution
role that submitted it. The stack creates that role and passes it as each job's
`RoleArn`. Its ARN is the `BatchOperationsRoleArn` stack output. Nothing about it
is a parameter, and no role you own needs editing.

| Grant | Resource | Needed for |
|---|---|---|
| `s3:InitiateReplication` | `<bucket>/*` for every bucket in `SourceBucketNames` | Initiating replication for each object in the manifest |
| `s3:GetObject` | State Bucket `manifests/*` | Reading the manifest |
| `s3:PutObject` | State Bucket `completion-reports/*` | Writing the completion report, which every job produces |
| `kms:Decrypt`, `kms:GenerateDataKey`, `kms:DescribeKey` | `KmsKeyArn`, when set | Reading an SSE-KMS manifest |

Its trust policy names `batchoperations.s3.amazonaws.com` and nothing else.

The role holds no replication permissions: no `s3:ReplicateObject`, no
destination bucket access, and nothing on a source bucket beyond
`s3:InitiateReplication`. Delivery is performed by the replication role attached
to each source bucket's replication configuration, which the Solution never
passes to a job and never modifies. Those are two distinct roles in
[S3 Batch Replication](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-batch-replication-policies.html).

Both the role and the State Bucket belong to the deploying account, so the
manifest and report grants sit in the role's own identity policy. The Solution
reads and writes no bucket policy at any point.

A job that fails with `Reading the manifest is forbidden` processed no objects
and means this role's manifest grant did not apply. Since the stack owns the
grant, that is a defect in the deployment rather than a condition in your
account.

## Config resource role

At stack create and update, a custom resource writes the internal config object
the Lambda reads at startup, and seeds a starting checkpoint for each source
bucket so the Solution begins at the deployment timestamp rather than at the
beginning of the journal (see [Journal Start Point](../README.md#journal-start-point)).

| Grant | Resource | Needed for |
|---|---|---|
| `s3:PutObject`, `s3:DeleteObject` | State Bucket `config/solution-config.json` | Writing the config object on create and update, removing it on delete |
| `s3:PutObject` | State Bucket `state/*` | Seeding a starting checkpoint for each newly added source bucket. `PutObject` only, so the role cannot delete a checkpoint |
| `kms:GenerateDataKey`, `kms:Decrypt`, `kms:DescribeKey` | The key named in `KmsKeyArn` | Writing both objects under SSE-KMS. Omitted when `KmsKeyArn` is empty |

This role runs only during stack create and update, and holds nothing else
beyond writing its own logs. It has no read access to the State Bucket and no
access to any source bucket.

The `state/*` grant is a prefix rather than a single key because the checkpoint
key carries the bucket name, and `SourceBucketNames` is a parameter, so the
individual keys are not known when the policy is written. It is split into its
own statement so that `s3:DeleteObject`, which the config object needs for stack
delete, is not also granted over checkpoints: deleting a checkpoint rewinds that
bucket to the start of the journal, and the handler never does it. The seed write
is conditional on the object not already existing, so it cannot overwrite a
checkpoint the Lambda has since advanced. Deleting the stack removes only the
config object; checkpoints remain.

## Custom resource caller trust

Nothing in CloudFormation stops an in-account principal holding
`lambda:InvokeFunction` from calling a custom resource function with an event of
its own. Two controls apply to the three that change something, the config
resource and both Lake Formation granters:

- Each privileged value comes from an environment variable the template sets, not
  from the invoking event. The principal registered as a Lake Formation
  administrator, the principal and namespace list the journal grants name, and the
  State Bucket and config key the config resource writes are all fixed at deploy
  time.
- Each handler compares the event's `StackId` against the stack's own and stops
  before any change when they differ, responding `FAILED` with a non-specific
  reason. Both Lake Formation granters also log the rejection at `ERROR` in their
  own log groups.

The [code package validation](#code-package-validation-role) resource only reads,
so it carries neither control.

Neither control is visible during a normal deployment. They matter when some
other principal in the account can invoke the functions, which is the case
whenever `lambda:InvokeFunction` is granted broadly, as it often is to a
deployment automation role.

## Code package validation role

Before the stack builds a Lambda around the zip that `CodeLocation` names, a
custom resource reads that zip and checks it contains `src/lambda_handler.py` at
its root. A package missing that entry fails the stack, rather than reaching
`CREATE_COMPLETE` with a Lambda that raises `ImportModuleError` on every
scheduled run. The usual cause is uploading the auto-generated
**Source code (zip)** asset from the Releases page, which nests everything under
a version directory, instead of `package-<version>.zip`.

| Grant | Resource | Needed for |
|---|---|---|
| `s3:GetObject` | The bucket named in `CodeLocation` | Reading the package to check its layout |

This role runs only during stack create and update, and holds nothing else
beyond writing its own logs.

The check is skipped, and the stack proceeds, whenever the object cannot be
read. The role carries no `kms:Decrypt`, because no parameter names the code
bucket's key, so a code bucket encrypted with a customer-managed key returns
`AccessDenied` here. A cross-account code bucket or a restrictive bucket policy
does the same. Failing the deployment in those cases would block configurations
that work correctly, so the rule is to fail on a package proven wrong and skip
when the layout cannot be determined. A skip is recorded as a `WARN` line in the
custom resource's own log group and does not appear in stack events, so on a
deployment using an encrypted or cross-account code bucket, treat the package as
unverified and rely on the checksum step in
[Verify the Checksum](../deploy/README.md#2-verify-the-checksum).

This is a guard against the wrong file, not an integrity control. It shows the
package is shaped like this Solution's, not that it is authentic. Anyone who can
write to the code bucket can satisfy it. Authenticity remains the published
`.sha256` asset and that same verification step.

## Lake Formation accounts

When the `s3tablescatalog/aws-s3` Glue catalog is governed by AWS Lake Formation
(i.e. `IAM_ALLOWED_PRINCIPALS` is absent from `CreateTableDefaultPermissions`),
the Lambda execution role requires explicit Lake Formation grants on each journal
namespace before Athena can query it.

The Solution detects catalog mode automatically at stack CREATE and UPDATE time
using an inline Lambda custom resource (`LFPermissionsGranterFunction`). No
manual Lake Formation configuration is needed.

**How it works:**

1. At deploy time, the `ExecuteLFPermissionsGranter` custom resource calls
   `glue:GetCatalog` to check whether `IAM_ALLOWED_PRINCIPALS` is present.
2. **IAM mode** (default): the custom resource returns immediately with no Lake
   Formation calls.
3. **Lake Formation mode**: the custom resource grants the execution role `SELECT` +
   `DESCRIBE` on the `b_<bucket-name>` namespace for each bucket in
   `SourceBucketNames`. These grants are re-issued on stack UPDATE, so adding a
   bucket and updating the stack automatically provisions the new grant. On stack
   DELETE, grants are revoked (best-effort).

### Lake Formation administrator elevation

To issue Lake Formation grants, the `LFGranterRole` Lambda role must be a Lake
Formation data lake administrator.

| Scenario | What to do |
|---|---|
| You manage Lake Formation administrators centrally | Set `LFAdminRoleArn` to an existing Lake Formation administrator role ARN. It must carry the IAM permissions listed below. |
| No dedicated Lake Formation administrator role | Leave `LFAdminRoleArn` empty. In Lake Formation mode the Solution registers `LFGranterRole` as an administrator itself, retrying with exponential backoff. In IAM mode, the default, it registers nothing. The registration is removed on stack DELETE. |

The registration is gated on catalog mode, detected the same way as the grant
path above. An IAM-mode account therefore gains no account-wide Lake Formation
administrator, because it needs no grants. If mode detection cannot complete, the
custom resource fails rather than assuming a mode, which fails the deployment.

On stack DELETE the removal is attempted whatever the current mode, so an
administrator registered while the catalog was in Lake Formation mode stays
removable after a mode change. A removal failure is logged at `ERROR` in the
`LFAdminGranter` function's log group and stack deletion continues, so the stack
does not wedge in `DELETE_FAILED`. An account-wide administrator entry then
outlives the stack, with that log line as the only signal, so confirm the
account's administrators after deleting a stack that registered one:

```bash
aws lakeformation get-data-lake-settings --region "$REGION" --no-cli-pager
```

### Permissions required on a supplied `LFAdminRoleArn`

When `LFAdminRoleArn` is set, the granter assumes that role and issues every
Lake Formation and Glue call as it. The stack's own `LFGranterRole` policy does
not apply to those calls, so the supplied role needs the following in its own
identity policy. Being a Lake Formation data lake administrator is not
sufficient: administrators hold implicit Lake Formation permissions but still
need these IAM permissions.

| Action | Resource | Why |
|---|---|---|
| `lakeformation:GrantPermissions` | `*` | Issue the journal grants |
| `lakeformation:RevokePermissions` | `*` | Revoke them on stack DELETE |
| `glue:GetCatalog` | `arn:aws:glue:REGION:ACCOUNT:catalog` and `catalog/s3tablescatalog`, `catalog/s3tablescatalog/aws-s3` | Detect whether the catalog is in IAM or LF mode |
| `glue:GetDatabase` | `arn:aws:glue:REGION:ACCOUNT:database/s3tablescatalog/aws-s3/*` | Required to grant on a named database |
| `glue:GetTable` | `arn:aws:glue:REGION:ACCOUNT:table/s3tablescatalog/aws-s3/*` | Required to grant on a named table |

A supplied role missing `glue:GetDatabase` fails deployment with
`AccessDeniedException ... Insufficient Glue permissions to access database
b_<bucket>`. A role missing `glue:GetCatalog` fails earlier, during mode
detection, naming the bare account-root catalog ARN as the resource. The trust
policy must allow the stack's `LFGranterRole` to assume the role.

### Verifying this path

The grant path only executes in a Lake Formation account, so a normal deployment never
exercises it. [Testing](testing.md#manual-integration-test-in-a-lake-formation-account)
has the manual procedure.

## Customer-managed KMS keys

Three independent KMS parameters are supported: `KmsKeyArn` (state objects and
Athena results), `JournalKmsKeyArn` (journal table reads), and `SnsKmsKeyArn`
(SNS topics). Each requires key-policy grants specific to that parameter. For
`KmsKeyArn` the key policy must name two stack-created principals, the
`ExecutionRoleArn` and the `BatchOperationsRoleArn` outputs, because the stack
cannot edit the policy on a key you supply.

See [Customer-Managed KMS Keys](kms.md) for the full key-policy statements,
SNS topic encryption detail, automatic key rotation, and the verification
procedure.

## Completion reporting

Every S3 Batch Operations job writes a completion report to the State Bucket. The
`s3:PutObject` grant on `completion-reports/*` is unconditional on the
[Batch Operations job role](#batch-operations-job-role), alongside its
manifest-read grant.

The Solution resolves each tracked object's outcome by reading that report back
from the State Bucket, which the execution role's existing State Bucket grant
already covers.

Setting `CompletionNotificationEmail` creates the report topic and grants the
execution role `sns:Publish` on it. That grant is the only permission the email
report adds.
