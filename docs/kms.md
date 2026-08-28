# Customer-Managed KMS Keys

Three independent parameters control encryption with customer-managed keys.
Each is optional and independent of the others.

| Parameter | What it encrypts |
|---|---|
| `KmsKeyArn` | State objects (`state/`, `manifests/`, `completion-reports/`) and Athena query results in the State Bucket |
| `JournalKmsKeyArn` | Athena reads of the S3 Metadata journal tables (only needed when the journal tables use SSE-KMS with a customer-managed key) |
| `SnsKmsKeyArn` | The completion-report and batch-job-failure SNS topics |

Leave any parameter empty to use the default: SSE-S3 for bucket objects,
unencrypted for SNS topics.

## Key policy grants

When you set `KmsKeyArn` or `JournalKmsKeyArn`, the key policy must grant the
stack's `ExecutionRole` (see the `ExecutionRoleArn` stack output) these actions:

- `kms:GenerateDataKey`
- `kms:Decrypt`
- `kms:DescribeKey`

Add these grants after the stack creates the role.

For `KmsKeyArn` specifically, the same three actions are also needed by the
stack's Batch Operations job role (see the `BatchOperationsRoleArn` stack
output), which is the principal S3 Batch Operations reads the manifest as. Both
principals are roles this stack creates, so both are known once the stack exists.

The role already holds those actions in its own identity policy. The key-policy
statement is still required whenever the key's policy does not delegate
authorization to IAM. Without it a job fails to read the manifest even though
`s3:GetObject` is in place.

To find a journal table's KMS key ARN: AWS Console → S3 → Table buckets →
select the table bucket → Properties → Default encryption.

## SNS topic encryption

The completion-report and batch-job-failure SNS topics are unencrypted by
default. SNS has no keyless server-side encryption option (there is no equivalent
of S3's `SSEAlgorithm: AES256`), so encrypting them requires a symmetric
customer-managed KMS key and adds a KMS request charge per publish.

Completion report bodies carry per-bucket object counts, replication outcomes,
matched rule IDs, destination bucket names, and tag and last-modified timestamp
ranges, not object keys or version IDs (see
[Report group fields](completion-reporting.md#report-group-fields)). If that
operational detail is sensitive in your environment, set `SnsKmsKeyArn` to a
symmetric customer-managed key ARN. The stack then sets `KmsMasterKeyId` on both
topics and grants the `ExecutionRole` the KMS actions needed to publish.

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
key. Omitting either statement does not fail the stack update. It fails each
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

Leaving `SnsKmsKeyArn` empty keeps the current behavior and needs no key
policy changes.

## Automatic key rotation

Enable annual automatic rotation on any symmetric customer-managed key you
create for `KmsKeyArn`, `JournalKmsKeyArn`, or `SnsKmsKeyArn`. Rotation replaces
the key's backing cryptographic material each year while the key ARN, key ID,
and any policy referencing it stay the same, so nothing in this Solution needs
to change and previously encrypted objects remain readable.

In CloudFormation, set `EnableKeyRotation: true` on the `AWS::KMS::Key`. For an
existing key: AWS Console → KMS → select the key → Key rotation →
Automatically rotate this KMS key every year. Or:

```bash
aws kms enable-key-rotation --key-id <key-id> --region <region> --no-cli-pager
```

See [Rotating AWS KMS keys](https://docs.aws.amazon.com/kms/latest/developerguide/rotate-keys.html)
for what rotation does and does not cover.

## Verifying a KMS-enabled deployment

A deployment with the KMS parameters empty never executes the KMS-dependent code
paths, so neither the unit suite nor a default-configuration deployment
exercises them. Run the six checks below after enabling a key.

Checks 1-3 are automated: set `S3ROT_TEST_KMS_KEY_ARN` and run
`tests/test_e2e_aws.py` (see [Testing](testing.md#end-to-end-test)). Checks 4-6
are manual.

| # | Check | How |
|---|---|---|
| 1 | State object is SSE-KMS | `HeadObject` on `state/<bucket>.json` returns `ServerSideEncryption: aws:kms` and your key ARN |
| 2 | Manifest is SSE-KMS and readable by the job | The manifest object is `aws:kms`, and the batch job reaches `Complete` with tasks succeeded, proving the Batch Operations role's `kms:Decrypt` works. Nothing is declared to `CreateJob`; the job decrypts using its `RoleArn` |
| 3 | Athena results are SSE-KMS | `HeadObject` on the newest `athena-results/` object. This reflects the workgroup, not the client |
| 4 | The workgroup enforces rather than defers | Run a query against the stack's workgroup passing a deliberately conflicting per-query `ResultConfiguration.EncryptionConfiguration` of `SSE_S3`. The result object must still be `aws:kms` |
| 5 | Both SNS topics are encrypted, and a publish arrives | `get-topic-attributes` reports `KmsMasterKeyId`. Then confirm a real publish: `AWS/SNS` `NumberOfMessagesPublished` increments and `NumberOfNotificationsFailed` stays 0 |
| 6 | The EventBridge publish arrives | Deliberately fail a batch job, then check `AWS/Events` `FailedInvocations` on the batch-job-failure rule is 0 (see [SNS topic encryption](#sns-topic-encryption)). This is the check the key policy most easily breaks |

Two notes on forcing the awkward cases.

**A deliberately failed job** (checks 5 and 6): create a job whose CSV manifest
declares `Fields: [Bucket, Key, VersionId]` but whose rows carry one field. The
job reaches `Failed` with `InvalidManifestContent` within about a minute, which
is a real `JobStatusChanged` event. A job whose *tasks* all fail is not
equivalent at test scale: below 1,000 tasks it reaches `Complete`, and the rule
matches only `Failed` or `Cancelled`.

**The report-missing handler's state write** is the one path that needs state
crafted by hand, because it writes only when a terminal job's completion report
is genuinely missing. Point the bucket's `submission_records` entry at a
terminal job that terminated more than an hour ago, set `manifest_key` to a path
with no report under it, and write that
state object under SSE-S3 first. Then invoke `CompletionReportCheckLambda`: the
object must come back `aws:kms`. Restore the original state object afterwards.
This handler writes the same state objects as the main function, so the check
confirms the key reached it rather than only the main write path.
