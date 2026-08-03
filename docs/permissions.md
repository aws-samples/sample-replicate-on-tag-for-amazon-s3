# Required AWS Permissions

The full least-privilege policy for the Lambda execution role is
[`deploy/iam-policy.json`](../deploy/iam-policy.json). It grants only source-side
actions. Nothing in this Solution accesses the destination account or region.

## Execution role permissions

| Permission | Purpose |
|---|---|
| `s3:GetReplicationConfiguration` on source buckets | Derive tag-scoped replication rules |
| Athena query submission and result reads, Glue catalog reads, S3 Tables metadata reads, `lakeformation:GetDataAccess` | Query the S3 Metadata journal |
| `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the State Bucket | Read and write checkpoints, manifests, and Athena results |
| `s3:CreateJob`, `iam:PassRole` | Submit S3 Batch Operations replication jobs |
| `sns:Publish` on the failure topic when `AlarmEmail` is set | Send the batch-job-failure and bucket-disabled alerts |
| `logs:PutLogEvents` on the failure log group, always | Write those same alerts to CloudWatch Logs regardless of `AlarmEmail` |

The replication role passed to S3 Batch Operations is not a parameter. At deploy
time the stack reads each source bucket's replication configuration and scopes
the `iam:PassRole` grant to exactly those roles. Deployment fails if a listed
bucket has no replication configuration, or if its replication role ARN is
malformed or belongs to another account.

## Lake Formation accounts

If your account uses AWS Lake Formation as the access-control layer for Glue
catalogs, the Solution detects this at deploy time and grants the execution role
the Lake Formation permissions it needs. No manual Lake Formation configuration
is required. See [LF Mode](../deploy/README.md#lf-mode).

## Customer-managed KMS keys

Setting `KmsKeyArn` encrypts state objects and Athena results with a
customer-managed key. The inventory manifest and its data files are then
encrypted with SSE-KMS as well.

Each supplied key's policy must grant the stack's `ExecutionRole` the required
actions. For `KmsKeyArn`, the replication role used by S3 Batch Operations must
also be able to read KMS-encrypted manifests. See
[KMS Key Setup](../deploy/README.md#kms-key-setup) for the exact grants.

## Completion reporting

Setting `CompletionNotificationEmail` requires one grant beyond
[`deploy/iam-policy.json`](../deploy/iam-policy.json): the replication role
attached to each source bucket needs `s3:PutObject` on the State Bucket under
the `completion-reports/` prefix, so S3 Batch Operations can write the job
completion report. This is opt-in, not a baseline grant. See the
[parameter reference](../deploy/README.md#parameter-reference) for the detail.
