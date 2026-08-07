# Testing

Deploying the Solution needs none of this. These are the tests and manual
procedures for working on the code itself.

## Unit suite

```bash
pip install -e ".[dev]"  # install the package with its test dependencies
pytest                   # run the unit suite
```

The suite runs without AWS credentials. Tests that need real resources skip
themselves, so a clean checkout passes.

## End-to-end test

`tests/test_e2e_aws.py` runs the full pipeline against real AWS resources: it
queries the journal, deduplicates, excludes archived objects, generates a
manifest, submits a Batch Operations job, and verifies the checkpoint advanced.

It is a standalone script rather than a set of pytest tests, so run it with
`python`, not `pytest`. Under `pytest` it collects nothing and exits 5, which is
easy to mistake for success. The filename still begins with `test_` so that the
unit suite's own collection ignores it by having no test functions to collect.

It reads every resource name from an environment variable, so no account-specific
value is committed. When any required variable is unset it exits 3 with a message
naming the variable.

Exit codes:

| Code | Meaning |
|------|---------|
| 0 | Success. A job was submitted, or the pipeline completed with nothing to submit |
| 2 | No new journal events, or every candidate was filtered out. Tag objects in the source bucket and re-run |
| 3 | A required environment variable is unset |
| 1 | Any unexpected failure |

### Required variables

| Variable | Description | Example |
|----------|-------------|---------|
| `S3ROT_TEST_ACCOUNT` | AWS account ID | `"123456789012"` |
| `S3ROT_TEST_REGION` | AWS Region | `"us-west-2"` |
| `S3ROT_TEST_SOURCE_BUCKET` | Source bucket name | `"amzn-s3-demo-source-123456789012"` |
| `S3ROT_TEST_DEST_BUCKET` | Destination bucket name | `"amzn-s3-demo-dest-123456789012"` |
| `S3ROT_TEST_STATE_BUCKET` | The stack's `StateBucketName` output. Must be the State Bucket the stack created, not a bucket of your own | `"s3-replicate-on-tag-solution-123456789012-us-west-2-an"` |
| `S3ROT_TEST_ROLE_ARN` | Replication role ARN (the bucket's own replication configuration role, which performs delivery) | `"arn:aws:iam::123456789012:role/amzn-s3-demo-replication-role"` |
| `S3ROT_TEST_BATCHOPS_ROLE_ARN` | The stack's `BatchOperationsRoleArn` output, passed as each batch job's `RoleArn` | `"arn:aws:iam::123456789012:role/s3-replicate-on-tag-BatchOperationsRole-abc123"` |
| `S3ROT_TEST_WORKGROUP` | The stack's `AthenaWorkGroupName` output | `"s3-replicate-on-tag-workgroup"` |
| `S3ROT_TEST_OUTPUT_LOCATION` | S3 URI for Athena query results, under the same State Bucket | `"s3://s3-replicate-on-tag-solution-123456789012-us-west-2-an/athena-results/"` |

`S3ROT_TEST_STATE_BUCKET` and `S3ROT_TEST_WORKGROUP` must be the values the stack
created, which the commands below read from the stack outputs. The Batch
Operations job role holds `s3:GetObject` only on the State Bucket's `manifests/`
prefix, so a manifest written anywhere else parses but every task then fails
because the job cannot read it. The workgroup carries the Glue catalog context
the journal query needs.

The test writes to the same checkpoint object the deployed Lambda uses,
`state/<source-bucket>.json`, and advances it on a successful submission. Expect
the next scheduled run to skip the journal records the test consumed. Run it
against a deployment where that is acceptable.

### Optional variable

| Variable | Description | Example |
|----------|-------------|---------|
| `S3ROT_TEST_KMS_KEY_ARN` | Symmetric CMK ARN. When set, the state object and the manifest are written with SSE-KMS and verified with `HeadObject`, as is the Athena result the workgroup produced | `"arn:aws:kms:us-west-2:123456789012:key/<key-id>"` |

Set it only against a stack deployed with the same key as `KmsKeyArn`. The Athena
assertion reads the workgroup's own encryption setting, so a run against a stack
with `KmsKeyArn` empty fails that assertion by design.

The key policy must carry the statements from
[Customer-Managed KMS Keys](kms.md#key-policy-grants), including `kms:Decrypt`
for the Batch Operations role. Without it the batch job cannot read the
manifest.

### Running it

Run `pip install -e ".[dev]"` first, so `import src` resolves. Without the
editable install, set `PYTHONPATH` to the repository root instead.

The three stack-created values are read from the stack outputs rather than typed,
which is what keeps them correct.

```bash
export S3ROT_TEST_ACCOUNT="123456789012"
export S3ROT_TEST_REGION="us-west-2"
export S3ROT_TEST_SOURCE_BUCKET="amzn-s3-demo-source-123456789012"
export S3ROT_TEST_DEST_BUCKET="amzn-s3-demo-dest-123456789012"
export S3ROT_TEST_ROLE_ARN="arn:aws:iam::123456789012:role/amzn-s3-demo-replication-role"
export S3ROT_TEST_STATE_BUCKET="$(aws cloudformation describe-stacks --stack-name s3-replicate-on-tag --region us-west-2 --query "Stacks[0].Outputs[?OutputKey=='StateBucketName'].OutputValue" --output text --no-cli-pager)"
export S3ROT_TEST_BATCHOPS_ROLE_ARN="$(aws cloudformation describe-stacks --stack-name s3-replicate-on-tag --region us-west-2 --query "Stacks[0].Outputs[?OutputKey=='BatchOperationsRoleArn'].OutputValue" --output text --no-cli-pager)"
export S3ROT_TEST_WORKGROUP="$(aws cloudformation describe-stacks --stack-name s3-replicate-on-tag --region us-west-2 --query "Stacks[0].Outputs[?OutputKey=='AthenaWorkGroupName'].OutputValue" --output text --no-cli-pager)"
export S3ROT_TEST_OUTPUT_LOCATION="s3://$S3ROT_TEST_STATE_BUCKET/athena-results/"
python tests/test_e2e_aws.py  # a script, not a pytest module
```

The run needs journal records newer than the stored checkpoint. S3 Metadata
delivers journal records within a few minutes of the change but not instantly, so
after tagging objects in the source bucket, expect to wait before the records
appear and the test has anything to submit. Until then it exits 2.

## Verifying a KMS-enabled deployment

A deployment with the KMS parameters empty never executes the KMS-dependent code
paths, so the unit suite and a default-configuration deployment do not exercise
them. [Customer-Managed KMS Keys](kms.md#verifying-a-kms-enabled-deployment) has
the six checks to run after enabling a key. Three of them are the automated
assertions above, driven by `S3ROT_TEST_KMS_KEY_ARN`; the other three are manual.

## Manual integration test in a Lake Formation account

The Lake Formation grant path only executes when the `s3tablescatalog/aws-s3`
Glue catalog is governed by Lake Formation, which a normal deployment is not. To
exercise it:

1. Register the catalog without `IAM_ALLOWED_PRINCIPALS`.
2. Deploy the stack. Confirm the `ExecuteLFPermissionsGranter` custom resource
   succeeds and that `grant_permissions` calls appear in the `LFGranterRole`
   Lambda logs.
3. Run an Athena query against the journal namespace and confirm it returns
   without `INSUFFICIENT_PRIVILEGES`.

[Required AWS Permissions](permissions.md#lake-formation-accounts) covers the
detection logic and the `LFAdminRoleArn` permissions this path needs.
