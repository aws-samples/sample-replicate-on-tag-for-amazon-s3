# Hardening Options

The Solution leaves three protections off by default to keep deployment simple.
Each one is listed here with what to change to turn it on.

## State Bucket access logging

The State Bucket has server access logging disabled. To record access to it,
choose one:

- **Recommended:** deliver server access logs to a CloudWatch Logs log group,
  and enable the S3 Tables integration to mirror them into an Apache Iceberg
  table queryable with Athena. This needs no log-delivery bucket and no
  lifecycle rules on this Solution's part; retention is set once on the log
  group. See [Query Amazon S3 access logs instantly with CloudWatch and S3
  Tables](https://aws.amazon.com/blogs/storage/query-amazon-s3-access-logs-instantly-with-cloudwatch-and-s3-tables/).
- Add a `LoggingConfiguration` to the `StateBucket` resource in
  `deploy/template.yaml`, targeting a log-delivery bucket you create first.
- Enable a CloudTrail S3 data event selector on the State Bucket.

## State Bucket versioning

The State Bucket has versioning disabled. If you enable it, add
`NoncurrentVersionExpiration` and `ExpiredObjectDeleteMarker` lifecycle rules in
the same change. The existing lifecycle rules expire current versions only, so
without version-specific rules the noncurrent versions accumulate indefinitely.

## SNS topic encryption

The completion-report and batch-job-failure topics are unencrypted by default.
SNS offers no keyless server-side encryption, so encrypting them requires a
symmetric customer-managed KMS key and adds a KMS request charge per publish.

No template edit is needed. Set the `SnsKmsKeyArn` parameter to the key ARN and
the stack sets `KmsMasterKeyId` on both topics.

The key policy is your responsibility. It must grant both `sns.amazonaws.com`
and `events.amazonaws.com` the `kms:Decrypt` and `kms:GenerateDataKey` actions.
EventBridge needs its own grant because the batch-job-failure notification is
published by a rule target rather than by the Lambda. Omitting either statement
does not fail the stack update, it fails each publish at runtime, so the
notification silently stops arriving. See
[SNS topic encryption](kms.md#sns-topic-encryption) for the key-policy
statements and how to verify delivery.

Completion report bodies carry per-bucket object counts, replication outcomes,
matched rule IDs, destination bucket names, and tag and last-modified timestamp
ranges. They do not carry object keys or version IDs. Encrypt these topics if
that operational detail is sensitive in your environment. See
[Report group fields](completion-reporting.md#report-group-fields) for the full
body.
