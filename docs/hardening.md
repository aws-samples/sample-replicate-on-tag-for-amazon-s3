# Hardening Options

The Solution leaves three protections off by default to keep deployment simple.
Each one is listed here with what to change to turn it on.

## State Bucket access logging

The State Bucket has server access logging disabled. To record access to it,
either add a `LoggingConfiguration` to the `StateBucket` resource in
`deploy/template.yaml`, targeting a log-delivery bucket you create first, or
enable a CloudTrail S3 data event selector on the State Bucket.

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
[SNS Topic Encryption](../deploy/README.md#sns-topic-encryption) for the policy
statements and how to verify delivery.

Completion report bodies carry object keys and version IDs, which is the reason
to encrypt these topics if those keys are sensitive in your environment.
