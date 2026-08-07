# Tagging Objects at Scale

This Solution replicates an object when a tag is added to it after creation and
that tag matches a tag-scoped rule in the source bucket's replication
configuration (see [Use Cases](use-cases.md)). To trigger
replication for a large set of existing objects, you apply that matching tag in
bulk with [S3 Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html).

There are two ways to do this. Choose based on whether the objects already carry
tags you need to keep.

| | Replace all object tags | Invoke Lambda to append a tag |
|---|---|---|
| Mechanism | Native Batch Operations operation | Batch Operations invokes a Lambda per object |
| Existing tags | Overwritten: the whole tag set is replaced | Preserved: the new tag is merged in |
| Setup | None beyond the job | Deploy a Lambda function |
| Use when | Objects have no tags you need to keep | Objects already carry tags (for example a GuardDuty scan-status tag) |

## Option A — Replace all object tags

The native [Replace all object tags](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-put-object-tagging.html)
operation calls `PutObjectTagging` on each object in the manifest. `PutObjectTagging`
replaces the object's entire tag set, so any tags already on the object are
discarded unless you include them in the job's tag set.

This is the simplest option, but it is unsafe when objects carry tags you need to
keep. In particular, do not use it on a bucket protected by GuardDuty Malware
Protection: it would overwrite the `GuardDutyMalwareScanStatus` tag that the
replication rule depends on.

## Option B — Invoke Lambda to append a tag (recommended)

There is no native "append a tag" operation, because `PutObjectTagging` is always
a full replace. To add a tag while keeping existing tags, run a Batch Operations
[Invoke AWS Lambda function](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-invoke-lambda.html)
job whose Lambda reads the current tag set, merges in the new tag, and writes the
merged set back.

The tag key and value the Lambda applies must match the tag filter on the
bucket's replication rule. That match is what causes this Solution to replicate
the object on its next run.

```python
import urllib.parse
import boto3

s3 = boto3.client("s3")

TAG_KEY = "replicate"
TAG_VALUE = "true"


def lambda_handler(event, context):
    task = event["tasks"][0]
    task_id = task["taskId"]
    # Schema 1.0 (default for CSV / S3 Inventory manifests) provides s3BucketArn.
    # Schema 2.0 provides s3Bucket (a plain bucket name) instead.
    bucket = task["s3BucketArn"].split(":")[-1]
    # Keys arrive URL-encoded and must be decoded before use.
    key = urllib.parse.unquote_plus(task["s3Key"])
    version_id = task.get("s3VersionId")

    kw = {"Bucket": bucket, "Key": key}
    if version_id:
        kw["VersionId"] = version_id

    try:
        existing = s3.get_object_tagging(**kw).get("TagSet", [])
        merged = [t for t in existing if t["Key"] != TAG_KEY]
        merged.append({"Key": TAG_KEY, "Value": TAG_VALUE})

        # S3 allows at most 10 tags per object.
        if len(merged) > 10:
            return _result(
                task_id, "PermanentFailure", "object already has 10 tags; cannot append"
            )

        s3.put_object_tagging(**kw, Tagging={"TagSet": merged})
        return _result(task_id, "Succeeded", "tag appended")
    except s3.exceptions.ClientError as error:
        code = error.response["Error"]["Code"]
        retryable = {"InternalError", "SlowDown", "RequestTimeout", "ThrottlingException"}
        result_code = "TemporaryFailure" if code in retryable else "PermanentFailure"
        return _result(task_id, result_code, code)


def _result(task_id, result_code, result_string):
    return {
        "invocationSchemaVersion": "1.0",
        "treatMissingKeysAs": "PermanentFailure",
        "results": [
            {
                "taskId": task_id,
                "resultCode": result_code,
                "resultString": result_string,
            }
        ],
    }
```

Notes:

- **Result contract.** Batch Operations reads `resultCode` per task. `Succeeded`
  and `PermanentFailure` are terminal; `TemporaryFailure` is retried. The
  `resultString` is recorded in the job's completion report (truncated to 1,024
  characters).
- **Schema version.** The example reads `s3BucketArn`, which the default 1.0
  schema provides. If you configure the job with schema 2.0, read `s3Bucket`
  instead.
- **Versioned buckets.** The Lambda forwards `s3VersionId` so it tags the exact
  version listed in the manifest. Grant `s3:GetObjectVersionTagging` and
  `s3:PutObjectVersionTagging` in addition to the non-versioned actions.

## IAM

- **Lambda execution role:** `s3:GetObjectTagging` and `s3:PutObjectTagging` on
  the target objects (plus the `...VersionTagging` variants for versioned
  buckets).
- **Batch Operations job role:** `lambda:InvokeFunction` on the Lambda, and
  `s3:GetObject` on the manifest object. See
  [Granting permissions for Batch Operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-iam-role-policies.html).

## Manifest

Both options need a manifest listing the objects to tag: either an
[S3 Inventory report](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html)
or a CSV you supply. Create the Batch Operations job in the same Region as the
bucket holding the objects; tagging always runs in that Region.

### Generating the manifest automatically

Instead of supplying an Inventory report or CSV, you can have S3 generate the
manifest for you from filter criteria, using the
[`--manifest-generator` flag](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html#automatically-generate-manifest-file)
on `aws s3control create-job` (console support is limited to Batch Replication
jobs; CLI/SDK/REST support this for any job type, including Option A and B
above). This avoids running an Inventory report or hand-building a CSV first.

```bash
aws s3control create-job \
  --account-id <ACCOUNT-ID> \
  --region <REGION> \
  --operation '{"LambdaInvoke": {"FunctionArn": "<LAMBDA-FUNCTION-ARN>"}}' \
  --manifest-generator '{
    "S3JobManifestGenerator": {
      "ExpectedBucketOwner": "<ACCOUNT-ID>",
      "SourceBucket": "arn:aws:s3:::<SOURCE-BUCKET>",
      "ManifestOutputLocation": {
        "Bucket": "arn:aws:s3:::<MANIFEST-OUTPUT-BUCKET>",
        "ManifestFormat": "S3InventoryReport_CSV_20211130"
      },
      "Filter": {
        "KeyNameConstraint": { "MatchAnyPrefix": ["<PREFIX/PATH>"] }
      },
      "EnableManifestOutput": true
    }
  }' \
  --report '{"Bucket":"arn:aws:s3:::<REPORT-BUCKET>","Prefix":"<REPORT-PREFIX>","Format":"Report_CSV_20180820","Enabled":true,"ReportScope":"AllTasks"}' \
  --priority 10 \
  --role-arn <IAM-ROLE-ARN>
```

The generator's `Filter` supports key prefix/suffix/substring, object creation
date range, storage class, server-side encryption type, and
`EligibleForReplication` (objects that match the bucket's existing replication
configuration). It does **not** support filtering by an object's current tag
value, so this option cannot target "objects already tagged X." If your
selection criteria is a tag, list the objects yourself (Athena against
[S3 Metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-metadata.html),
or `s3api list-objects-v2` plus `get-object-tagging`) and supply that list as a
CSV manifest instead.

Source: [S3 Batch Operations manifest generator, `create-job` reference](https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-create-job.html#automatically-generate-manifest-file).

This is a separate manifest from the one this Solution generates for the
*replication* job it submits after your tagging job completes. You supply the
manifest for the tagging job above; this Solution builds the replication job's
manifest for you, in S3 Inventory Report format, from the objects it matches in
the journal each run (see [Configuration](../README.md#configuration) in the main
README). No manual manifest step is needed for replication itself.
