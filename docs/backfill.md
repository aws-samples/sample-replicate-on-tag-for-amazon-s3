# Backfilling After a Replication-Rule Change

Adding or widening a tag-scoped rule does not replicate objects that were tagged before the change. Their journal records sit below the checkpoint watermark and outside the lookback window, so no run picks them up. Matching them from the source side would need a `HeadObject` per object in scope, so the Solution does not do it. Backfill manually instead.

The same applies to objects tagged before initial deployment. The Solution begins processing from the deployment timestamp (see [Journal Start Point](../README.md#journal-start-point)), so tagging activity that predates the stack has the same shape: records below the watermark, unreachable by the scheduled runs, and recoverable with the recipe below.

The recipe below queries the **S3 Metadata live inventory table** for the objects the new rule selects and writes a CSV manifest for an `S3ReplicateObject` Batch Operations job you submit yourself.

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

Create each job with the CSV manifest and operation `S3ReplicateObject`. The job role must trust `batchoperations.s3.amazonaws.com`, hold `s3:InitiateReplication` on the source bucket, and be able to read the manifest object. The stack's own job role (the `BatchOperationsRoleArn` output) satisfies the first two and reads manifests under the State Bucket's `manifests/` prefix, so `UNLOAD`ing there lets you reuse it; a manifest anywhere else needs a role granted read access on that location. Delivery is performed by the bucket's replication configuration role either way.

Job progress is visible in the Batch Operations console; the Solution's completion reporting does not track manually created jobs.
