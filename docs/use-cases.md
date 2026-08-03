# Use Cases

The Solution replicates any object whose post-creation tag matches a tag-scoped replication rule on the source bucket. Two common patterns follow. To apply a matching tag across a large set of existing objects, see [Tagging objects at scale](tagging-at-scale.md).

## Copying an existing dataset to another Region with property preservation

To copy an existing dataset to a bucket in another Region, add a tag-scoped replication rule pointing at the destination, then tag the objects you want copied with the key/value that rule matches — see [Tagging objects at scale](tagging-at-scale.md) for how to apply that tag in bulk without overwriting existing tags. The Solution reads those tagging operations from the journal and submits an S3 Batch Operations replication job for the matched objects.

The Solution drives S3 Batch **Replication**, not S3 Batch Operations **Copy**. For a cross-Region dataset copy that matters in two ways:

- **Object properties are preserved.** Replication reproduces the source object's system metadata — including the original last-modified time and version ID — on the destination object, and carries object tags across. A Batch Operations Copy job writes a new object that gets a new last-modified time and version ID instead. Per the [AWS Storage blog](https://aws.amazon.com/blogs/storage/considering-four-different-replication-options-for-data-in-amazon-s3/), replication is the only S3 method that preserves the source last-modified time. Because the replica is a byte-identical copy, its ETag also matches the source.
- **No 5 GB per-object limit.** A Batch Operations Copy job supports objects up to 5 GB; copying larger objects requires a [custom multipart-upload Lambda invoked by Batch Operations](https://aws.amazon.com/blogs/storage/copying-objects-greater-than-5-gb-with-amazon-s3-batch-operations/). Batch Replication has no such limit — the replication engine handles large objects with multipart internally, so no extra Lambda is needed.

| | S3 Batch Operations Copy | This Solution (S3 Batch Replication) |
|---|---|---|
| Last-modified time | New value on copy | Preserved from source |
| Version ID | New version ID | Preserved from source |
| ETag | May differ | Matches source (identical content) |
| Object tags | Copied only if requested | Replicated |
| Max object size | 5 GB (larger needs a custom Lambda) | No practical limit; multipart handled internally |

Versioning must be enabled on both source and destination buckets for replication, as with any S3 replication configuration.

## Replicating only malware-scanned objects

[Amazon GuardDuty Malware Protection for S3](https://docs.aws.amazon.com/guardduty/latest/ug/how-malware-protection-for-s3-gdu-works.html) scans newly uploaded objects and, when object tagging is enabled on the protected bucket, adds a predefined tag to each scanned object once the scan finishes:

`GuardDutyMalwareScanStatus`:`<scan result>`

The scan-result values are `NO_THREATS_FOUND`, `THREATS_FOUND`, `UNSUPPORTED`, `ACCESS_DENIED`, and `FAILED`.

GuardDuty applies this tag after the object is created, so native S3 Replication never triggers on it — S3 Replication evaluates tag-scoped rules only at object-creation time. This is the exact gap the Solution closes: pairing GuardDuty tagging with this Solution replicates only objects that GuardDuty has confirmed clean.

To enable this pattern:

1. Enable Malware Protection for S3 on the source bucket with the tagging option turned on. Enable tagging before objects are uploaded — GuardDuty cannot tag an object whose scan already ran.
2. Add a tag-scoped rule to the source bucket's replication configuration whose tag filter is `GuardDutyMalwareScanStatus` = `NO_THREATS_FOUND`.
3. Deploy this Solution with the bucket in `SourceBucketNames`.

On each run the Solution reads the tagging operations from the journal, matches objects carrying `GuardDutyMalwareScanStatus=NO_THREATS_FOUND`, and submits a Batch Operations replication job for them. Objects tagged `THREATS_FOUND` — or any non-clean value — never match the rule and are never replicated.

| GuardDuty tag value | Replicated by the Solution |
|---|---|
| `NO_THREATS_FOUND` | Yes — matches the tag-scoped rule |
| `THREATS_FOUND`, `UNSUPPORTED`, `ACCESS_DENIED`, `FAILED` | No — does not match the rule |

The tag filter lives in the bucket's replication configuration, not in this Solution (see [Configuration](../README.md#configuration)). To require a different scan status or combine it with other tags, edit the replication rule; no change to this Solution is needed.
