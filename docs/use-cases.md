# Use Cases

S3 Replication acts on an object at two points, and only two: when the object is written, and when an S3 Batch Replication job names the object version explicitly. Tagging an object that already exists is neither. The object may now match a tag-scoped rule, but nothing re-evaluates it.

That is why this Solution submits S3 Batch Replication jobs: there is no API to make S3 re-evaluate an existing object against its replication rules. Batch Replication is the only mechanism S3 provides for replicating an object that was written before it qualified.

Two patterns follow, then a variation that applies to both. The patterns differ in what the tag means, not in how the Solution works.

| Pattern | The tag means |
|---|---|
| [Gating on a post-upload check](#gating-on-a-post-upload-check) | This object has passed a check and is now eligible |
| [Copying a selected dataset to another Region](#copying-a-selected-dataset-to-another-region) | This object is part of the set I want moved |

Either pattern can send objects to more than one destination bucket, driven by the tag's value. See [Fanning out to several destination buckets](#fanning-out-to-several-destination-buckets).

Both patterns need the same things: versioning enabled on the source and destination buckets, as with any S3 replication configuration, and a tag-scoped rule on the source bucket whose filter matches the tag you apply. To apply a matching tag across a large set of existing objects, see [Tagging Objects at Scale](tagging-at-scale.md).

## Gating on a post-upload check

The most common reason a tag arrives after the PUT is that an object's eligibility is not known when it is uploaded. Something inspects the object and records the verdict as a tag: a scanner marks it clean, a validation job marks a record approved, a classification job marks a file non-sensitive. Only objects carrying the passing verdict should leave the bucket.

Native replication cannot express this, because the verdict does not exist at the moment replication makes its decision. With this Solution, the tag-scoped rule matches the passing value and nothing else, so a failed or unfinished check means the object is never replicated. The gate is the rule's tag filter, held in the bucket's replication configuration rather than in this Solution.

Two properties make this safe as a control:

- **Default deny.** An object with no tag, or with a non-passing value, does not match the rule. Failure of the checking process leaves objects unreplicated rather than replicated.
- **Re-tagging re-qualifies.** If a later check changes the verdict to the passing value, that is a new tagging operation and the object is picked up on the next run. See [Repeat Tagging](../README.md#repeat-tagging).

### Replicating only malware-scanned objects

[Amazon GuardDuty Malware Protection for S3](https://docs.aws.amazon.com/guardduty/latest/ug/how-malware-protection-for-s3-gdu-works.html) scans newly uploaded objects and, when object tagging is enabled on the protected bucket, adds a predefined tag to each scanned object once the scan finishes:

`GuardDutyMalwareScanStatus`:`<scan result>`

The scan-result values are `NO_THREATS_FOUND`, `THREATS_FOUND`, `UNSUPPORTED`, `ACCESS_DENIED`, and `FAILED`.

GuardDuty applies this tag after the object is written, so replication never acts on it. Pairing GuardDuty tagging with this Solution replicates only the objects GuardDuty has confirmed clean.

To enable this pattern:

1. Enable Malware Protection for S3 on the source bucket with the tagging option turned on. Enable tagging before objects are uploaded, because GuardDuty cannot tag an object whose scan has already run.
2. Add a tag-scoped rule to the source bucket's replication configuration whose tag filter is `GuardDutyMalwareScanStatus` = `NO_THREATS_FOUND`.
3. Deploy this Solution with the bucket in `SourceBucketNames`.

On each run the Solution reads the tagging operations from the journal, matches objects carrying `GuardDutyMalwareScanStatus=NO_THREATS_FOUND`, and submits an S3 Batch Replication job for them.

| GuardDuty tag value | Replicated by the Solution |
|---|---|
| `NO_THREATS_FOUND` | Yes, matches the tag-scoped rule |
| `THREATS_FOUND`, `UNSUPPORTED`, `ACCESS_DENIED`, `FAILED` | No, does not match the rule |

To require a different scan status, or to combine it with other tags, edit the replication rule. No change to this Solution is needed (see [Configuration](../README.md#configuration)).

## Copying a selected dataset to another Region

A dataset that already exists in one Region is often needed in another: close to the compute that will process it, in a Region a partner or team operates in, or alongside an application being stood up elsewhere. Cross-Region reads work, but paying the latency and data transfer on every read is worse than moving the data once.

It is rarely the whole bucket that has to move. It is a subset, selected by something the bucket layout does not express, such as a project, a date range, a customer, or a model training set. Tagging is how you nominate that subset: apply a tag to the objects you want, and the tag-scoped rule carries exactly those objects and nothing else. Widening or narrowing the set later is a tagging operation, not a change to the replication configuration.

To use it:

1. Add a tag-scoped replication rule on the source bucket pointing at the destination, with a tag filter of your choosing.
2. Tag the objects you want copied with that key and value. See [Tagging Objects at Scale](tagging-at-scale.md) for how to apply a tag in bulk without overwriting the tags an object already carries.
3. The Solution reads those tagging operations from the journal and submits an S3 Batch Replication job for the matched objects.

Tagging in waves is fine, and so is tagging objects that have already been replicated. [Repeat Tagging](../README.md#repeat-tagging) covers what happens in each case, including the cost of a resubmission. If you add or widen the rule *after* the objects were tagged, those objects are not picked up; see [Backfilling After a Replication-Rule Change](backfill.md).

### Why Batch Replication rather than Batch Operations Copy

The Solution drives S3 Batch **Replication**, not S3 Batch Operations **Copy**. For a cross-Region dataset copy that matters in two ways.

**Object properties are preserved.** Replication reproduces the source object's system metadata on the destination object, including the original last-modified time and version ID, and carries object tags across. A Copy job writes a new object that gets a new last-modified time and version ID instead. Per the [AWS Storage blog](https://aws.amazon.com/blogs/storage/considering-four-different-replication-options-for-data-in-amazon-s3/), replication is the only S3 method that preserves the source last-modified time. Because the replica is a byte-identical copy, its ETag also matches the source.

**There is no 5 GB per-object limit.** A Copy job supports objects up to 5 GB; copying larger objects requires a [custom multipart-upload Lambda invoked by Batch Operations](https://aws.amazon.com/blogs/storage/copying-objects-greater-than-5-gb-with-amazon-s3-batch-operations/). Batch Replication has no such limit, because the replication engine handles large objects with multipart internally.

| | S3 Batch Operations Copy | This Solution (S3 Batch Replication) |
|---|---|---|
| Last-modified time | New value on copy | Preserved from source |
| Version ID | New version ID | Preserved from source |
| ETag | May differ | Matches source (identical content) |
| Object tags | Copied only if requested | Replicated |
| Max object size | 5 GB (larger needs a custom Lambda) | No practical limit; multipart handled internally |

Preserved last-modified times matter beyond tidiness. Destination-side lifecycle rules, age-based reporting, and anything that reconciles the two Regions by object age all behave as though the data had always been there.

## Fanning out to several destination buckets

This is a variation on both patterns above rather than a pattern in its own right. It applies when the check has more than a pass and a fail outcome, or when the dataset splits into subsets with different homes, and the tag's *value* decides which destination bucket an object goes to.

A source bucket may carry several tag-scoped rules, each with its own tag filter and its own destination bucket. Destinations are buckets, so they may be in different Regions, in different accounts, or in the same Region as the source, whatever each audience needs.

A classification workflow is the clearest example. Objects are uploaded unclassified, a classification job inspects each one and records the result as a tag, and the result decides where the object is allowed to go:

| Tag applied by the classification job | Rule on the source bucket | Outcome |
|---|---|---|
| `Classification=public` | Matches, destination is the bucket serving the external audience | Replicated to the public bucket |
| `Classification=internal` | Matches, destination is the internal analytics bucket | Replicated to the analytics bucket |
| `Classification=restricted` | No rule matches this value | Stays in the source bucket only |

The tag arrives after upload, which is why native replication cannot do this and why the Solution is needed. If your classification is known at upload time, add the tag as part of the PUT and S3 routes the object at write time with no Solution involved.

How the Solution handles the multiple-rule case:

- All of a bucket's tag-scoped rules are evaluated together on each run. Rules with no tag filter are ignored, because S3 already applies those when the object is written.
- Every matched object across every rule goes into **one** S3 Batch Replication job per bucket per run. Rule count does not change the number of jobs, and so does not change cost.
- S3 routes each object according to the rules its **live** tags match when the job runs. If a tag changes between the tagging operation and the job, the newer tag state decides the destination.

One consequence is worth knowing if you use completion reporting: because a single job covers every matched rule, each report entry carries one aggregate outcome plus the list of routing configuration identifiers that matched the object. There is no per-destination outcome breakdown. See [What a destination list does not tell you](completion-reporting.md#what-a-destination-list-does-not-tell-you).
