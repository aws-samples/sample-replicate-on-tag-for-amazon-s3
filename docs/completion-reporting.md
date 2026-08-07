# Completion Reporting

Reference detail for the per-object replication reports summarised in [Completion Reporting](../README.md#completion-reporting). That section covers what is always written and what setting `CompletionNotificationEmail` turns on. This page covers the tracking mechanics, the outcomes an object can reach, and the fields each report entry carries.

Everything here applies only when `CompletionNotificationEmail` is set. Leave it empty and only the completion-report CSV diagnostic runs.

## What tracking does per run

- Once a job's completion report confirms processing, each object version is tracked with one `x-amz-replication-status` check per run, capped at `CompletionCheckBatchSize` (default 2,000/run).
- One SNS message per source bucket per run covers every object that resolved and passed the tag-quiescence check. Reports split across as many messages as needed to stay within SNS's 256 KB limit, so a bucket resolving tens of thousands of objects in one interval still reports. An object leaves tracking only once the message covering it publishes successfully.
- An alert fires if a job's completion report has not appeared within 1 hour of that job finishing.

## Outcomes

Every tracked object reaches a terminal outcome. `GONE` and `EXPIRED` exist so that an object cannot be tracked indefinitely.

| Outcome | Meaning |
|---|---|
| `COMPLETE` | S3 reported `x-amz-replication-status: COMPLETED` on the source object |
| `PENDING`, `FAILED` | Verbatim `x-amz-replication-status` value |
| `UNKNOWN` | The check succeeded but the object carried no replication-status header |
| `GONE` | The object version no longer exists, so replication can never be confirmed |
| `EXPIRED` | The object stayed unresolved past `CompletionItemTtlHours` and was abandoned |

## Report entry fields

An entry carries the following fields. Object keys and version IDs are not redacted in this report, unlike the structured logs described in [Monitoring](../README.md#monitoring).

| Field | Meaning |
|---|---|
| `object_key`, `version_id` | Identify the object version the entry covers |
| `outcome` | One aggregate outcome from the table above |
| `source_bucket` | The bucket the object was replicated from |
| `outstanding` | Report-level, not per entry: how many of the bucket's objects still await an answer (see [Knowing when a batch has fully landed](#knowing-when-a-batch-has-fully-landed)) |
| `tagged_at` | When the tag that matched the object was applied |
| `last_modified` | The object version's last-modified time |
| `matched_rules` | IDs of the replication rules that matched the object |
| `destinations` | Buckets those rules replicate to |

`tagged_at`, `last_modified`, `matched_rules`, and `destinations` are omitted from an entry when the Solution does not hold that value for the object.

## What a destination list does not tell you

`destinations` names where an object was bound for, not what happened at each one. The `outcome` is a single aggregate across every destination: S3 reports `COMPLETED` only once replication to all destinations succeeds, and `FAILED` when one or more fail. So a `FAILED` entry listing two destinations means at least one of the two failed, not that both did. Determining which requires the destination buckets themselves, which this Solution does not read.

## Report format and email subject

Each report opens with a plain-English `summary` line stating the bucket, the object count, what happened to those objects, whether anything needs attention, and how many objects remain in tracking. Outcomes are listed most severe first, so failures lead even when they are the minority. The email subject carries the same verdict, so an inbox of reports can be triaged without opening them.

## Knowing when a batch has fully landed

A report covers the objects that reached a terminal answer since the previous report for that bucket, not the objects you tagged together. A tagging wave whose objects replicate at different speeds is therefore reported across several emails.

The `outstanding` field is what closes the loop. It counts the tracked objects for that bucket still awaiting a terminal answer once the report was sent, and the `summary` states it in words:

| `outstanding` | Summary reads | Means |
|---|---|---|
| 0 | `No objects remain in tracking.` | Nothing is left awaiting confirmation for this bucket |
| 1 | `1 object remains in tracking.` | One more object to resolve; a later report will cover it |
| n | `n objects remain in tracking.` | More reports are coming |

A non-zero count is also appended to the email subject, so a subject with no `still tracking` marker is the last report for that wave.

Two limits on reading it as an absolute all-clear. An object enters tracking only once its Batch Operations job's completion report has been read, so a zero means nothing is awaiting confirmation rather than that no tagging work exists upstream of that point. And when one run's report is split across several messages to fit SNS's size limit, every message carries the same count, since the split is a transport detail with no ordering rather than a sequence to count down.

## Permissions

The Batch Operations role the stack creates holds the `s3:PutObject` grant the report write needs. See [Completion reporting](permissions.md#completion-reporting).
