# Completion Reporting

Reference detail for the replication reports summarised in [Completion Reporting](../README.md#completion-reporting). That section covers what is always written and what setting `CompletionNotificationEmail` turns on. This page covers the tracking mechanics, the outcomes an object can reach, and the fields each report group carries.

Everything here applies only when `CompletionNotificationEmail` is set. Leave it empty and only the completion-report CSV diagnostic runs.

## What tracking does per run

- Once a job's completion report confirms processing, each object version is tracked with one `x-amz-replication-status` check per run, capped at `CompletionCheckBatchSize` (default 2,000/run).
- One SNS message per source bucket per run covers every object that resolved and passed the tag-quiescence check. A report's length is set by how many rule and destination combinations the bucket has, not by how many objects replicated, so a bucket resolving tens of thousands of objects in one interval still reports in one message. A bucket with hundreds of combinations splits across as many messages as SNS's 256 KB limit requires, always between groups and never within one. An object leaves tracking only once the message covering it publishes successfully.
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

## Report group fields

The report body is a `groups` list, declared by `format_version: 2`. Objects
are aggregated by `(source_bucket, matched_rules, destinations)` rather than
listed individually, so the three values every object in a group shares are
stated once. Each group carries:

| Field | Meaning |
|---|---|
| `source_bucket` | The bucket the objects in this group were replicated from |
| `matched_rules` | IDs of the replication rules that matched these objects |
| `destinations` | Buckets those rules replicate to |
| `count` | Number of objects in this group |
| `outcome_counts` | Breakdown of outcomes within the group |
| `tagged_at_range` | `[earliest, latest]` timestamps of when these objects were tagged |
| `last_modified_range` | `[earliest, latest]` last-modified times of these objects |

`matched_rules` and `destinations` are empty lists when the Solution holds no
routing for the objects in the group. A range is omitted entirely when no
object in the group holds that timestamp, and spans only the objects that do.

Object keys and version IDs are not in the email. A report of several hundred
keys and version UUIDs cannot be read, and the per-object detail is in the S3
Batch Operations completion report CSV on the state bucket under
`completion-reports/`, which is where to look to find out which specific
object failed.

The top-level fields are `summary`, `format_version`, `source_bucket`,
`item_count`, `outstanding`, and `outcome_counts`. `item_count` equals the sum
of every group's `count`.

### Example report

A 563-object all-success batch to one destination produces:

```json
{
  "summary": "egummett-fresh-eu-west-1: 563 objects replicated successfully. No action needed. No objects remain in tracking.",
  "format_version": 2,
  "source_bucket": "egummett-fresh-eu-west-1",
  "item_count": 563,
  "outstanding": 0,
  "outcome_counts": {"COMPLETE": 563},
  "groups": [
    {
      "source_bucket": "egummett-fresh-eu-west-1",
      "matched_rules": ["tagtest-to-us-west-2"],
      "destinations": ["egummett-fresh-us-west-2"],
      "count": 563,
      "outcome_counts": {"COMPLETE": 563},
      "tagged_at_range": ["2026-08-08T21:24:28.335000+00:00", "2026-08-08T21:24:30.737000+00:00"],
      "last_modified_range": ["2025-11-18T20:13:09+00:00", "2025-11-18T21:15:16+00:00"]
    }
  ]
}
```

## What a destination list does not tell you

`destinations` names where the objects in a group were bound for, not what happened at each destination. The `outcome_counts` is a single aggregate across every destination: S3 reports `COMPLETED` only once replication to all destinations succeeds, and `FAILED` when one or more fail. So a group with `outcome_counts: {"FAILED": 3}` listing two destinations means at least one of the two failed for each of those 3 objects, not that both did. Determining which requires the destination buckets themselves, which this Solution does not read.

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
