# Changelog

All notable changes to this project are documented here.

## [1.1.0] — 2026-08-27

> **Action required before upgrading.**
>
> Drop the `CompletionCheckBatchSize` and `CompletionItemTtlHours` parameters from
> your `update-stack` command and from any parameter file. This release removes
> both, and CloudFormation rejects a parameter the template does not declare, so an
> otherwise unchanged command fails until you do. See
> [Upgrading from 1.0.1](#upgrading-from-101).
>
> The SNS completion-report payload becomes `format_version: 3` and removes the
> `outstanding` field, with nothing taking its name. A subscriber that parses the
> payload (SQS, Lambda, or HTTPS rather than email) needs updating. See
> [Delivery and tracking state](docs/completion-reporting.md#delivery-and-tracking-state).

- Completion outcomes now come from the S3 Batch Operations completion report
  already written for every job, rather than from polling each source object's
  `x-amz-replication-status`. A succeeded task resolves to `COMPLETE`, a failed task
  to `FAILED`, an unrecognized row to `UNKNOWN`.
- A report that is missing, partial, or unreadable stays retryable instead of being
  marked processed. Every result object's MD5 and row count is checked against
  `DescribeJob` first.
- A terminal job whose report has existed for 48 hours without being consumed now
  raises the same alert as a missing report, identifying it as present but
  unconsumed. Previously it retried silently, which mattered because
  `LifecycleExpirationDays` expires the `completion-reports/` prefix and can make a
  report permanently unreadable with no service fault.
- Every outstanding Batch Operations job for a source bucket is tracked, with
  submission records keyed by job ID. Under 1.0.1, submitting while a previous job
  was still running discarded that job's ID: its report was never read, the
  report-missing check could not see it, and if it later failed its objects were
  abandoned with no retry and no alert. Buckets of large objects are the exposed
  case, since a job's duration follows replication throughput rather than object
  count. The report-missing check and its alert suppression are now per job, and
  the auto-disable circuit breaker counts consecutive failing intervals correctly
  with several jobs outstanding.
- **New stack parameter `MaxConcurrentJobsPerBucket`** (default 3, minimum 1,
  maximum 10) caps how many Batch Operations jobs may be outstanding at once for one
  source bucket. At the limit the bucket is skipped for that run, emitting a
  `submission_deferred_job_in_flight` audit entry and a `SubmissionDeferred` metric;
  pending tagging stays pending and is picked up whole once a job finishes. At most
  one job is submitted per bucket per run either way, so `BatchJobsSubmitted` stays
  0 or 1. Set it to 1 for strict serialization.
- A tagging burst larger than `JournalReadRowCap` now drains instead of stalling the
  bucket. The cap covers the whole journal read: the `JournalLookbackSeconds` window
  the Solution re-scans for late-arriving records, and the new records above it.
  Under 1.0.1, once that window alone held a cap's worth of rows, a run read only
  records it had already submitted, so the checkpoint never advanced and every later
  run repeated the same read. At the shipped defaults, tagging 600,000 objects in
  one burst was enough, and nothing alarmed: the run completed successfully and
  reported zero submissions.

  At least 20% of the cap is now reserved for new records, so a run always
  progresses and the total read stays within the cap; the memory ceilings in
  [Scale & performance](deploy/README.md#parameter-reference) are unchanged. A
  backlog large enough to need the other 80% shortens the re-scan window while it
  lasts, which reduces tolerance for late-arriving records; that emits an error
  entry and a new per-bucket `JournalTailShortened` metric, and
  `journal_read_capped` gains `tail_rows`, `new_row_budget`, and `tail_shortened`.
  Sizing the window adds one single-row Athena query per bucket per run, and
  bounding it a second only when needed; see
  [Athena queries per run](docs/cost.md#athena-queries-per-run).

  **`JournalReadRowCap`'s minimum is now 2.** At 1 the re-scan window's share
  rounds down to nothing, which leaves no read window that is both memory-bounded
  and lossless, so 1 is rejected at deploy time.
- The completion-report payload becomes `format_version: 3`. The `outstanding` field
  is **removed** and nothing takes its name; `outstanding_jobs` and
  `submission_deferred` are added.

  `outstanding` counted objects still awaiting a terminal answer, and
  `outstanding: 0` answered "has everything I tagged arrived?". No count of stored
  items can carry that meaning now, because an object enters tracking only once its
  job's completion report has been read: by then the question is settled for it.
  `outstanding_jobs` answers it at the level where the work is actually pending, and
  the all-clear clause turns on it. Read `null` as "in progress or unknown", never
  as zero: it means the bucket's jobs were not checked, which includes a bucket
  skipped as disabled. See
  [Delivery and tracking state](docs/completion-reporting.md#delivery-and-tracking-state).
- The outcomes `GONE` and `EXPIRED` are counted as `UNKNOWN` in a report's summary.
  Having no outcome phrase, they were dropped from the summary and the actionable
  total, and a report of only such objects produced an empty clause list.
- Removed the source-object polling that completion tracking previously performed,
  and everything that existed only to serve it: the **`CompletionCheckBatchSize` and
  `CompletionItemTtlHours` stack parameters**, and the execution role's
  `s3:GetObject`, `s3:GetObjectVersion`, and `s3:ListBucket` grants on source
  buckets. The incremental source-object request cost of completion tracking falls
  to zero; State Bucket, Lambda, and SNS costs remain.

### Upgrading from 1.0.1

An ordinary stack update. The State Bucket, every source bucket's checkpoint, and
every object still being tracked for a completion email are preserved. The only
required change is the parameter removal at the top of this entry, which is why
this is a minor version bump rather than a patch.

Objects that 1.0.1 had submitted but never resolved are reported as `UNKNOWN` in
the next completion email covering their bucket, and then leave tracking. Their
per-object detail is still in the Batch Operations completion report CSV under
`completion-reports/` on the State Bucket, which is where to look if you need to
know what happened to a specific object.

Rolling back to 1.0.1 needs the 1.0.1 template supplied with the parameters it
expects, since an update cannot restore parameters this template does not declare.
State written by 1.1.0 stays readable by 1.0.1.

## [1.0.1] — 2026-08-11

- `JournalLookbackSeconds` defaults to `7200` rather than `3600`, widening the
  journal re-scan window from one hour to two to account for late delivery of S3
  Metadata journal records. A tagging record that reaches the journal after the
  watermark has advanced past `record_timestamp + JournalLookbackSeconds` is
  excluded permanently, with no retry and no alarm, so the old default left only
  an hour of margin against delivery latency that the service does not bound.
  Every run re-scans the whole window, so the wider default scans more journal
  rows per run and holds processed-operation entries for longer.

  Existing stacks keep their current value. A stack updated with
  `UsePreviousValue=true` for this parameter resolves to the value it already
  had, so moving an existing deployment to the new default takes an explicit
  `ParameterKey=JournalLookbackSeconds,ParameterValue=7200`.

## [1.0.0] — 2026-08-09

First release.


[README.md](README.md) has the prerequisites, the parameter reference, and the
behavior tables.
