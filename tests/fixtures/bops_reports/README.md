# Real S3 Batch Operations completion reports

Captures of reports S3 actually wrote, used by `tests/test_bops_report_golden.py`.
They exist because hand-written fixtures cannot catch a wrong assumption: during
the 1.1.0 review the fixtures agreed with the implementation about the report's
column order, about the spelling of a null version, and about the row-count
rationale, and the implementation was wrong or unjustified on all three. A real
report cannot agree with a wrong assumption.

## The one modification

Bucket names have been replaced with `example-source-bucket` and
`example-state-bucket`, and `nulltest/manifest.json`'s declared `MD5Checksum` has
been recomputed over the rewritten result bytes so the reader's checksum
verification still runs against the file it ships with.

Nothing else is altered. Every property these tests assert on is preserved
exactly as S3 wrote it: the `null` and `\N` version tokens, the real version ID,
the column order and its disagreement with the declared `ReportSchema`, the task
statuses, HTTP status and error codes, row order, and CRLF line endings.

Do not otherwise regenerate, reformat, or tidy these files. Any change to
`nulltest/result.csv` breaks the declared checksum and fails the test for the
wrong reason. If a byte must change, recompute the checksum in the same commit.

## `nulltest/`

Job `b2f9f42d-dba4-4a77-945e-074cef95450e`, `us-west-2`, 2026-08-27.
`manifest.json` is the report's own top-level manifest and `result.csv` is the
single result object it declares, so the checksum and row-count checks run
against a complete report rather than an assembled one.

Three tasks, all succeeded. Two objects were written before versioning was
enabled on the source bucket and so carry the null version; the third was written
afterwards and carries a real version ID. That mix is the point. It fixes the
spelling S3 uses for a null version and confirms a real version ID passes through
untouched, in one report.

The report manifest sits at `<prefix>//job-<id>/manifest.json`. The double slash
is real: S3 appends `/job-<id>/...` to a report prefix that already ends in `/`.

## `failed-row-2026-07-21.csv`

The single-row result object from job `0f65a1b7-9b4c-4124-a1ad-06ea77d7224f`,
2026-07-21, the only failed task in a 100,002-task job. It carries `\N` in the
`VersionId` column, which is what the report writes when the manifest supplied an
empty version field rather than the literal `null`. An empty version field always
fails that task with `SrcObjectNotFound: Object versionID is invalid`, which is
the defect `ManifestEntry.to_csv_row` was later changed to prevent.

That job's other result object holds the remaining 100,001 rows and is 10 MB, too
large to commit, so this row is used with a manifest the test builds rather than
the real one. The row is as S3 wrote it apart from the bucket rename above; only
the envelope around it is synthetic, and the test says so.

That job is also the evidence for the row-count check: its manifest entry count,
its `TotalNumberOfTasks`, and its `NumberOfTasksSucceeded + NumberOfTasksFailed`
all equal 100,002, and the two result objects parse to exactly that many rows.
