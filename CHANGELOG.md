# Changelog

All notable changes to this project are documented here.

## [Unreleased]

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
