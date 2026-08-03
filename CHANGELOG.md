# Changelog

All notable changes to this project are documented here.

## [0.3.0] — 2026-08-03

### Fixed

- **A second tagging event on an object version is no longer discarded.**
  `logical_operation_id` now includes a truncated SHA-256 of the resulting tag
  set, so it identifies a tagging event rather than an object version. Retagging
  an object within `JournalLookbackSeconds` is submitted instead of being
  suppressed as a duplicate delivery. This closes two silent-loss cases: a
  non-matching tag set followed by a matching one on the same version, and a tag
  set selecting destination A followed by one selecting destination B.

  The first case is reachable without any second tagging by the operator. A
  service that tags objects independently — GuardDuty Malware Protection for S3
  writes `GuardDutyMalwareScanStatus`, and backup or governance tools behave
  similarly — produces a second `UPDATE_METADATA` record on the same object
  version with a different tag set. When that record and the operator's tagging
  fall in different runs, the earlier non-matching one previously suppressed the
  later matching one and the object was never replicated.
- **`processed_window` now records only operations that were replicated.**
  `submitted_refs` is built from the operations whose kept triple reached the
  written manifest of a successfully submitted job, not from every eligible
  operation. An operation that matched no rule, or that the
  Deleted_Version_Filter excluded, is no longer suppressed on the strength of a
  submission driven by a different object.
- **The checkpoint still advances over eligible operations that reached no
  manifest.** `advance_checkpoint`, `StateStore.release_lease`, and the
  orchestrator's `StateWriter.release_lease` take an optional
  `candidate_max_watermark`, the high-water mark over all eligible operations, so
  narrowing `submitted_refs` cannot stall the cursor on a bucket whose tags never
  match.
- **A version that lost the manifest slot is no longer dropped.** When two or
  more versions of one key are tagged in the same interval, the manifest carries
  one of them; the others previously received a processed ref anyway and were
  suppressed permanently, so they were never replicated. They now stay eligible
  and reach the manifest on a subsequent interval, one per interval, until all
  are replicated.
- **Completion report rows now match the objects they describe.**
  `bops_report_reader` percent-decodes the report's `Key` column. The Solution's
  manifest percent-encodes object keys so that a comma or newline in a key cannot
  break the CSV row, and Batch Operations echoes the manifest key into the
  report, so without decoding, a key containing any character that needed
  encoding never joined to its tracked object and that object never resolved.
  AWS does not document the report's key encoding, so it was verified against a
  real job: a key containing a literal percent sequence came back from the report
  byte-identical to the manifest's encoded form. Recorded at the call site.
- **A crafted invocation event can no longer lengthen the reinvocation chain.**
  `handler` clamps a non-integer or negative `reinvocation_depth` to `0`.
  `depth < chain_limit` is always true for a negative depth, so an event
  carrying one made every subsequent generation pass the chain-limit check and
  the chain never terminated.

### Added

- `bucket_disabled` audit log entry when a bucket's `disabled` flag is written to
  `solution-config.json`, so a disable is auditable from the same log stream as
  the other security-critical mutations rather than only from an `error` entry.

### Changed

- `select_eligible_operations` returns a 3-tuple
  `(eligible, skipped, candidate_max_watermark)`; `candidate_refs` is gone, along
  with the `_leased_manifest_and_submit` parameter that carried it.
- `_build_manifest` returns `(WrittenManifest, kept_triples) | None`.
- `journal_dedup` gains `build_submitted_refs(ops, kept_triples)`.
- `moto==5.1.22` added to the `dev` extra. The window-population tests assert on
  the persisted `processed_window` through a real `StateStore` over a moto-backed
  S3, and the dependency was not declared, so the suite collected only on
  machines that already had moto installed and failed at import in CI. Verified
  in a clean virtualenv installing nothing but `".[dev]"`.
- **`Tagging_Operations` and the `TaggingOperationsRead` metric read higher, and
  `duplicate_records_discarded` lower, for the same activity.** Two journal
  records on one object version with different tag sets are now two logical
  operations rather than one, so records previously counted as journal redundancy
  are now counted as the distinct events they are. The number of jobs and
  manifest entries is unchanged — `ManifestGenerator` still emits one entry per
  `(bucket, key)` — so this is a counting change, not a cost change. Alarms with
  thresholds on `TaggingOperationsRead` may need adjusting. `processed_window`
  holds correspondingly more entries per object; it stays bounded by
  `JournalLookbackSeconds` as before.
- README documents repeat-tagging behaviour, the within-interval tag-supersession
  limit, the multiple-versions-per-key drain, and a manual Athena/Batch Operations
  recipe for the rule-change backfill case, which stays out of scope.

### Upgrade cost — one-off, on the first interval after deploy

No migration path and no dual-format reader are provided. Existing
`processed_window` entries carry the old identity format and match nothing, so
they stop suppressing:

- The first interval re-submits whatever objects are currently inside the
  lookback window. That is **one additional Batch Operations job per affected
  bucket, plus its per-object charge, once.** Batch Operations bills every
  manifest object whether or not it is already replicated, so this is not free.
- Those objects' `manifest_generated_at` is refreshed, which defers their
  Completion_Reports by one quiescence cycle
  (`completion_tracker.quiescence_check`).

Both are one-time. Subsequent intervals behave as before.

## [0.2.2] — 2026-08-03

### Added

- Completion report email now includes `tagged_at` (tag event timestamp) and
  `last_modified` (object last-modified date) for each replicated object.
- Athena journal query fetches `last_modified_date` column.
- Timestamps are persisted in the state object at manifest-generation time and
  attached to TrackedObjects when the BOPS report is processed.

## [0.2.1] — 2026-08-03

### Added

- Batch Operations jobs now include a `Description` field showing source bucket,
  config ID, and object count for easier identification in the S3 console.

## [0.2.0] — 2026-08-02

### Removed

- **Compatibility removal:** legacy state-object shapes from pre-0.1.18 deployments
  are no longer read. Upgrade condition: a deployment must have completed at least
  one successful run on 0.1.18 or later before upgrading to this version. Affected
  paths: singular `submission_record` key, 2-column manifest entries,
  per-config_id submission record keying in completion reports, and pre-migration
  TrackedObject handling.
- Unreferenced code: `get_submission_failure_streak`, `Manifest` dataclass,
  `MIN_BUCKETS`, unused `state_bucket` local
- Production-dead code: `classify_delete`, `read_manifest_entries`,
  `SubmissionResult.was_skipped`, `SourceStatusResult` predicates,
  `ManifestResult.no_matches_reason`
- Three vestigial parameters: `now`, `replication_config_id`, `account_id`
- Two unreachable IAM-policy test fixtures

### Changed

- All `Optional[X]` annotations unified to `X | None` (56 sites)
- Unused imports removed (8 total across the cleanup)
- `_CompletionHooks | _NullCompletionHooks` replaced with a named type alias
- `src/orchestrator.py`: `_submit_job`, `_process_bucket`, `_build_manifest`
  brought within the 120-line / nesting-3 limits via extraction
- `src/adapters/state_store.py`: 19 near-identical get_object/json.loads/NoSuchKey
  blocks replaced with two shared helpers (`_read_state_payload`, `_write_state_payload`)
- `.holmes/accepted-risks.md`: AR2 (manifest column-count inference) moved to
  fixed — the inference is deleted, exceeding AR2's own preferred remediation

### Added

- `ruff` lint gate in CI (`lint-python` job): selects F, ARG, ERA, UP; fails on any finding
- `[tool.ruff]` configuration in `pyproject.toml`
- All Lambda functions now use Graviton (`arm64`) architecture (previously only
  ReplicationLambda and CompletionReportCheckLambda were arm64; six inline helper
  functions defaulted to x86_64)

## [0.1.18] — 2026-08-01

### Changed

- **`_process_bucket` decomposed into named phases.** The 1,152-line method is
  now a short sequence of calls to single-purpose helpers coordinated through a
  frozen `_BucketContext` dataclass, a `StateWriter` that owns the ETag chain,
  a null-object completion-tracking collaborator (`_CompletionHooks` /
  `_NullCompletionHooks`), and a pure recovery-planning module
  (`src/core/job_recovery.py`). No externally observable behaviour change —
  same AWS calls, same logs, same metrics, same state-object schema.

### Fixed

- **A lease acquired by `_process_bucket` is now released on every post-acquire
  exit path** (preflight failure, no matches, all-candidates-excluded,
  manifest-write failure, submission-streak disable). Previously a leaked
  `IN_FLIGHT` lease suppressed eligible operations until the next scheduled run
  for that bucket. The checkpoint does not advance on these paths.

## [0.1.17] — 2026-07-31

### Fixed

- **LF granter no longer fails open.** The `LFPermissionsGranterFunction`
  swallowed `AccessDenied` on `glue:GetCatalog` and fell through to
  attempting Lake Formation grants, which also failed — producing the opaque
  `Insufficient Glue permissions to access database` error on second-stack
  deploys. `detect_mode` now retries 5 times with exponential backoff and
  raises on exhaustion, naming the catalog and the last error. On an IAM-mode
  catalog it correctly detects IAM mode and skips grants entirely.

- **Batch-job failure email now has a useful subject line.** EventBridge's
  direct-to-SNS target does not support a `Subject` field, so every failure
  email arrived as `"A replication job did not finish successfully..."` —
  unidentifiable in an inbox with multiple stacks. Replaced with a notifier
  Lambda that publishes with `"Batch job Failed: <stack> (<region>)"`.
  Removes `BatchJobFailureTopicPolicy` (no longer needed — the Lambda
  publishes via its own IAM role).

## [0.1.16] — 2026-07-29

### Changed

- **Each alert kind now writes to its own CloudWatch log stream.** All three
  alerts share one log-writing helper, which hardcoded a `report-missing`
  stream prefix — so the bucket-disabled and submission-failure alerts landed
  in a stream named for an unrelated condition. Streams are now
  `report-missing-`, `bucket-disabled-`, and `submission-failure-`. Log group,
  message content, and SNS behaviour are unchanged; only stream names differ,
  and a query across the whole log group sees the same entries as before.

- **The end-to-end test now exercises the Solution's own submission path.** It
  previously hand-rolled its manifest CSV, its manifest `Fields` spec, and its
  `CreateJob` call, so it was a parallel implementation of the code it exists
  to validate — it could not have caught the `ManifestEncryption` defect it was
  written after, because it never called `submit_batch_job`. Three defects in
  the harness followed from that divergence, all of them observed:

  - Its all-or-nothing version check collapsed a 100,062-object manifest to a
    2-field spec because a single object lacked a version ID, and
    `S3ReplicateObject` rejected the job outright. It now uses the Solution's
    `serialize`, which always emits three fields, writes the literal `null`
    for a null version, and percent-encodes keys.
  - It never applied the Deleted_Version_Filter. This was the cause of a 100%
    task failure rate: 100,028 of 100,062 manifest entries were permanently
    deleted versions that the Solution excludes. After filtering, 34 tasks ran
    and all 34 succeeded.
  - It failed the whole run on a transient `DescribeJob` error raised *after* a
    successful submission. Monitoring now sits outside the submission
    `try`/`except` and tolerates transient errors, matching the orchestrator's
    best-effort posture.

  The job's completion report is now enabled: with `Report.Enabled=false` a
  job that fails every task reports only an aggregate failure rate, and S3
  records no per-task reason anywhere, which is what made the first failure
  undiagnosable.

- **`.holmes/accepted-risks.md`:** adds FP13 (Bandit `B106` on test-only
  placeholder strings) and replaces AR3's stale line-number reference with the
  named assignment it refers to.

## [0.1.15] — 2026-07-29

### Added

- **Submission-failure classification and escalation.** A `ParamValidationError`
  from botocore (the request was rejected before signing) is now classified as
  `PERMANENT_CLIENT` and distinguished from a service-side error (`SERVICE`),
  timeout (`TIMEOUT`), or unknown failure (`UNKNOWN`). The classification
  appears in the emitted log entry so a log query can separate the two classes.

- **Immediate alert on permanent submission failure.** On the first occurrence
  of a permanent client-side rejection, an alert is published to
  `BatchJobFailureLogGroup` (always) and `BatchJobFailureTopic` (when
  `AlarmEmail` is configured), naming the bucket, the operation, and the
  validation error. The alert is suppressed for subsequent intervals while the
  same failure persists and fires again after a successful submission clears the
  suppression.

- **Disable at threshold for permanent failures.** When a bucket records a
  permanent client-side submission failure on `MaxBatchJobFailures` consecutive
  intervals, the Solution disables the bucket with a reason stating the cause is
  a code defect and re-enabling without a fix will reproduce it. Service-side
  errors never count toward this threshold.

- **CI deadline fix.** The `ci` Hypothesis profile now sets `deadline=None` to
  prevent timing flakes on shared runners. The `default` profile retains the
  200ms deadline for local development.

## [0.1.14] — 2026-07-28

### Fixed

- **A job-submission failure moved no metric.** On `CREATE_FAILED` the run
  emitted one `ERROR` log entry and left its `errored` flag `False`, so
  `BucketErrors` published `0.0` for a bucket whose every submission was
  failing. The consecutive-failure circuit breaker could not see it either: that
  counter increments only for a *terminal job* failure observed through
  `DescribeJob`, and a job that was never created has neither a record nor a
  status. A deployment that could not submit at all therefore looked healthy in
  every metric — which is exactly how the `ManifestEncryption` defect in 0.1.13
  ran unnoticed. A creation failure now marks the bucket errored. The checkpoint
  is still left unadvanced, so the window is retried rather than skipped.

  This is task 1 of `.kiro/specs/submission-failure-visibility/`. The remainder
  of that spec — classifying a client-side `ParamValidationError` as permanent,
  alerting once per episode, and disabling a bucket only for that class — is not
  implemented here.

### Added

- **The test suite now runs in CI.** `.gitlab-ci.yml` gained a `unit-tests` job
  in the `validate` stage, and `release-gitlab` depends on it, so a tag cannot
  publish artifacts the suite has not passed on. Previously `cfn-lint` on the
  template was the only gate a push had to clear: 1,583 tests existed and ran
  only when someone ran them locally, including the request-shape tests added in
  0.1.13 to catch exactly the class of defect that reached a deployment.

### Changed

- `PyYAML==6.0.3` is declared in the `dev` extra. It was always required —
  `tests/test_template.py` and the two inline-Lambda test modules parse
  `deploy/template.yaml` — but was absent from the dependency set, so the suite
  collected only on machines that happened to have PyYAML installed. A clean
  environment failed at import on three modules. Found by running the new CI job
  in a fresh virtual environment before trusting it.

## [0.1.13] — 2026-07-28

### Fixed

- **A partially-filtered manifest still contained the objects the filter
  excluded.** The Deleted_Version_Filter separates matched objects into those to
  keep and those permanently deleted, but the manifest was serialized from the
  pre-filter list. Only the all-excluded case was handled, via an early return
  that skips job creation entirely. So when *some* of an interval's objects had
  been permanently deleted, they were still submitted to S3 Batch Operations —
  while the run logged `deleted_versions_excluded` claiming they were dropped.
  The submitted object count was the pre-filter total for the same reason.

  Effect was wasted batch-job tasks failing against objects that no longer
  exist, and completion reports counting objects the Solution had decided not to
  replicate. The manifest is now built from the filter's survivors, and the
  recorded count describes what was actually submitted. Every pre-existing
  orchestrator test stubbed the permanent-delete read to an empty set, which is
  why the partial case was never exercised.

- **The report-missing handler downgraded state-object encryption.**
  `CompletionReportCheckLambda` was never given `KMS_KEY_ARN`, so its state
  store wrote with bucket-default encryption. It writes the same objects as the
  main handler, so on a deployment with `KmsKeyArn` set, every alert-suppression
  write silently rewrote an SSE-KMS object under SSE-S3. It now receives the
  same key, wired identically to the main function.

- **No job could be submitted at all when `KmsKeyArn` was set.** For the
  inventory-manifest path the submission declared the manifest's encryption as
  `Manifest.Location.ManifestEncryption.SSEKMS.KeyId`. S3 Control's
  `JobManifestLocation` has no such member — it accepts only `ObjectArn`,
  `ObjectVersionId`, and `ETag` — so botocore rejected the call during
  parameter validation, before signing. Every interval returned
  `CREATE_FAILED`, indefinitely: a submission failure does not set the run's
  `errored` flag and does not feed the consecutive-failure circuit breaker
  (which counts terminal job failures, not creation failures), so the only
  signal was an `ERROR` log line while `BucketErrors` stayed at zero. The
  declaration is removed: an SSE-KMS
  manifest needs nothing declared, because S3 Batch Operations decrypts it
  with the job's `RoleArn`, which `deploy/README.md` already requires be
  granted `kms:Decrypt`. Verified end to end against a KMS-enabled
  deployment — job created, SSE-KMS `manifest.json` read, one task succeeded.
  Every existing test for this call used a `MagicMock`, which accepts any
  parameter, so the invented member was invisible; the new tests validate the
  `create_job` kwargs against botocore's own `s3control` service model.

- **The batch-job-failure email was never sent.** `BatchJobFailureRule`
  publishes to `BatchJobFailureTopic` from an EventBridge target, so the
  `ExecutionRole`'s `sns:Publish` grant does not authorize it, and the topic
  carried only SNS's default access policy — conditioned on `AWS:SourceOwner`,
  which does not cover a service principal. Every rule-driven publish was
  rejected. The rule's other target, the batch-job-failures log group, kept
  working, so the event still appeared in the log and the rule looked healthy.
  A `BatchJobFailureTopicPolicy` now grants `events.amazonaws.com`
  `sns:Publish`, scoped by `aws:SourceArn` to this rule.

- **A far-future or malformed checkpoint watermark halted replication
  silently.** `last_processed_watermark` was accepted as any string, so a
  well-typed value such as `9999-12-31T23:59:59.000000Z` made every subsequent
  journal query return nothing, with no error. Deserialization now requires the
  epoch watermark or a canonical timestamp no more than 24 hours ahead of now.

- **A failed job with no recorded low watermark reset the bucket to epoch.** The
  readmission rollback took `min()` over the failed jobs' `watermark_low`
  values; an empty value — which pre-migration records legitimately carry —
  collapsed that to the epoch and re-admitted the entire journal history as
  duplicate jobs. Empty values are now excluded from the rollback candidates and
  reported.

### Security

- **Replication role ARNs are validated before use.** The ARN is read from a
  source bucket's replication configuration without validation, and reaches both
  the State_Bucket policy `Principal` and `CreateJob`'s `RoleArn`. Only the
  bucket-policy path checked it, and only when completion reporting was enabled
  — so with completion reporting off, an unvalidated value reached
  `CreateJob`. Validation now happens once where the ARN is derived, covering
  both consumers, and rejects anything that is not a well-formed IAM role ARN in
  the deploying account. The deploy-time `ReplicationRolePassGranter` applies
  the same check before writing an ARN into an `iam:PassRole` policy.

- **The State_Bucket policy grant is audited.** Priming the bucket policy grants
  an external replication role `s3:PutObject`. That write now emits a
  `completion_report_bucket_policy_granted` audit entry naming the bucket, role,
  and account, alongside the existing `iam:PassRole` record. It fires only when
  a write actually occurred.

### Added

- **The end-to-end test can exercise the KMS paths.** Setting
  `S3ROT_TEST_KMS_KEY_ARN` makes `tests/test_e2e_aws.py` write the state object
  and the manifest with SSE-KMS and verify each write with `HeadObject`, along
  with the Athena result the workgroup produced. `deploy/README.md` gains
  "Verifying a KMS-enabled deployment", covering the three checks the script
  cannot make — workgroup enforcement, SNS topic encryption, and the EventBridge
  publish — and how to force a genuinely failed batch job and the
  report-missing handler's state write.

- **Every AWS request the Solution builds is now validated against botocore's
  own service models in tests.** `tests/api_shape.py` replays the kwargs a
  mocked client received through the same parameter validation the SDK performs
  on a live call, and `tests/test_api_param_shapes.py` exercises each call site
  that assembles its request conditionally — KMS on/off, `If-Match` vs
  `If-None-Match`, version id present/absent, pagination token, optional SNS
  subject, optional CloudWatch dimensions. Those conditional branches are the
  ones no default-configuration deployment executes, which is how the
  `ManifestEncryption` defect above reached an account. An invented,
  missing-required, or wrongly-typed parameter now fails in unit tests.

- **`SnsKmsKeyArn` parameter** for server-side encryption of the
  completion-report and batch-job-failure SNS topics. Empty by default, so
  existing deployments are unchanged. Completion report bodies carry object keys
  and version IDs, which is the case encryption is worth paying for. SNS has no
  keyless SSE option, so this needs a symmetric customer-managed key, and that
  key's policy must allow both `sns.amazonaws.com` and `events.amazonaws.com` —
  the second because the failure notification is published by an EventBridge
  target rather than by the Lambda. Omitting either statement fails each publish
  at runtime rather than failing the stack update. See
  `deploy/README.md` "SNS Topic Encryption".

### Changed

- `deploy/README.md` documents enabling automatic KMS key rotation for any key
  supplied to `KmsKeyArn`, `JournalKmsKeyArn`, or `SnsKmsKeyArn`.

- The `release-cli` container image in the GitLab release job is pinned to
  `v0.24.0` rather than tracking `:latest`.

- `pyproject.toml`'s build-system requirements are pinned exactly —
  `setuptools==83.0.0`, `wheel==0.47.0` — rather than `setuptools>=68` with
  `wheel` unconstrained. Nothing in the repository builds through this backend
  today (the Lambda artifact comes from `deploy/build-package.sh`, and the CI
  lint job installs only `cfn-lint` and `awscli`), so this buys reproducibility
  for a build path that is currently unused rather than closing an active
  exposure. Verified by building the wheel through PEP 517 isolation with the
  pins in place.

- `deploy/iam-policy.json`'s note on the omitted `iam:PassedToService` condition
  now records what was measured rather than what was assumed. `s3control:CreateJob`
  authorizes the caller's `iam:PassRole` at create time and refuses the call
  without it, so the grant is load-bearing; and it does not populate
  `iam:PassedToService`, so a statement carrying that condition denies and job
  creation fails. The condition must not be added. `.holmes/accepted-risks.md`
  AR8 carries the result table, including why an earlier measurement — made with
  admin user credentials rather than the throwaway role's, so the variant
  policies were never in the authorization path — concluded the opposite.

## [0.1.12] — 2026-07-27

### Changed

- **Every operator email is now readable prose.** Previously the
  bucket-disabled and report-missing alerts published a raw `json.dumps` blob
  to SNS, and the completion report's summary read
  `1057 object(s) processed. Outcomes: GONE: 1057` — exposing an internal enum,
  repeating the count, and never saying whether anything needed attention. The
  same content is still written to `BatchJobFailureLogGroup` as structured JSON,
  so queryability in Logs Insights is unaffected; only the email body changed.

  The completion summary now reads
  `example-source-bucket: 1,057 objects were deleted before replication could
  be confirmed. No failures.` Outcomes are spelled out in plain English and
  ordered most severe first, so `FAILED` leads even when it is the minority;
  counts carry thousands separators and agree in number with their verb; a
  single outcome covering every object absorbs the total rather than repeating
  it; and the sentence ends by stating whether action is needed.

- **SNS `Subject` set on every notification the Solution publishes.** Without
  one, every alert arrived titled with the SNS topic name and an inbox of them
  could not be triaged without opening each. Subjects carry the bucket and the
  verdict, and are coerced to ASCII, stripped of newlines, and truncated below
  SNS's 100-character limit — truncating the bucket name rather than the
  verdict — so a malformed subject can never cost the alert itself.

- The batch-job-failure email (an EventBridge input transformer) now leads with
  the consequence rather than the event, and states that a single failure is
  retried automatically and usually needs no action.

- Requirement 4.10 added to the `source-status-completion-tracking` spec,
  covering summary wording, the subject line, and the prose-email/structured-log
  split. Two acceptance criteria in that requirement were both numbered 8;
  renumbered to 8, 9, 10.

## [0.1.11] — 2026-07-27

### Removed

- **The `DuplicateRecordsDiscarded` CloudWatch metric.** Its documented meaning
  was wrong: computed as raw journal records read minus *eligible* operations,
  it conflated records dropped as malformed (already logged individually),
  genuine at-least-once duplicate deliveries, and records excluded as already
  processed — the last including every record re-read by the deliberate
  `JournalLookbackSeconds` re-scan, which dominates on a steady deployment. A
  run with no tagging activity at all still reported a non-zero count, purely
  from lookback re-reads. No threshold on the conflated total implied an
  action, so it was paying $0.30/month to publish a misleading signal.

  `duplicate_records_discarded` remains a field of the `interval_summary` log
  entry, so the value is still available for diagnosis at no per-metric cost.
  `RunResult.duplicate_records_discarded` is removed, the metric publisher
  having been its only consumer. Run-level datums drop from two to one, so a
  single-bucket deployment publishes five metrics rather than six.

## [0.1.10] — 2026-07-27

### Added

- **`DisabledBuckets` CloudWatch metric** — a run-level count of buckets
  skipped because their `disabled` flag is set, published every run including
  when zero, so `>= 1` is a plain alarm threshold. A disabled bucket is skipped
  before any `BucketMetrics` exists for it and so contributes no per-bucket
  datum of any kind, which left auto-disable — the one failure that stops
  replication for a bucket indefinitely and requires a manual re-enable — as
  the only failure mode with no metric behind it. Previously observable only in
  the log stream and in the one-shot notification email, which reports the
  transition rather than the ongoing state.

  Deliberately run-level rather than dimensioned on `SourceBucket`: it costs a
  flat $0.30/month regardless of bucket count, where a per-bucket gauge would
  reintroduce the per-bucket multiplication removed in 0.1.7. The bucket's
  identity is already carried by the notification email, the per-run `error`
  log entry, and the `disabled` flags in `solution-config.json`. Requirement
  3.4 added to the `cloudwatch-metrics` spec.

## [0.1.9] — 2026-07-27

### Fixed

- **An idle interval now records a zero-match tag scan, so a quiet bucket can
  publish its completion reports.** `_process_bucket` returned at its "nothing
  to process" early exit before reaching `record_scan_result`, so
  `completion_scan_state` stayed frozen at whatever the last non-empty interval
  observed. `quiescence_check` requires a scan recorded after manifest
  generation with `match_count == 0`, so once a bucket went quiet that
  condition could never be met: no Completion_Report was ever published, and
  because deletion is gated on a successful publish, every `RESOLVED`
  Tracked_Object was retained forever. Observed on the test deployment as ten
  objects resolved `COMPLETE` on 2026-07-21 still held in state six days later.
  An empty journal window is now recorded as the zero-match cycle it is.
  Requirement 5.5 added to the `source-status-completion-tracking` spec.

  This was the remaining blocker on 0.1.8's fixes taking effect: objects
  resolved correctly to `GONE` but could not drain out of the state object.

## [0.1.8] — 2026-07-27

### Fixed

- **A deleted source object no longer pins its tracked object in `PENDING`
  forever.** `check_source_replication_status` mapped every `ClientError` to
  the transient `CHECK_FAILED`, so an object version that had been deleted was
  re-checked on every run indefinitely and its entry could never leave the
  state object. A 404 (`NoSuchKey`, `NoSuchVersion`, or a bare HTTP 404, which
  is what `HeadObject` usually returns since it carries no response body) is
  now the terminal `OBJECT_GONE`, resolving to outcome `GONE`. 403 remains
  transient deliberately: it is also what a genuine permission fault returns,
  and treating it as terminal would silently abandon objects during an IAM
  misconfiguration.
- **`s3:ListBucket` granted on source buckets.** Without it S3 masks object
  existence and returns 403 where it would otherwise return 404, which made
  the condition above undetectable. The same reasoning was already documented
  for the State Bucket grant in `deploy/iam-policy.json` but had never been
  applied to source buckets. The fix above does nothing without this grant.
- **Completion reports are split across SNS messages.** `publish_completion_report`
  issued a single `Publish` with no size handling, and an entry serializes to
  ~193 bytes, so any bucket resolving more than ~1,240 objects in one interval
  produced a body over the SNS 256 KiB limit. Because items are deleted only
  after a successful publish, such a report failed identically on every
  subsequent run and retained its items permanently. `chunk_items_for_report`
  now sizes each entry as serialized — at its real nesting depth, so
  indentation is counted — and the orchestrator publishes per batch, deleting
  only the items whose batch published successfully.

### Added

- **`CompletionItemTtlHours` (default 168, minimum 24)** — a tracked object
  still unresolved after this window is abandoned with outcome `EXPIRED` and
  removed from tracking, without spending a `HeadObject` on it. A general
  backstop bounding state-object growth whatever the cause, measured from the
  most recent covering job's `manifest_generated_at`, so no new state field or
  migration is required. Expiry emits a `completion_item_expired` audit entry
  and reuses the normal publish-then-delete path, so abandoned objects are
  reported rather than vanishing silently. A non-positive value disables it.
- Requirements 3.7, 3.8, and 4.8 added to the `source-status-completion-tracking`
  spec for the three behaviours above.

## [0.1.7] — 2026-07-24

### Changed

- **Zero-suppression for the per-bucket activity metrics** — CloudWatch bills
  each unique metric-and-dimension combination per hour in which a data point
  is sent, so publishing flat zeros for an idle bucket cost the same as real
  activity. `TaggingOperationsRead`, `MatchedObjects`, and `BatchJobsSubmitted`
  are now published only for buckets that had activity in that run, and as a
  group, so a run with any activity still carries any genuine zeros among them.
  At 10 buckets seeing occasional bursts this takes the metric bill from
  ~$12.30/month to under $4.
- **`BucketErrors` is still published every run, including its `0` value**, and
  is now documented as the per-bucket liveness signal: its presence means the
  bucket was enabled and processed, so a missing data point (disabled bucket,
  removed from `SourceBucketNames`, or a run that never reached it) is
  alarmable with `treatMissingData: breaching`. `DuplicateRecordsDiscarded`
  remains unconditional as the run-level heartbeat.
- Amended Requirement 2.6 of the `cloudwatch-metrics` spec, which previously
  mandated publishing every per-bucket value including zeros, and split it into
  acceptance criteria 6, 7, and 8.

### Documentation

- **README restructured** into a deploy-and-operate order (Prerequisites →
  Getting Started → Required AWS Permissions → Parameters → Configuration →
  Completion Reporting → Deleted-Version Filtering → Monitoring → Checkpoint
  and Recovery → Cost → Development). Use Cases moved to `docs/use-cases.md`.
- **Corrected the Required AWS Permissions table**, which claimed `athena:*`,
  `glue:*`, and `s3tables:*` while `deploy/iam-policy.json` enumerates specific
  read actions.
- **Cost section** gained CloudWatch and SNS components, per-bucket scaling is
  stated explicitly, and the section was condensed.
- **Documented what goes in `MetricsNamespace`** — any name you choose, created
  on first publish, `AWS/` prefixes reserved — in both READMEs.
- Added `CompletionNotificationEmail`, `CompletionCheckBatchSize`, `AlarmEmail`,
  and `MaxBatchJobFailures` to the README parameters table; each gates
  documented behaviour but appeared only in `deploy/README.md`.

## [0.1.6] — 2026-07-24

### Changed

- **Recalibrated the `JournalReadRowCap` memory-safety ceiling table** from a
  real measurement of the in-memory manifest path
  (`ManifestGenerator.accumulate` → `finalize` → `serialize`, ~1.9 KiB/object)
  rather than the prior unverified ~0.8 KiB/object estimate. The
  `IN_MEMORY_MEMORY_CEILING` values are halved (250,000 rows at 1,024 MiB up
  to 2,500,000 at 10,240 MiB); the shipped `JournalReadRowCap` default stays
  at 500,000. Updated the README ceiling table and the corresponding spec docs.

### Added

- **`row_cap_overshoot` audit log** — the row cap is a target, not an exact
  bound: `read_journal` reads the boundary timestamp inclusively (so a tied
  batch there is never split and no operation is dropped), which can push the
  rows actually read modestly above the cap. A capped run whose rows read
  exceed the cap now emits an audit entry, making the run-time overshoot
  visible that the config-load check cannot see.
- `benchmarks/real_manifest_benchmark.py` — reproducible real-path peak-RSS
  benchmark the ceiling table is anchored to.

## [0.1.5] — 2026-07-24

### Removed

- **The Athena UNLOAD manifest-generation path** — `src/adapters/unload_generator.py`,
  the UNLOAD anti-join SQL, and the UNLOAD-only helpers in `data_file_hasher.py`
  are removed. In_Memory_Generation is now the sole manifest-generation path.
  `select_manifest_strategy` / `ManifestGenerationMode` and the `ScaleThreshold`,
  `InlineHashCeilingBytes`, and `InlineHashCeilingFiles` CloudFormation
  parameters/env vars are retired; the corresponding UNLOAD-only IAM statement
  was removed from `deploy/iam-policy.json`.

### Added

- **Self-reinvocation** — a Capped_Run (one that hit `JournalReadRowCap`) that
  successfully submits its job now triggers the next run immediately via an
  async self-invoke (`InvocationType='Event'`) instead of waiting for the next
  scheduled trigger, bounded by a new `ReinvocationChainLimit` CloudFormation
  parameter (default `20`) / `REINVOCATION_CHAIN_LIMIT` env var. Raises the
  sustained drain rate from a schedule-bound ceiling (`JournalReadRowCap ×
  runs_per_day`, ~48M rows/day at defaults on a 15-minute schedule) to ~120M
  rows/day, bounded by per-run wall-clock instead of `CheckFrequencyMinutes`.
  A trigger failure is logged and swallowed — the completed run is never
  affected, and the next scheduled trigger remains a self-healing fallback.
  New `ReinvocationSelfInvokePolicy` IAM statement and
  `ReplicationLambdaEventInvokeConfig` (`MaximumRetryAttempts: 0`) in
  `deploy/template.yaml`.
- **Row-cap memory-safety validation** — `JournalReadRowCap` is now the
  Solution's single scale knob, governing both journal-read pagination cost
  and in-memory manifest size. At configuration load, a new
  `validate_row_cap` check rejects a `JournalReadRowCap` that exceeds the safe
  ceiling for the configured `LambdaMemoryMB` (500,000 rows at 1,024 MiB
  scaling up to 5,000,000 rows at 10,240 MiB), failing fast before any
  S3/Athena access rather than risking an out-of-memory failure mid-run.
- New observability entries: `reinvocation_triggered` (chain position) and
  `reinvocation_chain_limit_reached` (chain limit reached with backlog
  remaining); the existing `journal_read_capped` audit entry's visibility at
  the deployed Lambda's default logging level was confirmed/fixed.

### Changed

- `deploy/README.md` and the main `README.md` updated: removed references to
  `ScaleThreshold`/`InlineHashCeilingBytes`/`InlineHashCeilingFiles` and the
  UNLOAD path, added `ReinvocationChainLimit` to the parameter reference, and
  added a new "Sustained drain rate" subsection documenting the
  schedule-bound and self-reinvocation drain ceilings.

## [0.1.4] — 2026-07-23

### Added

- **Source-side-only completion tracking** — per-object-version replication
  completion tracking via the source object's native `x-amz-replication-status`
  header, without any destination-account or destination-region access.

  - Reads `x-amz-replication-status` on the source object only, gated on each
    job's S3 Batch Operations completion report confirming the object was
    processed.
  - Publishes one SNS completion report per source bucket per interval, gated
    on the new `CompletionNotificationEmail` parameter.
  - Detects a permanently-missing BOPS completion report (typically a missing
    `s3:PutObject` grant on the customer's replication role) via a dedicated
    5-minute check, alerting through the existing `BatchJobFailureTopic` /
    `BatchJobFailureLogGroup`.
  - Primes the customer's replication role with write access to its own
    completion-report prefix via a per-config bucket-policy statement on the
    State Bucket, applied just-in-time before each job submission — no changes
    to the customer's own IAM role.

  New CloudFormation parameter `CompletionNotificationEmail`; new
  `CompletionCheckBatchSize` parameter (default `2000`) bounding checks issued
  per run; new `CompletionCheckMemoryMB` parameter (default `256`) sizing the
  report-missing-detection Lambda independently.

- **Journal read row cap** — `JournalReadRowCap` (default `500,000`, new
  CloudFormation parameter / `JOURNAL_READ_ROW_CAP` env var) bounds how many
  journal rows a single invocation reads. A cheap boundary query finds the
  cap point before reading, and the existing watermark-advancement logic
  already resumes exactly at that boundary on the next scheduled run — no new
  state is needed to track a resume position. Protects against pagination
  alone approaching the Lambda timeout on an unusually large single-interval
  tagging burst.

### Changed

- **`ReplicationLambda` concurrency pinned to 1** via
  `ReservedConcurrentExecutions: 1`. The function processes every bucket
  sequentially with no in-process concurrency, so this has no throughput
  cost, and it prevents two invocations (for example an overrunning run
  overlapping the next scheduled trigger) from racing on the same bucket's
  lease.

### Fixed

- Completion-tracking's `Source_Status_Check` was missing
  `s3:GetObject`/`s3:GetObjectVersion` on source objects, so every
  `Tracked_Object` stayed `PENDING` forever; the grant is now included in
  `deploy/iam-policy.json` and the `ExecutionRole`.
- The BOPS completion-report reader listed every object under the report
  prefix, including `manifest.json`/`manifest.json.md5` sidecars, producing
  garbage entries; it now filters to `results/*.csv` keys only.
- Checkpoint-clobber and null-version supersession bugs found during live
  verification against real AWS resources.

## [0.1.3] — 2026-06-25

### Fixed

- **Region format validation** — `load_config` now validates the `region`
  field of each bucket entry against the AWS region format
  (`^[a-z]{2,4}-(?:[a-z]+-)+\d{1,2}$`). Strings that are well-formed but
  not valid AWS region identifiers (uppercase, underscores, missing digit
  suffix, etc.) are rejected with a clear `ConfigError` at startup rather
  than producing a cryptic `ClientError` during the first S3 API call.
  Affects only manual edits to `solution-config.json` — the deployment
  `ConfigResourceFunction` always writes the stack region, so this path is
  safe by construction.

- **BOPS failure circuit breaker** — if an S3 Batch Operations job for a
  replication configuration fails or is cancelled `MaxBatchJobFailures`
  consecutive times (default 4), the bucket is disabled in
  `solution-config.json` via the same `on_bucket_disable` callback used for
  `InlineHashCeiling` breaches. This prevents runaway per-job charges on a
  low-churn deployment where a misconfigured job would otherwise cycle
  indefinitely. The `consecutive_failures` counter is stored in each
  `SubmissionRecord` entry and resets to zero on the first successful
  (`Complete`) job. A `DescribeJob` transient error does not increment the
  counter — the existing failure history is carried forward unchanged.

  New CloudFormation parameter `MaxBatchJobFailures` (default `4`,
  minimum `1`); new Lambda env var `MAX_BATCH_JOB_FAILURES`.

### Added

- **Batch failure monitoring** — four new CloudFormation resources detect
  S3 Batch Operations job failures and surface them as a CloudWatch alarm:

  - `BatchJobFailureLogGroup` (`RetentionInDays: 90`, `DeletionPolicy:
    Retain`) receives every failed/cancelled job event.
  - `BatchJobFailureResourcePolicy` authorises `events.amazonaws.com` to
    deliver to the log group.
  - `BatchJobFailureRule` matches `AWS Service Event via CloudTrail` events
    where `eventName=JobStatusChanged` and `status ∈ {Failed, Cancelled}`.
    An active CloudTrail trail (management events) is required in the stack
    region — documented as a prerequisite in `deploy/README.md`.
  - `BatchJobFailureMetricFilter` → `FailedBatchJobs` metric in the
    `{StackName}/BatchJobs` namespace.
  - `BatchJobFailureAlarm` fires when `Sum(FailedBatchJobs) > 0` over one
    300s period (`TreatMissingData: notBreaching`).
  - Optional `AlarmEmail` parameter: when set, a conditional `BatchJobFailureTopic`
    / `BatchJobFailureSubscription` sends email on alarm and the SNS topic is
    added as a second rule target.
  - `BatchJobFailureAlarmArn` stack output.

- **Batch failure recovery** — when a prior S3 Batch Operations job is found
  to be in `Failed` or `Cancelled` state (and below the circuit-breaker
  threshold), the orchestrator re-admits the affected operations in the same
  run without a separate state write:

  - `SubmissionRecord` gains `watermark_low` (the bucket watermark before the
    run that submitted the job — the re-admission resume point),
    `watermark_high` (the run's `candidate_hwm`), and `consecutive_failures`.
  - `StateStore.get_submission_records` — new read path returning all
    per-config `SubmissionRecord` objects for a bucket.
  - `record_submission` now writes a `submission_records` dict keyed by
    `replication_config_id` (replaces the previous singular
    `submission_record` field; the legacy field is migrated on read and
    stripped on the next write).
  - `checkpoint_serializer` adds `serialize_submission_record` and
    `deserialize_submission_records` helpers, including legacy migration.
  - In `_process_bucket`, right after `get_checkpoint`: calls `DescribeJob`
    for each prior submission; for `Failed`/`Cancelled` jobs rolls the
    in-memory watermark back to `min(watermark_low)` across all failing
    configs and prunes `processed_window` entries above it. `DescribeJob`
    errors are absorbed at WARNING and never block the run.
  - `s3:DescribeJob` added to the `S3BatchOperationsCreateJob` statement in
    `deploy/iam-policy.json` and the `ExecutionRole` inline policy.

### Changed

- **ClientFactory client caching** — `ClientFactory` now caches boto3 clients
  by `(service, region)` behind a `threading.Lock`. Repeat calls for the same
  region return the same client object instead of creating a new one per call,
  eliminating three redundant `boto3.client()` constructions per bucket per
  run (all previously identical since the stack always stamps all buckets with
  the same region). This also prepares the factory for safe use under the
  parallel bucket-processing work planned for a future release.

## [0.1.2] — 2026-06-25

### Security

- **Scoped `sts:AssumeRole`** off `Resource: "*"` in `LFGranterRole`. A
  separate `AssumeLFAdminRole` statement now scopes the permission to
  `LFAdminRoleArn` when provided, or to a non-existent dummy ARN when the
  solution manages its own LF elevation. The `LFOperations` statement retains
  `Resource: "*"` with a comment explaining that Lake Formation, Glue catalog,
  and S3 Tables management-plane APIs do not support resource-level ARN scoping.

- **LIKE metacharacter escaping** — added `_escape_like_pattern()` to
  `athena_journal_adapter.py`. It escapes `\`, `%`, and `_` before a value is
  embedded in a LIKE predicate, preventing a replication-rule prefix that
  contains `%` or `_` from broadening the SQL match beyond the intended scope.
  `build_rule_predicate` in `preflight_counter.py` now uses
  `_escape_like_pattern` and adds `ESCAPE '\\'` to each prefix conjunct.
  `unload_generator.py` inherits the fix via `build_rule_predicate`.

- **LF database grant reduced from `ALL` to `DESCRIBE`** — the Lambda only
  needs to read database metadata; `CREATE_TABLE`, `ALTER`, and `DROP` (all
  included in `ALL`) are not used and violated least privilege.

- **KMS encryption for config writes** — `solution-config.json` writes in the
  `ConfigResourceFunction` and `_disable_bucket_in_config` now apply SSE-KMS
  when `KmsKeyArn` is set, matching the encryption already applied to state
  objects. `ConfigResourceRole` gains the `kms:GenerateDataKey` /
  `kms:Decrypt` / `kms:DescribeKey` permissions via an inline `!If [HasKmsKey]`
  statement.

### Observability

- **CloudWatch log groups with 90-day retention** — explicit
  `AWS::Logs::LogGroup` resources added for all five Lambda functions
  (`ReplicationLambda`, `ConfigResourceFunction`,
  `ReplicationRolePassGranterFunction`, `LFAdminGranterFunction`,
  `LFPermissionsGranterFunction`, `CodeLocationParserFunction`). Custom
  resource invocations declare `DependsOn` on their log group so the group
  exists before first invocation. All groups carry `DeletionPolicy: Retain`.

- **Structured audit logging in privileged custom resource Lambdas** —
  `ReplicationRolePassGranterFunction`, `LFAdminGranterFunction`, and
  `LFPermissionsGranterFunction` now emit `logging.INFO` / `logging.WARNING` /
  `logging.ERROR` entries before and after every privileged operation
  (`iam:PutRolePolicy`, `lakeformation:PutDataLakeSettings`, `sts:AssumeRole`,
  `lakeformation:GrantPermissions`, `lakeformation:RevokePermissions`).

### Documentation

- **Deliberate-design comment** — `config_resource/index.py` (and its
  ZipFile copy in `deploy/template.yaml`) now explains that the fresh config
  written on CloudFormation Update intentionally clears per-bucket `disabled`
  flags: a stack update is the expected operator recovery path after an
  `InlineHashCeiling` breach.

## [0.1.1] — prior release

See git log.

## [0.1.0] — initial release

See git log.
