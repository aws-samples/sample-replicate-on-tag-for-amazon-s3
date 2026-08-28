"""AWS Lambda handler for the tag-based S3 replication backfill Solution.

Loads the Solution_Config from the State_Bucket, builds the Runtime_Config from
environment variables, and delegates to ``run_interval`` for a single
Processing_Interval execution.

Any failure (missing env var, S3 error, malformed config) propagates as an
unhandled exception so Lambda surfaces it as an invocation failure.

When the per-bucket circuit breaker trips after too many consecutive S3
Batch Operations job failures, ``run_interval`` calls the
``on_bucket_disabled`` callback provided here.  By then the orchestrator has
already written ``disabled=true``, ``disabled_reason``, and ``disabled_at``
into that bucket's state object, so the bucket is skipped on subsequent
intervals.  Other buckets in the same run are unaffected.

A separate ``on_journal_unavailable`` callback fires on the first interval in
which a source bucket's S3 Metadata journal cannot be found. That condition
leaves the run itself successful, so it raises no Lambda ``Errors`` metric and
``ReplicationLambdaErrorAlarm`` does not fire; without this callback the bucket
would replicate nothing and report nothing. The bucket is left enabled, since
the remedy is enabling the journal rather than any change to the Solution's
configuration.

Self-service recovery: the same write that sets the flag also clears the
bucket's persisted ``SubmissionRecord``, so a stale/dead ``job_id`` cannot
immediately re-trip the circuit breaker as soon as the bucket is re-enabled.
This callback then publishes an alert (SNS email, when ``AlarmEmail`` is
configured, plus a CloudWatch Logs entry unconditionally) naming the exact
recovery step: set ``disabled`` to ``false`` for this bucket in
``state/<bucket>.json`` on the State Bucket. No Lambda invocation and no
redeploy is required to recover.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 8.1
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, UTC

import boto3
from botocore.exceptions import ClientError

from src.adapters import bops_report_reader
from src.adapters import state_store as state_store_module
from src.core import completion_tracker
from src.core import job_recovery
from src.core import observability
from src.core.manifest_strategy import JOURNAL_READ_ROW_CAP_DEFAULT
from src.core.reinvocation import should_reinvoke
from src.core.row_cap_validation import validate_row_cap
from src.orchestrator import _completion_report_prefix, run_interval

_DEFAULT_CONFIG_KEY = "config/solution-config.json"
_TERMINAL_JOB_STATUSES = job_recovery.TERMINAL_JOB_STATUSES

# Reinvocation_Chain_Limit default (tasks.md "Resolved decisions": at ~6 min
# per capped run this is ~2 hours of continuous draining before deferring to
# the schedule). The `ReinvocationChainLimit` CloudFormation parameter and its
# env var wiring in deploy/template.yaml are added in a later task; until
# then, `REINVOCATION_CHAIN_LIMIT` is simply unset and this default applies.
_REINVOCATION_CHAIN_LIMIT_DEFAULT = 20

_logger = logging.getLogger(__name__)

# SNS rejects a Subject that is non-ASCII, contains a newline, or reaches 100
# characters.
_SNS_MAX_SUBJECT_CHARS = 99


def _sns_subject(text: str) -> str:
    """Coerce *text* into a subject line SNS will accept.

    Strips non-ASCII characters and newlines and truncates to the length limit,
    so a bucket name containing anything unexpected can never cause the publish
    itself to fail — losing the alert entirely would be far worse than losing
    a few characters of its subject.
    """
    ascii_only = text.encode("ascii", "ignore").decode("ascii")
    single_line = " ".join(ascii_only.split())
    return single_line[:_SNS_MAX_SUBJECT_CHARS]


def _publish_bucket_disabled_alert(
    sns_client,
    logs_client,
    topic_arn: str | None,
    log_group_name: str,
    state_bucket: str,
    bucket_name: str,
    reason: str,
    now: datetime,
) -> None:
    """Deliver one bucket-disabled escalation: always log, publish to SNS
    iff ``topic_arn`` is present — mirrors
    :func:`_publish_report_missing_alert`'s log-always/SNS-conditional
    pattern (Requirements 8.3, 8.4) so the same operational guarantee
    (never silent, richer when ``AlarmEmail`` is configured) applies here.

    The message states the concrete, sufficient recovery step: set
    ``disabled`` to ``false`` in the bucket's state object. The same write
    that set the flag also cleared this bucket's submission history, so no
    other action is needed, and no redeploy: a stack update does not touch
    the state object.

    Reached only after the flag has been persisted (see
    ``orchestrator._disable_bucket``), so this never names a recovery step
    for a bucket that is not actually disabled.

    Any exception raised by the log write or the SNS publish propagates to
    the caller, which is responsible for isolating it so a failed alert does
    not look like a failed disable.
    """
    recovery = (
        f'Set "disabled": false for bucket {bucket_name!r} in '
        f"s3://{state_bucket}/"
        f"{state_store_module.state_object_key(bucket_name)} and wait for "
        f"the next scheduled run. No other action is needed — this bucket's "
        f"prior S3 Batch Operations job failure history has already been "
        f"cleared — and no redeploy, since a stack update does not modify "
        f"this object."
    )
    # The log entry stays structured JSON so it remains queryable in
    # CloudWatch Logs Insights; the email gets prose, because a JSON blob in
    # an inbox is not a readable notification.
    message = json.dumps(
        {
            "event": "bucket_disabled",
            "source_bucket": bucket_name,
            "cause": reason,
            "recovery": recovery,
        }
    )
    _write_batch_job_failure_log(
        logs_client, log_group_name, message, now,
        stream_prefix="bucket-disabled",
    )
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=_sns_subject(f"S3 replication paused for {bucket_name}"),
            Message=(
                f"Replication has been paused for source bucket "
                f"{bucket_name}.\n\n"
                f"Why: {reason}\n\n"
                f"No objects newly tagged in this bucket will be replicated "
                f"until it is re-enabled. Other monitored buckets are "
                f"unaffected.\n\n"
                f"To resume:\n{recovery}\n"
            ),
        )


def _publish_submission_failure_alert(
    sns_client,
    logs_client,
    topic_arn: str | None,
    log_group_name: str,
    bucket_name: str,
    error_reason: str,
    now: datetime,
) -> None:
    """Deliver one submission-failure escalation: always log, publish to SNS
    iff ``topic_arn`` is present — mirrors
    :func:`_publish_bucket_disabled_alert`'s log-always/SNS-conditional
    pattern (submission-failure-visibility Requirement 3.1, 3.3).

    The message names the bucket, the operation (CreateJob), and the
    validation message, so an operator reading only the email can identify
    the defect without opening CloudWatch Logs.
    """
    message = json.dumps(
        {
            "event": "submission_failure_permanent",
            "source_bucket": bucket_name,
            "operation": "CreateJob",
            "cause": error_reason,
        }
    )
    _write_batch_job_failure_log(
        logs_client, log_group_name, message, now,
        stream_prefix="submission-failure",
    )
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=_sns_subject(
                f"S3 replication submission rejected for {bucket_name}"
            ),
            Message=(
                f"S3 Batch Operations job submission for source bucket "
                f"{bucket_name} was rejected before the request was sent.\n\n"
                f"Operation: CreateJob\n"
                f"Validation error: {error_reason}\n\n"
                f"This is a code defect in the Solution - the request "
                f"contains a parameter the AWS API does not accept. "
                f"Retrying will not help. A fix must be deployed before "
                f"this bucket can replicate.\n\n"
                f"The bucket will be automatically disabled after "
                f"MaxBatchJobFailures consecutive intervals if "
                f"the defect is not fixed.\n"
            ),
        )


def _publish_journal_unavailable_alert(
    sns_client,
    logs_client,
    topic_arn: str | None,
    log_group_name: str,
    bucket_name: str,
    cause: str,
    now: datetime,
) -> None:
    """Deliver one journal-unavailable escalation: always log, publish to SNS
    iff ``topic_arn`` is present — the same log-always/SNS-conditional pattern
    as :func:`_publish_bucket_disabled_alert` and
    :func:`_publish_submission_failure_alert`.

    Fired on the first interval in which a source bucket's S3 Metadata journal
    cannot be found. Without this the condition is only a log line, and because
    the run itself still succeeds it raises no Lambda ``Errors`` metric, so
    ``ReplicationLambdaErrorAlarm`` never fires. Nothing would replicate from
    that bucket and nothing would say so.

    The bucket is not disabled, so no config edit is part of the recovery. The
    message says as much, since an operator who has just been told replication
    is not happening will otherwise go looking for a switch to flip.
    """
    recovery = (
        f"Enable the S3 Metadata journal on bucket {bucket_name!r} (S3 console: "
        f"select the bucket, Metadata configuration, Create metadata "
        f"configuration), and confirm the S3 Tables analytics-services "
        f"integration is enabled in this Region so the s3tablescatalog Glue "
        f"catalog exists. Replication resumes on the next scheduled run once "
        f"the journal is present. This bucket has not been disabled, so no "
        f"configuration change is needed."
    )
    message = json.dumps(
        {
            "event": "journal_unavailable",
            "source_bucket": bucket_name,
            "cause": cause,
            "recovery": recovery,
        }
    )
    _write_batch_job_failure_log(
        logs_client, log_group_name, message, now,
        stream_prefix="journal-unavailable",
    )
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=_sns_subject(
                f"S3 replication cannot read the journal for {bucket_name}"
            ),
            Message=(
                f"The S3 Metadata journal for source bucket {bucket_name} "
                f"could not be read, because the journal table or its "
                f"namespace does not exist.\n\n"
                f"Details: {cause}\n\n"
                f"No objects newly tagged in this bucket will be replicated "
                f"until this is resolved. Retrying will not help on its own, "
                f"since the journal is a prerequisite the Solution reads and "
                f"does not create. Other monitored buckets are "
                f"unaffected.\n\n"
                f"To resolve:\n{recovery}\n"
            ),
        )


def _load_solution_config(s3_client, state_bucket: str, config_key: str) -> dict:
    """Download and parse the Solution_Config JSON object from S3.

    Parameters
    ----------
    s3_client:
        A boto3 S3 client.
    state_bucket:
        Name of the State_Bucket containing the config object.
    config_key:
        S3 object key for the Solution_Config JSON file.

    Returns
    -------
    dict
        The parsed configuration dictionary.
    """
    response = s3_client.get_object(Bucket=state_bucket, Key=config_key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def _build_runtime_config(env: dict) -> dict:
    """Construct the Runtime_Config dict from environment variables.

    Reads five required variables unconditionally (KeyError propagates on
    absence). Includes ``kms_key_arn`` only when ``KMS_KEY_ARN`` is set and
    non-empty after stripping whitespace.

    Includes ``journal_lookback_seconds`` only when ``JOURNAL_LOOKBACK_SECONDS``
    is set and non-empty after stripping; otherwise the orchestrator applies its
    default lookback.

    Parameters
    ----------
    env:
        A mapping of environment variables (typically ``os.environ``).

    Returns
    -------
    dict
        Runtime configuration suitable for ``run_interval``.
    """
    runtime_config = {
        "state_bucket": env["STATE_BUCKET"],
        "athena_workgroup": env["ATHENA_WORKGROUP"],
        "athena_output_location": env["ATHENA_OUTPUT_LOCATION"],
        "account_id": env["ACCOUNT_ID"],
        "batch_operations_role_arn": env["BATCH_OPERATIONS_ROLE_ARN"],
    }

    kms_key_arn = env.get("KMS_KEY_ARN", "")
    if kms_key_arn.strip():
        runtime_config["kms_key_arn"] = kms_key_arn

    lookback = env.get("JOURNAL_LOOKBACK_SECONDS", "")
    if lookback.strip():
        runtime_config["journal_lookback_seconds"] = lookback.strip()

    metrics_namespace = env.get("METRICS_NAMESPACE", "")
    if metrics_namespace.strip():
        runtime_config["metrics_namespace"] = metrics_namespace.strip()

    deployment_id = env.get("METRICS_DEPLOYMENT_ID", "")
    if deployment_id.strip():
        runtime_config["metrics_dimensions"] = {"Deployment": deployment_id.strip()}

    journal_read_row_cap = env.get("JOURNAL_READ_ROW_CAP", "")
    if journal_read_row_cap.strip():
        try:
            runtime_config["journal_read_row_cap"] = int(journal_read_row_cap.strip())
        except ValueError:
            pass  # orchestrator falls back to JOURNAL_READ_ROW_CAP_DEFAULT

    max_failures = env.get("MAX_BATCH_JOB_FAILURES", "")
    if max_failures.strip():
        try:
            runtime_config["max_batch_job_failures"] = int(max_failures.strip())
        except ValueError:
            pass

    max_concurrent_jobs = env.get("MAX_CONCURRENT_JOBS_PER_BUCKET", "")
    if max_concurrent_jobs.strip():
        try:
            runtime_config["max_concurrent_jobs_per_bucket"] = int(
                max_concurrent_jobs.strip()
            )
        except ValueError:
            pass  # orchestrator falls back to MAX_CONCURRENT_JOBS_DEFAULT

    # Reinvocation_Chain_Limit (Requirement 5.1) — the `ReinvocationChainLimit`
    # CFN parameter/env var wiring lands in a later task; read the env var
    # now so the handler is ready for it, falling back to
    # _REINVOCATION_CHAIN_LIMIT_DEFAULT (20) when unset or invalid, following
    # the same env-var-with-default pattern as journal_read_row_cap above.
    reinvocation_chain_limit = env.get("REINVOCATION_CHAIN_LIMIT", "")
    if reinvocation_chain_limit.strip():
        try:
            runtime_config["reinvocation_chain_limit"] = int(
                reinvocation_chain_limit.strip()
            )
        except ValueError:
            pass  # falls back to _REINVOCATION_CHAIN_LIMIT_DEFAULT

    completion_report_topic_arn = env.get("COMPLETION_REPORT_TOPIC_ARN", "")
    if completion_report_topic_arn.strip():
        runtime_config["completion_report_topic_arn"] = (
            completion_report_topic_arn.strip()
        )

    return runtime_config


def handler(event, context):
    """Lambda entry point — executes one Processing_Interval.

    Reads ``STATE_BUCKET`` and optional ``SOLUTION_CONFIG_KEY`` from the
    environment, downloads and parses the Solution_Config, builds the
    Runtime_Config, and calls ``run_interval``.

    Provides an ``on_bucket_disabled`` callback so that when ``run_interval``
    disables a bucket after a permanent per-bucket failure, the operator is
    told which bucket and how to re-enable it. ``run_interval`` persists the
    flag itself, to that bucket's state object, without affecting the other
    buckets in the same run.

    Also provides an ``on_journal_unavailable`` callback, fired on the first
    interval a bucket's S3 Metadata journal is found to be absent, so an unmet
    prerequisite is escalated rather than only logged.

    Self_Reinvocation (Requirements 4.1, 4.4, 4.5, 5.1, 5.3): reads
    ``reinvocation_depth`` from ``event`` (absent on a scheduled
    EventBridge trigger, treated as ``0``). After ``run_interval``
    returns its ``RunOutcome``, computes ``should_reinvoke`` and, when
    true, issues an async self-invoke (``InvocationType='Event'``) against
    ``context.invoked_function_arn`` carrying ``reinvocation_depth + 1``.
    The invoke call is isolated in its own try/except: a trigger failure
    is logged and swallowed, never raised, since the run above already
    completed successfully and the next scheduled trigger remains a
    self-healing fallback.
    """
    state_bucket = os.environ["STATE_BUCKET"]
    config_key = os.environ.get("SOLUTION_CONFIG_KEY", _DEFAULT_CONFIG_KEY)
    batch_job_failure_topic_arn = (
        os.environ.get("BATCH_JOB_FAILURE_TOPIC_ARN", "").strip() or None
    )
    batch_job_failure_log_group = os.environ.get(
        "BATCH_JOB_FAILURE_LOG_GROUP", ""
    ).strip()

    runtime_config = _build_runtime_config(os.environ)

    # -------------------------------------------------------------------
    # Journal_Read_Row_Cap memory-safety validation (Requirement 3.2) —
    # fail fast at configuration load, before any S3/Athena access, rather
    # than risk an out-of-memory failure mid-run. The Lambda's actual
    # configured memory size comes from ``context.memory_limit_in_mb``
    # (always populated by the Lambda runtime for a real invocation);
    # skipped when ``context`` does not carry it (e.g. a library/test
    # caller invoking ``handler`` with ``context=None``), since there is
    # then no memory size to validate against.
    # -------------------------------------------------------------------
    memory_limit_in_mb = getattr(context, "memory_limit_in_mb", None)
    if memory_limit_in_mb is not None:
        row_cap = runtime_config.get(
            "journal_read_row_cap", JOURNAL_READ_ROW_CAP_DEFAULT
        )
        validate_row_cap(row_cap, int(memory_limit_in_mb))

    s3_client = boto3.client("s3")
    config_source = _load_solution_config(s3_client, state_bucket, config_key)

    # logs_client is created unconditionally so the bucket-disabled alert is
    # always logged (Requirement 8.4-style guarantee); sns_client only when
    # a topic ARN is configured, mirroring check_report_handler's pattern.
    logs_client = boto3.client("logs") if batch_job_failure_log_group else None
    sns_client = boto3.client("sns") if batch_job_failure_topic_arn else None

    # Notification only. The orchestrator has already persisted the flag to
    # the bucket's state object by the time this fires, through its own
    # conditional-write ETag chain, so there is nothing to write here.
    runtime_config["on_bucket_disabled"] = (
        lambda bucket_name, reason: _publish_bucket_disabled_alert(
            sns_client,
            logs_client,
            batch_job_failure_topic_arn,
            batch_job_failure_log_group,
            state_bucket=state_bucket,
            bucket_name=bucket_name,
            reason=reason,
            now=datetime.now(tz=UTC),
        )
        if logs_client is not None
        else None
    )

    runtime_config["on_submission_failure"] = (
        lambda bucket_name, error_reason: _publish_submission_failure_alert(
            sns_client=sns_client,
            logs_client=logs_client,
            topic_arn=batch_job_failure_topic_arn,
            log_group_name=batch_job_failure_log_group,
            bucket_name=bucket_name,
            error_reason=error_reason,
            now=datetime.now(tz=UTC),
        )
    )

    runtime_config["on_journal_unavailable"] = (
        lambda bucket_name, cause: _publish_journal_unavailable_alert(
            sns_client=sns_client,
            logs_client=logs_client,
            topic_arn=batch_job_failure_topic_arn,
            log_group_name=batch_job_failure_log_group,
            bucket_name=bucket_name,
            cause=cause,
            now=datetime.now(tz=UTC),
        )
    )

    # -------------------------------------------------------------------
    # Self_Reinvocation depth (Requirement 4.1, design.md "Handler"): a
    # scheduled EventBridge trigger carries no `reinvocation_depth` at all,
    # which is treated identically to depth 0. Guard against `event` being
    # `None`/not a dict, matching the defensive style used elsewhere in this
    # module (e.g. `getattr(context, ...)` above).
    # -------------------------------------------------------------------
    reinvocation_depth = (
        event.get("reinvocation_depth", 0) if isinstance(event, dict) else 0
    )
    # Clamp to non-negative int: a crafted event with a negative depth would
    # bypass the chain_limit check (depth < chain_limit is always True for
    # negative values), creating an effectively unbounded reinvocation chain.
    if not isinstance(reinvocation_depth, int) or reinvocation_depth < 0:
        reinvocation_depth = 0

    outcome = run_interval(config_source, runtime_config)

    # -------------------------------------------------------------------
    # Self_Reinvocation decision + trigger (Requirements 4.1, 4.4, 4.5, 5.3).
    #
    # `bucket_active` reasoning: `RunOutcome.any_capped_and_progressed`
    # (task 6.1 / src/orchestrator.py) is already `True` only when at least
    # one bucket both (a) hit the Journal_Read_Row_Cap this run (a
    # Capped_Run) AND (b) `progressed` — submitted its
    # Batch_Replication_Job and advanced its checkpoint. A disabled or
    # circuit-broken bucket is skipped by `run_interval`'s per-bucket loop
    # before `_process_bucket` runs at all (disabled buckets: skipped by
    # the state-object disable check; circuit-broken buckets: the circuit
    # breaker's own disable path returns early with `progressed` left
    # `False`) — so such a bucket can never contribute `capped=True,
    # progressed=True` to the aggregate. `any_capped_and_progressed` being
    # `True` therefore already implies "some bucket was capped, progressed,
    # AND active" — the three signals `should_reinvoke` would otherwise take
    # as separate `capped`/`progressed`/`bucket_active` arguments collapse
    # into this one aggregate at the run level. Passing
    # `capped=progressed=bucket_active=outcome.any_capped_and_progressed`
    # reproduces exactly this collapsed semantic without re-deriving
    # per-bucket state the handler does not have (RunOutcome intentionally
    # does not expose which bucket triggered it, only the aggregate).
    # -------------------------------------------------------------------
    reinvocation_chain_limit = int(
        runtime_config.get(
            "reinvocation_chain_limit", _REINVOCATION_CHAIN_LIMIT_DEFAULT
        )
    )
    trigger_reinvocation = should_reinvoke(
        capped=outcome.any_capped_and_progressed,
        progressed=outcome.any_capped_and_progressed,
        depth=reinvocation_depth,
        chain_limit=reinvocation_chain_limit,
        bucket_active=outcome.any_capped_and_progressed,
    )
    if trigger_reinvocation:
        invoked_function_arn = getattr(context, "invoked_function_arn", None)
        if invoked_function_arn:
            # Isolated try/except (Requirement 5.3): a failure issuing the
            # self-invoke must be logged and swallowed, never raised — the
            # run above already completed successfully, and the next
            # scheduled trigger remains a self-healing fallback.
            try:
                lambda_client = boto3.client("lambda")
                lambda_client.invoke(
                    FunctionName=invoked_function_arn,
                    InvocationType="Event",
                    Payload=json.dumps(
                        {"reinvocation_depth": reinvocation_depth + 1}
                    ).encode("utf-8"),
                )
                # Structured observability entry (Requirements 6.2, 6.4),
                # matching the existing `journal_read_capped` audit-entry
                # scheme rather than a plain-text log line, so this event is
                # consistent with every other entry in the observability
                # scheme and machine-parseable by the same log pipeline.
                # `chain_position` is the depth the newly-triggered
                # invocation will run at (reinvocation_depth + 1).
                observability.emit(observability.log_reinvocation_triggered(
                    chain_position=reinvocation_depth + 1,
                ))
            except Exception as exc:  # noqa: BLE001
                _logger.error(
                    "Failed to trigger Self_Reinvocation (depth %d): %s. "
                    "The completed run is unaffected; the next scheduled "
                    "trigger will still make progress.",
                    reinvocation_depth,
                    exc,
                )
        else:
            # No `context` (e.g. a library/test caller) or a context that
            # doesn't carry `invoked_function_arn` — there is no target to
            # self-invoke, so skip gracefully rather than raising.
            _logger.warning(
                "Self_Reinvocation was indicated but context.invoked_"
                "function_arn is unavailable; skipping."
            )
    elif outcome.any_capped_and_progressed and reinvocation_depth >= reinvocation_chain_limit:
        # Requirement 6.3: distinguish "eligible to reinvoke but blocked by
        # the Reinvocation_Chain_Limit" (this branch) from "nothing to
        # reinvoke for" (not capped, not progressed, or an inactive bucket —
        # `trigger_reinvocation` is False for a different reason and this
        # branch's guard, `any_capped_and_progressed`, is also False, so no
        # entry is emitted). Only when the run was otherwise eligible
        # (capped + progressed + active, collapsed into
        # `any_capped_and_progressed`) AND `depth >= chain_limit` was the
        # actual reason `should_reinvoke` returned `False` do we emit this
        # entry — the limit stopped further reinvocation and backlog
        # remains for the next scheduled trigger.
        observability.emit(observability.log_reinvocation_chain_limit_reached(
            chain_limit=reinvocation_chain_limit,
            depth=reinvocation_depth,
        ))


# ---------------------------------------------------------------------------
# check_report_handler — report-missing detection (design.md Decision 9)
# ---------------------------------------------------------------------------


def _write_batch_job_failure_log(
    logs_client,
    log_group_name: str,
    message: str,
    now: datetime,
    stream_prefix: str,
) -> None:
    """Write one structured log entry to the existing ``BatchJobFailureLogGroup``.

    Creates a log stream for this invocation if it does not already exist
    (tolerating ``ResourceAlreadyExistsException``), then puts a single log
    event. This is a distinct log group from the one this Lambda's own
    invocations write to — writing here is what keeps the alert visible via
    the existing ``BatchJobFailureLogGroup``/``BatchJobFailureAlarm``
    CloudWatch mechanism even when ``AlarmEmail`` is not configured
    (Requirement 8.4).

    ``stream_prefix`` names the alert kind, so each kind lands in its own
    daily stream. It is a required parameter rather than a default because
    without it a new alert kind could silently reuse another kind's stream
    name, which is misleading when navigating the log group. A default would
    let the next alert kind reintroduce that silently.

    Any exception here (e.g. missing IAM permission) propagates to the
    caller, which is responsible for isolating it per-config (Requirement
    8.8) — this function itself performs no isolation.
    """
    stream_name = f"{stream_prefix}-{now.strftime('%Y-%m-%d')}"
    try:
        logs_client.create_log_stream(
            logGroupName=log_group_name, logStreamName=stream_name
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    logs_client.put_log_events(
        logGroupName=log_group_name,
        logStreamName=stream_name,
        logEvents=[
            {"timestamp": int(now.timestamp() * 1000), "message": message}
        ],
    )


def _publish_report_missing_alert(
    sns_client,
    logs_client,
    topic_arn: str | None,
    log_group_name: str,
    source_bucket: str,
    replication_config_id: str,
    job_id: str,
    now: datetime,
    reason: str = "missing",
) -> None:
    """Deliver one completion-report escalation: always log, publish to SNS iff
    ``topic_arn`` is present (design.md Decision 9, Requirements 8.3, 8.4).

    The log write always happens and never depends on the SNS outcome; the
    SNS publish is attempted only when ``topic_arn`` is truthy. Neither
    call is wrapped in its own try/except here — the caller
    (``check_report_handler``'s per-config loop) isolates any failure from
    this delivery step per Requirement 8.8.

    *reason* selects between the two conditions that leave a terminal job's
    outcomes unconfirmed. Both produce the same event type and the same
    operator action, so they share one alert rather than two:

    ``missing``                 No report manifest exists an hour after the job
                                terminated. The likely cause is the Batch
                                Operations job role lacking ``s3:PutObject`` on
                                the report location.
    ``present but unconsumed``  A report manifest exists but the Solution has
                                not consumed it after several of its own
                                intervals, so it cannot read it. Likely causes
                                are a checksum or row-count mismatch, a
                                malformed row, or the report objects having been
                                removed by the State Bucket's
                                ``LifecycleExpirationDays`` rule (Requirement 1.7).
    """
    unconsumed = reason == "present but unconsumed"
    if unconsumed:
        cause = (
            "BOPS_Completion_Report has existed for 48 hours without being "
            "consumed, so the Solution cannot read it. Likely a checksum or "
            "row-count mismatch, a malformed row, or report objects expired by "
            "the State Bucket lifecycle rule."
        )
    else:
        cause = (
            "BOPS_Completion_Report has not appeared within 1 hour of "
            "job termination. The stack-created Batch Operations job role "
            "likely lacks s3:PutObject on the report location."
        )
    # Structured for the log group, prose for the inbox — see
    # _publish_bucket_disabled_alert for the same split.
    message = json.dumps(
        {
            "event": "completion_report_missing",
            "source_bucket": source_bucket,
            "replication_config_id": replication_config_id,
            "job_id": job_id,
            "reason": reason,
            "cause": cause,
        }
    )
    _write_batch_job_failure_log(
        logs_client, log_group_name, message, now,
        stream_prefix="report-missing",
    )
    if topic_arn:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=_sns_subject(
                f"S3 replication cannot confirm completion for {source_bucket}"
            ),
            Message=(
                (
                    f"An S3 Batch Operations job for source bucket "
                    f"{source_bucket} wrote a completion report more than 48 "
                    f"hours ago, but this Solution has not been able to read "
                    f"that report, so it cannot confirm whether those objects "
                    f"replicated.\n\n"
                    f"Job ID: {job_id}\n\n"
                    f"Most likely causes: the report failed an integrity "
                    f"check (a result object's MD5 checksum or the total row "
                    f"count did not match what DescribeJob reported, or a row "
                    f"was malformed), or the report objects were deleted by "
                    f"the State Bucket's lifecycle rule on the "
                    f"completion-reports/ prefix before they could be read. "
                    f"That rule uses the LifecycleExpirationDays parameter, "
                    f"which defaults to 30 days.\n\n"
                    f"The report itself is still the record of what happened, "
                    f"if it has not expired: read it under the "
                    f"completion-reports/ prefix of the State Bucket.\n\n"
                    f"Replication itself may well have succeeded — this is a "
                    f"reporting gap, not a confirmed replication failure.\n"
                )
                if unconsumed
                else (
                    f"An S3 Batch Operations job for source bucket "
                    f"{source_bucket} finished more than an hour ago, but its "
                    f"completion report has never appeared, so this Solution "
                    f"cannot confirm whether those objects replicated.\n\n"
                    f"Job ID: {job_id}\n\n"
                    f"Most likely cause: the Batch Operations job role this "
                    f"stack created is missing s3:PutObject on the report "
                    f"location under the State Bucket's completion-reports/ "
                    f"prefix. The stack grants that itself, so this is a "
                    f"defect in the deployment rather than something to fix "
                    f"on a role you own: check the policy on the role named "
                    f"by the BatchOperationsRoleArn stack output against the "
                    f"Batch Operations job role table in "
                    f"docs/permissions.md.\n\n"
                    f"Replication itself may well have succeeded — this is a "
                    f"reporting gap, not a confirmed replication failure.\n"
                )
            ),
        )


def check_report_handler(event, context):  # noqa: ARG001 — event/context imposed by Lambda runtime
    """Lambda entry point — report-missing detection (design.md Decision 9).

    Runs on its own 5-minute schedule, independent of ``handler``'s
    Processing_Interval. For each Monitored_Bucket, for **every**
    ``SubmissionRecord`` that is terminal, unconfirmed
    (``not store.completion_job_exists(job_id)``), and not currently under alert
    suppression: checks whether the exact top-level BOPS_Completion_Report
    manifest now exists via ``bops_report_reader.report_manifest_written_at``,
    which returns the manifest's ``LastModified`` or ``None``.

    Every record, because a bucket may have several jobs outstanding at once. The
    previous single-record lookup meant the one mechanism built to detect an
    unread report was blind to the case that produced one. Suppression is keyed
    by ``job_id`` for the same reason (Requirements 1.4, 1.5).

    If the manifest was written recently, this check-only path does nothing
    further — the next ``handler`` invocation's own creation hook reads and
    merges the report normally. If it has existed longer than
    ``completion_tracker.is_report_unconsumed_overdue`` allows, the Solution
    cannot read it and the same escalation fires with a different reason
    (Requirement 1.7).

    That second condition is measured from when the report was written, not from
    a job timestamp, because a job's duration is unbounded: measuring from
    ``CreationTime`` would give a job that ran for days no grace at all.

    Terminal jobs with zero invoked tasks are skipped because S3 does not create
    their reports. If the manifest is absent and
    ``completion_tracker.is_report_overdue`` holds, the handler publishes the
    escalation (SNS + log) and marks the config as alerted.

    Each record's processing is wrapped in its own try/except so a single
    failure never blocks the rest of the invocation's
    batch (Requirement 8.8). This handler is entirely independent of
    ``run_interval`` — it never touches the checkpoint, lease, or
    Batch_Replication_Job submission path.

    Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.7, 8.8
    """
    from src.core import config_loader  # local import to avoid a module cycle

    state_bucket = os.environ["STATE_BUCKET"]
    config_key = os.environ.get("SOLUTION_CONFIG_KEY", _DEFAULT_CONFIG_KEY)
    account_id = os.environ["ACCOUNT_ID"]
    batch_job_failure_topic_arn = (
        os.environ.get("BATCH_JOB_FAILURE_TOPIC_ARN", "").strip() or None
    )
    batch_job_failure_log_group = os.environ.get(
        "BATCH_JOB_FAILURE_LOG_GROUP", ""
    ).strip()
    # This handler writes the same state objects as the main handler (via
    # add_alerted_config / clear_alerted_config), so it must encrypt them the
    # same way. Without this, a deployment that sets KmsKeyArn would have its
    # SSE-KMS state objects silently rewritten under SSE-S3 whenever this
    # handler touched them — an encryption downgrade.
    kms_key_arn = os.environ.get("KMS_KEY_ARN", "").strip() or None

    s3_client = boto3.client("s3")
    config_source = _load_solution_config(s3_client, state_bucket, config_key)
    app_config = config_loader.load_config(config_source)

    store = state_store_module.StateStore(kms_key_arn=kms_key_arn)
    logs_client = boto3.client("logs")
    sns_client = boto3.client("sns") if batch_job_failure_topic_arn else None

    now = datetime.now(tz=UTC)

    for bucket in app_config.buckets:
        bucket_name = bucket.name

        try:
            regional_s3 = boto3.client("s3", region_name=bucket.region)
            s3control_client = boto3.client("s3control", region_name=bucket.region)
        except Exception as exc:  # noqa: BLE001 — Requirement 8.8 isolation
            _logger.error(
                "Failed to create regional clients for bucket %r (region %r): %s",
                bucket_name, bucket.region, exc,
            )
            continue

        try:
            alerted = store.get_alerted_configs(regional_s3, state_bucket, bucket_name)
        except Exception as exc:  # noqa: BLE001 — Requirement 8.8 isolation
            _logger.error(
                "Failed to read alerted configs for bucket %r: %s", bucket_name, exc,
            )
            continue

        try:
            prior_submissions = store.get_submission_records(
                regional_s3, state_bucket, bucket_name
            )
        except Exception as exc:  # noqa: BLE001 — Requirement 8.8 isolation
            _logger.error(
                "Failed to read submission records for bucket %r: %s", bucket_name, exc,
            )
            continue

        # ---------------------------------------------------------------
        # Every submission record for the bucket, not one. A bucket may have
        # several jobs outstanding at once (MaxConcurrentJobsPerBucket), and
        # indexing a single record was the reason a missing report could go
        # undetected for exactly the job that produced one. Requirement 1.4.
        # ---------------------------------------------------------------
        for rec in list(prior_submissions.values()):
            # The report prefix is derived from the bucket name at submission
            # time, so the read side must derive it the same way. Unlike the
            # suppression identity below, this is not per job.
            config_id = bucket_name
            # Suppression is per job (Requirement 1.5): keying it on the bucket
            # would let one job's alert hide every other job's. The stored list
            # holds arbitrary strings, so this needs no schema change, and a
            # bucket-name entry left by an earlier build is inert — nothing
            # reads or writes it any more.
            alert_identity = rec.job_id

            try:
                if not rec.job_id:
                    continue
                if alert_identity in alerted:
                    continue  # suppressed (Requirement 8.5)
                if store.completion_job_exists(
                    regional_s3, state_bucket, bucket_name, rec.job_id
                ):
                    continue  # already confirmed — nothing to detect as missing

                resp = s3control_client.describe_job(
                    AccountId=account_id, JobId=rec.job_id
                )
                job = resp["Job"]
                status = job.get("Status")
                if status not in _TERMINAL_JOB_STATUSES:
                    continue  # not yet terminal — nothing to check

                progress_summary = job.get("ProgressSummary", {})
                tasks_succeeded = progress_summary.get("NumberOfTasksSucceeded")
                tasks_failed = progress_summary.get("NumberOfTasksFailed")
                if tasks_succeeded == 0 and tasks_failed == 0:
                    # S3 does not generate a completion report until at least
                    # one task is invoked, so a report-missing alert would be
                    # false.
                    continue

                terminal_at = job.get("TerminationDate") or job.get("CreationTime")
                if terminal_at is None:
                    continue

                report_prefix = _completion_report_prefix(
                    config_id, rec.manifest_key
                )
                report_written_at = bops_report_reader.report_manifest_written_at(
                    regional_s3, state_bucket, report_prefix, rec.job_id
                )

                # Reaching this point means the job is terminal, invoked at least
                # one task, and is NOT recorded as processed in state. Both
                # branches below escalate from that, on different clocks:
                #
                #   report absent      S3 writes a report within minutes of
                #                      terminal, so an hour is enough to
                #                      conclude it is not coming.
                #   report present     The Solution has had several of its own
                #                      intervals to consume a report that exists
                #                      and has not, which means it cannot read
                #                      it. No extra read is needed to establish
                #                      that, so this branch does not download
                #                      the report.
                #
                # A present report inside the shorter window is left alone for
                # the next handler() invocation to read and merge normally
                # (Decision 9).
                #
                # The two branches measure from different points on purpose. A
                # missing report is measured from the job terminating, since S3
                # writes it within minutes of that. An unconsumed report is
                # measured from when the report itself was written, because a
                # job's duration is unbounded and measuring from a job timestamp
                # would give a long-running job no grace at all.
                if report_written_at is not None:
                    overdue = completion_tracker.is_report_unconsumed_overdue(
                        report_written_at, now
                    )
                    reason = "present but unconsumed"
                else:
                    overdue = completion_tracker.is_report_overdue(terminal_at, now)
                    reason = "missing"

                if overdue:
                    _publish_report_missing_alert(
                        sns_client,
                        logs_client,
                        batch_job_failure_topic_arn,
                        batch_job_failure_log_group,
                        source_bucket=bucket_name,
                        replication_config_id=alert_identity,
                        job_id=rec.job_id,
                        now=now,
                        reason=reason,
                    )
                    store.add_alerted_config(
                        regional_s3, state_bucket, bucket_name, alert_identity
                    )
                    # Keep the in-memory set in step with the write, so two
                    # records that somehow share an identity cannot both alert
                    # within one invocation.
                    alerted.add(alert_identity)
            except Exception as exc:  # noqa: BLE001 — Requirement 8.8 isolation
                # Per record, so one job's failure does not stop the next job's
                # check for the same bucket.
                _logger.error(
                    "Report-missing check failed for bucket %r (job %r): %s",
                    bucket_name, rec.job_id, exc,
                )
                continue
