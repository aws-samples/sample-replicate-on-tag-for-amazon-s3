"""AWS Lambda handler for the tag-based S3 replication backfill Solution.

Loads the Solution_Config from the State_Bucket, builds the Runtime_Config from
environment variables, and delegates to ``run_interval`` for a single
Processing_Interval execution.

Any failure (missing env var, S3 error, malformed config) propagates as an
unhandled exception so Lambda surfaces it as an invocation failure.

When the per-bucket circuit breaker trips after too many consecutive S3
Batch Operations job failures, ``run_interval`` calls the
``on_bucket_disable`` callback provided here.  The
callback writes ``disabled=true``, ``disabled_reason``, and ``disabled_at``
back to ``solution-config.json`` so the bucket is skipped on subsequent
intervals.  Other buckets in the same run are unaffected.

Self-service recovery: the callback also (a) clears the bucket's persisted
``SubmissionRecord`` in the state store, so a stale/dead ``job_id`` cannot
immediately re-trip the circuit breaker as soon as the bucket is
re-enabled, and (b) publishes an alert (SNS email, when ``AlarmEmail`` is
configured, plus a CloudWatch Logs entry unconditionally) naming the exact
recovery step: set ``disabled: false`` for this bucket in
``solution-config.json`` on the State Bucket. No Lambda invocation or
manual state-object edit is required to recover.

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
from src.core import observability
from src.core.manifest_strategy import JOURNAL_READ_ROW_CAP_DEFAULT
from src.core.reinvocation import should_reinvoke
from src.core.row_cap_validation import validate_row_cap
from src.orchestrator import _completion_report_prefix, run_interval

_DEFAULT_CONFIG_KEY = "config/solution-config.json"
_TERMINAL_JOB_STATUSES = ("Complete", "Failed", "Cancelled")

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
    config_key: str,
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
    ``disabled: false`` for this bucket's entry in the Solution_Config —
    the disable callback has already cleared this bucket's submission
    history, so no other action is needed.

    Any exception raised by the log write or the SNS publish propagates to
    the caller — the caller (the disable callback) is responsible for
    isolating this failure so it never blocks the (more important)
    disabled-flag write itself.
    """
    recovery = (
        f'Set "disabled": false for bucket {bucket_name!r} in the '
        f"Solution_Config at s3://{state_bucket}/{config_key} and "
        f"wait for the next scheduled run. No other action is "
        f"needed — this bucket's prior S3 Batch Operations job "
        f"failure history has already been cleared."
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


def _disable_bucket_in_config(
    s3_client,
    state_bucket: str,
    config_key: str,
    bucket_name: str,
    reason: str,
    kms_key_arn: str | None = None,
    sns_client=None,
    logs_client=None,
    batch_job_failure_topic_arn: str | None = None,
    batch_job_failure_log_group: str = "",
) -> None:
    """Mark a bucket as disabled in solution-config.json on the State Bucket.

    Reads the current config, finds the matching bucket entry, adds
    ``disabled=true``, ``disabled_reason``, and ``disabled_at``, then writes
    it back conditionally with ``If-Match`` against the ETag from the read.
    Failures are logged but not re-raised — the caller continues
    processing other buckets regardless.

    When ``kms_key_arn`` is provided the write uses SSE-KMS with that key,
    matching the encryption applied to state objects (consistency with
    state_store.py).

    Self-service recovery (see module docstring): after successfully
    writing the disabled flag, this also (a) best-effort clears the
    bucket's persisted ``SubmissionRecord`` via
    ``StateStore.clear_submission_records`` so a stale ``job_id`` cannot
    immediately re-trip the circuit breaker on re-enable, and (b)
    best-effort publishes the recovery-instructions alert via
    :func:`_publish_bucket_disabled_alert` when ``logs_client`` is
    provided. Neither step blocks or is blocked by the other, and neither
    failing prevents the disabled-flag write itself from having already
    succeeded — both are isolated in their own try/except.
    """
    try:
        resp = s3_client.get_object(Bucket=state_bucket, Key=config_key)
        # Passed through exactly as S3 returned it (quotes included), matching
        # state_store's conditional writes. A response without an ETag would
        # leave no precondition to write under, so treat that as a failure
        # rather than writing unconditionally.
        config_etag = resp.get("ETag")
        if not config_etag:
            _logger.error(
                "Could not disable bucket %r — the config read returned no ETag, "
                "so the write cannot be made conditional. Bucket is NOT reported "
                "as disabled. Reason for disable attempt: %s",
                bucket_name,
                reason,
            )
            return
        config = json.loads(resp["Body"].read().decode("utf-8"))
        patched = False
        for entry in config.get("buckets", []):
            if entry.get("name") == bucket_name:
                entry["disabled"] = True
                entry["disabled_reason"] = reason
                entry["disabled_at"] = datetime.now(tz=UTC).isoformat()
                patched = True
                break
        if not patched:
            _logger.error(
                "Could not find bucket %r in config to disable it", bucket_name
            )
            return
        put_kwargs: dict = {
            "Bucket": state_bucket,
            "Key": config_key,
            "Body": json.dumps(config, indent=2).encode("utf-8"),
            "ContentType": "application/json",
            "IfMatch": config_etag,
        }
        if kms_key_arn:
            put_kwargs["ServerSideEncryption"] = "aws:kms"
            put_kwargs["SSEKMSKeyId"] = kms_key_arn
        s3_client.put_object(**put_kwargs)
        observability.emit(observability.log_audit(
            action="bucket_disabled",
            source_bucket=bucket_name,
            details={
                "reason": reason,
                "config_key": config_key,
            },
        ))
        _logger.error(
            "Bucket %r marked disabled in %s. "
            "Re-enable after resolving: %s",
            bucket_name,
            config_key,
            reason,
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "PreconditionFailed":
            _logger.error(
                "Could not disable bucket %r — config was modified concurrently. "
                "Bucket is NOT reported as disabled. Reason for disable attempt: %s",
                bucket_name,
                reason,
            )
        else:
            _logger.error(
                "Failed to write disabled flag for bucket %r to %s: %s. "
                "Manual update required.",
                bucket_name,
                config_key,
                exc,
            )
        return
    except Exception as exc:  # noqa: BLE001
        _logger.error(
            "Failed to write disabled flag for bucket %r to %s: %s. "
            "Manual update required.",
            bucket_name,
            config_key,
            exc,
        )
        return

    # Self-service recovery, part 1: clear the stale SubmissionRecord so
    # re-enabling this bucket doesn't immediately re-trip the circuit
    # breaker on the same dead job_id. Best-effort — a failure here is
    # logged but must not be treated as a failure of the disable itself
    # (the disabled flag above has already been persisted).
    try:
        state_store_module.StateStore(kms_key_arn=kms_key_arn).clear_submission_records(
            s3_client, state_bucket, bucket_name
        )
    except Exception as exc:  # noqa: BLE001
        _logger.error(
            "Failed to clear stale submission records for bucket %r: %s. "
            "Re-enabling this bucket without resolving that first may "
            "immediately re-trip the circuit breaker.",
            bucket_name,
            exc,
        )

    # Self-service recovery, part 2: publish the recovery-instructions
    # alert. Best-effort and independent of part 1.
    if logs_client is not None:
        try:
            _publish_bucket_disabled_alert(
                sns_client,
                logs_client,
                batch_job_failure_topic_arn,
                batch_job_failure_log_group,
                state_bucket=state_bucket,
                config_key=config_key,
                bucket_name=bucket_name,
                reason=reason,
                now=datetime.now(tz=UTC),
            )
        except Exception as exc:  # noqa: BLE001
            _logger.error(
                "Failed to publish bucket-disabled alert for bucket %r: %s",
                bucket_name,
                exc,
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

    Reads four required variables unconditionally (KeyError propagates on
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

    completion_check_batch_size = env.get("COMPLETION_CHECK_BATCH_SIZE", "")
    if completion_check_batch_size.strip():
        try:
            runtime_config["completion_check_batch_size"] = int(
                completion_check_batch_size.strip()
            )
        except ValueError:
            pass

    completion_item_ttl_hours = env.get("COMPLETION_ITEM_TTL_HOURS", "")
    if completion_item_ttl_hours.strip():
        try:
            runtime_config["completion_item_ttl_hours"] = float(
                completion_item_ttl_hours.strip()
            )
        except ValueError:
            pass  # orchestrator falls back to COMPLETION_ITEM_TTL_DEFAULT

    return runtime_config


def handler(event, context):
    """Lambda entry point — executes one Processing_Interval.

    Reads ``STATE_BUCKET`` and optional ``SOLUTION_CONFIG_KEY`` from the
    environment, downloads and parses the Solution_Config, builds the
    Runtime_Config, and calls ``run_interval``.

    Provides an ``on_bucket_disable`` callback so that if ``run_interval``
    detects a permanent per-bucket failure (InlineHashCeiling exceeded), it
    can mark that bucket as disabled in ``solution-config.json`` without
    affecting the other buckets in the same run.

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

    runtime_config["on_bucket_disable"] = (
        lambda bucket_name, reason: _disable_bucket_in_config(
            s3_client, state_bucket, config_key, bucket_name, reason,
            kms_key_arn=runtime_config.get("kms_key_arn"),
            sns_client=sns_client,
            logs_client=logs_client,
            batch_job_failure_topic_arn=batch_job_failure_topic_arn,
            batch_job_failure_log_group=batch_job_failure_log_group,
        )
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
    # before `_process_bucket` runs at all (disabled buckets: skipped in
    # the `bucket.disabled` check; circuit-broken buckets: the circuit
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
) -> None:
    """Deliver one report-missing escalation: always log, publish to SNS iff
    ``topic_arn`` is present (design.md Decision 9, Requirements 8.3, 8.4).

    The log write always happens and never depends on the SNS outcome; the
    SNS publish is attempted only when ``topic_arn`` is truthy. Neither
    call is wrapped in its own try/except here — the caller
    (``check_report_handler``'s per-config loop) isolates any failure from
    this delivery step per Requirement 8.8.
    """
    cause = (
        "BOPS_Completion_Report has not appeared within 1 hour of "
        "job termination. The replication IAM role likely lacks "
        "s3:PutObject permission to the report location."
    )
    # Structured for the log group, prose for the inbox — see
    # _publish_bucket_disabled_alert for the same split.
    message = json.dumps(
        {
            "event": "completion_report_missing",
            "source_bucket": source_bucket,
            "replication_config_id": replication_config_id,
            "job_id": job_id,
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
                f"An S3 Batch Operations job for source bucket "
                f"{source_bucket} finished more than an hour ago, but its "
                f"completion report has never appeared, so this Solution "
                f"cannot confirm whether those objects replicated.\n\n"
                f"Job ID: {job_id}\n\n"
                f"Most likely cause: the replication IAM role is missing "
                f"s3:PutObject permission for the report location. See the "
                f"CompletionNotificationEmail section of deploy/README.md for "
                f"the exact grant required.\n\n"
                f"Replication itself may well have succeeded — this is a "
                f"reporting gap, not a confirmed replication failure.\n"
            ),
        )


def check_report_handler(event, context):  # noqa: ARG001 — event/context imposed by Lambda runtime
    """Lambda entry point — report-missing detection (design.md Decision 9).

    Runs on its own 5-minute schedule, independent of ``handler``'s
    Processing_Interval. For each Monitored_Bucket, for each
    ``replication_config_id`` with a terminal, unconfirmed
    ``SubmissionRecord`` (``not store.completion_job_exists(job_id)``) not
    currently under alert suppression: checks whether the
    BOPS_Completion_Report object now exists via
    ``bops_report_reader.report_object_exists``. If found, this check-only
    path does nothing further — the next ``handler`` invocation's own
    creation hook reads and merges the report normally. If absent and
    ``completion_tracker.is_report_overdue`` holds, publishes the escalation
    (SNS + log) and marks the config as alerted.

    Each ``replication_config_id``'s processing is wrapped in its own
    try/except so a single failure never blocks the rest of the invocation's
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
        # design.md D6 (task 6.1): the ``submission_records`` dict holds
        # exactly ONE entry, keyed by the per-bucket sentinel
        # (``bucket_name``). Look up that single record directly.
        # ---------------------------------------------------------------
        rec = prior_submissions.get(bucket_name)
        if rec is None:
            continue

        config_id = bucket_name
        alert_identity = bucket_name

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

            terminal_at = job.get("TerminationDate") or job.get("CreationTime")
            if terminal_at is None:
                continue

            report_prefix = _completion_report_prefix(config_id, rec.manifest_key)
            if bops_report_reader.report_object_exists(
                regional_s3, state_bucket, report_prefix
            ):
                # Found — leave it for the next handler() invocation's
                # creation hook to read and merge normally (Decision 9).
                continue

            if completion_tracker.is_report_overdue(terminal_at, now):
                _publish_report_missing_alert(
                    sns_client,
                    logs_client,
                    batch_job_failure_topic_arn,
                    batch_job_failure_log_group,
                    source_bucket=bucket_name,
                    replication_config_id=alert_identity,
                    job_id=rec.job_id,
                    now=now,
                )
                store.add_alerted_config(
                    regional_s3, state_bucket, bucket_name, alert_identity
                )
        except Exception as exc:  # noqa: BLE001 — Requirement 8.8 isolation
            _logger.error(
                "Report-missing check failed for bucket %r (job %r): %s",
                bucket_name, rec.job_id, exc,
            )
            continue
