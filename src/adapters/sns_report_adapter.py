"""Completion_Tracker adapter — publishes a Completion_Report to SNS.

This is the thin I/O shell for step 8 of the Completion_Tracker design
(Decision 8): publishing the JSON payload produced by
``completion_tracker.build_completion_report`` to the stack-provisioned
``CompletionReportTopic``. All business logic for *when* a report is ready to
publish (resolution + Quiescence_Check) and *what* the report contains lives
in the pure core; this module is responsible only for the SNS API call and
translating its outcome into a :class:`PublishResult` the orchestrator can
log without raising (Requirement 4.5) so a publish failure never escapes the
isolated completion-tracking step (Requirement 6).

Follows the same timeout-wrapped, non-raising pattern as
``batch_operations_adapter.py``'s ``submit_batch_job`` / ``_call_with_timeout``.

Requirements: 4.5
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from botocore.exceptions import ClientError

from src.adapters._aws_call_helpers import call_with_timeout as _call_with_timeout
from src.adapters._aws_call_helpers import client_error_reason as _client_error_reason

# ---------------------------------------------------------------------------
# Timeout budget (same bound as batch_operations_adapter.py, Requirements 10.1/10.3)
# ---------------------------------------------------------------------------
#
# Maximum wall-clock seconds to spend waiting for the publish call. The real
# bound against a TCP-level stall is the socket-level connect/read timeout
# configured on the client by ClientFactory (code-review-remediation spec
# Req 5); _TIMEOUT_SECONDS here is a defense-in-depth thread-pool bound
# re-exported from ``src.adapters._aws_call_helpers`` (formerly duplicated in
# this module).


# ---------------------------------------------------------------------------
# PublishResult
# ---------------------------------------------------------------------------


@dataclass
class PublishResult:
    """Return value of :func:`publish_completion_report`.

    Attributes
    ----------
    success:
        ``True`` iff the SNS ``Publish`` call completed successfully.
    message_id:
        The SNS-assigned message id; ``None`` unless ``success`` is ``True``.
    error_reason:
        Human-readable failure reason (a botocore error code/message, a
        timeout description, or another exception's string form); ``None``
        when ``success`` is ``True``. Never raised — callers log this value
        per Requirement 4.5 rather than catching an exception.
    """

    success: bool
    message_id: str | None = None
    error_reason: str | None = None


# ---------------------------------------------------------------------------
# publish_completion_report — public interface
# ---------------------------------------------------------------------------


def publish_completion_report(
    sns_client, topic_arn: str, report: dict, subject: str = "",
) -> PublishResult:
    """Publish one Completion_Report to the SNS_Completion_Topic.

    *subject* is the email subject line, built by the caller (see
    ``completion_tracker.format_completion_report_subject``) so this module
    stays a pure I/O shell. When empty the parameter is omitted from the
    ``publish`` call, since SNS rejects a blank Subject — the message body is
    unaffected either way.

    Implements the publish half of Decision 8 (Requirement 4.5). The call is
    wrapped in the same timeout budget ``batch_operations_adapter.py`` uses
    for AWS API calls, and every failure mode — a timeout, a
    :class:`~botocore.exceptions.ClientError`, or any other exception — is
    translated into a failed :class:`PublishResult` rather than propagated,
    so the caller (the isolated completion-tracking step, Requirement 6) can
    log the failure and retry at the next Completion_Poll_Interval without
    the exception itself aborting the rest of that pass.

    The SNS message body is ``report`` serialized as pretty-printed JSON
    (``json.dumps(report, indent=2)``), unchanged in structure. ``report``
    itself carries a human-readable ``summary`` field as its first key
    (see ``completion_tracker.build_completion_report``), so an operator
    reading the raw notification (e.g. the SNS-to-email body) sees the
    headline result on the first content line without parsing JSON, while
    the message body remains strictly valid JSON for every other SNS
    subscriber protocol (SQS, Lambda, HTTPS).

    Parameters
    ----------
    sns_client:
        A boto3 ``sns`` client.
    topic_arn:
        ARN of the stack-provisioned ``CompletionReportTopic``. This adapter
        does not care whether the ARN is stack-provisioned or otherwise —
        it simply publishes to whatever ARN it is given.
    report:
        The JSON-serializable dict produced by
        ``completion_tracker.build_completion_report(job)``.

    Returns
    -------
    PublishResult
        ``success=True`` with the SNS ``MessageId`` on success, or
        ``success=False`` with a human-readable ``error_reason`` on any
        failure.

    Requirements: 4.5
    """
    kwargs: dict = {"TopicArn": topic_arn, "Message": json.dumps(report, indent=2)}
    if subject:
        # Omitted rather than sent empty: SNS rejects a blank Subject.
        kwargs["Subject"] = subject

    try:
        response = _call_with_timeout(lambda: sns_client.publish(**kwargs))
    except TimeoutError as exc:
        # API call hung beyond _TIMEOUT_SECONDS; report immediately (4.5)
        return PublishResult(success=False, error_reason=str(exc))
    except ClientError as exc:
        # AWS returned an explicit error response (permission denied, etc.)
        return PublishResult(success=False, error_reason=_client_error_reason(exc))
    except Exception as exc:  # noqa: BLE001
        # Network error, unexpected SDK error, etc.
        return PublishResult(
            success=False,
            error_reason=str(exc) or "unknown error during sns publish",
        )

    return PublishResult(success=True, message_id=response.get("MessageId"))



