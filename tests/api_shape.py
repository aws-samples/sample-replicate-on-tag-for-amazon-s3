"""Validate captured mock calls against botocore's own AWS service models.

Every adapter in ``src/adapters`` takes an injected boto3 client, and every
test passes a ``MagicMock``. A ``MagicMock`` accepts any keyword, at any
nesting depth, with any type — so a request parameter that does not exist in
the AWS API looks identical to one that does. That is not a hypothetical: the
job-submission path passed ``Manifest.Location.ManifestEncryption``, which S3
Control has never had, and the entire test suite was green while no
KMS-configured deployment could submit a job at all. botocore rejected the
call during parameter validation, before signing, so the failure surfaced only
as a runtime ``CREATE_FAILED`` in an account with ``KmsKeyArn`` set.

:func:`assert_calls_match_api` closes that gap. It replays the kwargs a mock
client actually received through ``botocore.validate.validate_parameters``
against the real service model, which is the same validation the SDK performs
on a live call. An invented parameter, a missing required parameter, or a
wrongly-typed one fails in unit tests instead of in an account.

What this does not check: whether a value is *semantically* accepted by the
service (a malformed ARN, a nonexistent bucket, an ETag that no longer
matches). Those need a live call or a stubbed response, and are covered
elsewhere.
"""
from __future__ import annotations

from typing import Any

import botocore.session
from botocore import xform_name
from botocore.validate import validate_parameters

_SESSION = botocore.session.get_session()
_MODELS: dict[str, Any] = {}


def _service_model(service_name: str):
    if service_name not in _MODELS:
        _MODELS[service_name] = _SESSION.get_service_model(service_name)
    return _MODELS[service_name]


def _operation_for_method(service_name: str, method_name: str) -> str | None:
    """Map a boto3 client method name to its API operation name.

    boto3 derives ``put_object`` from ``PutObject`` via ``xform_name``, so the
    same transform identifies which operation a recorded mock call refers to.
    Returns ``None`` for a method that is not an API operation (``exceptions``,
    ``get_paginator``, and anything else a test happens to touch on the mock).
    """
    model = _service_model(service_name)
    for operation_name in model.operation_names:
        if xform_name(operation_name) == method_name:
            return operation_name
    return None


def assert_calls_match_api(mock_client, service_name: str, *, expected: int | None = None) -> list[str]:
    """Validate every API call recorded on *mock_client* against *service_name*.

    Parameters
    ----------
    mock_client:
        A ``MagicMock`` that stood in for a boto3 client.
    service_name:
        botocore service name — ``"s3"``, ``"s3control"``, ``"athena"``,
        ``"sns"``, ``"cloudwatch"``, ``"lambda"``, ``"logs"``.
    expected:
        When given, assert exactly this many API calls were validated. Guards
        against a test that silently validates nothing because the code path
        under test returned early.

    Returns
    -------
    list[str]
        The operation names validated, in call order — useful for asserting
        ordering in the caller.

    Raises
    ------
    AssertionError
        On a parameter that the service model rejects, with the operation and
        the botocore message. Also when *expected* does not match.
    """
    validated: list[str] = []

    for call in mock_client.mock_calls:
        method_path, args, kwargs = call
        # Only direct method calls on the client itself: "put_object", not
        # "put_object().__str__" or a nested attribute mock.
        if "." in method_path or "(" in method_path or not method_path:
            continue
        operation_name = _operation_for_method(service_name, method_path)
        if operation_name is None:
            continue
        if args:
            raise AssertionError(
                f"{service_name}.{method_path} was called with positional "
                f"arguments {args!r}; boto3 requires keyword arguments"
            )
        model = _service_model(service_name)
        input_shape = model.operation_model(operation_name).input_shape
        if input_shape is None:
            validated.append(operation_name)
            continue
        try:
            validate_parameters(kwargs, input_shape)
        except Exception as exc:  # botocore raises ParamValidationError
            raise AssertionError(
                f"{service_name}:{operation_name} was called with parameters "
                f"the AWS API does not accept.\n{exc}\n"
                f"Called with: {sorted(kwargs)}"
            ) from exc
        validated.append(operation_name)

    if expected is not None:
        assert len(validated) == expected, (
            f"expected {expected} validated {service_name} call(s), got "
            f"{len(validated)}: {validated}. A count of 0 usually means the "
            f"code under test returned before reaching the API call."
        )
    return validated
