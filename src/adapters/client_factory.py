"""Source-side boto3 client factory with startup no-destination-client guard.

Creates and caches source-side boto3 clients (S3, Athena, S3 Control), keyed
by ``(service, region)``.  The cache is thread-safe: a ``threading.Lock``
single-flights construction so that concurrent bucket threads (Req 2) share one
client per service+region pair instead of creating redundant connections.

No destination-account or destination-region client is ever constructed here —
this is the core architectural guarantee for open-source deployment.

The :meth:`ClientFactory.check_no_destination_client` startup smoke check
enforces this invariant by inspecting the factory method signatures at startup,
failing fast if any destination-side parameter name is detected.

Requirements: 12.2, 13.1
"""
from __future__ import annotations

import inspect
import threading
from typing import Any

import boto3
from botocore.config import Config

# ---------------------------------------------------------------------------
# Socket-level timeout budget (code-review-remediation spec Req 5)
# ---------------------------------------------------------------------------
#
# Every client this factory constructs is bounded at the socket layer so a
# TCP-level stall (connected but silent) raises botocore's own
# ConnectTimeoutError/ReadTimeoutError within a bounded time, instead of
# hanging indefinitely. This is the actual fix for the underlying problem the
# thread-backed `_call_with_timeout` wrappers in `batch_operations_adapter.py`
# and `sns_report_adapter.py` were trying (and failing) to solve on their own:
# a thread-pool `future.result(timeout=...)` cannot bound a call whose
# underlying socket read never times out, because exiting the
# `ThreadPoolExecutor` context still waits for that same hung thread to
# finish. Bounding the socket itself makes the call return control on its
# own within the budget.
#
# `read_timeout` bounds a single HTTP request/response cycle; `connect_timeout`
# bounds TCP connection establishment. `retries.max_attempts=0` means no
# botocore-level retries are layered on top of this timeout (retries, if any,
# are the caller's responsibility, since a caller such as the
# thread-backed-timeout adapters already has its own bounded-time contract).
#
# IMPORTANT — verified empirically against a real unresponsive endpoint
# (code-review-remediation spec Req 5 follow-up verification): botocore's
# legacy retry mode's `max_attempts` counts *additional retries after the
# first attempt*, not total attempts, despite the boto3 docs reading as if
# it were a total-attempts count. `max_attempts=1` therefore produces TWO
# total connection attempts (observed: two ~10s connect_timeout attempts,
# ~20s total, with the second request's own header showing
# `amz-sdk-request: attempt=2; max=2`) — silently doubling the intended
# worst-case bound. `max_attempts=0` was confirmed (via the same live probe)
# to produce exactly one attempt, bounded by `connect_timeout` alone. Do not
# "fix" this back to 1 without re-verifying against a real stalled endpoint.
_CONNECT_TIMEOUT_SECONDS: float = 10.0
_READ_TIMEOUT_SECONDS: float = 55.0

_CLIENT_CONFIG = Config(
    connect_timeout=_CONNECT_TIMEOUT_SECONDS,
    read_timeout=_READ_TIMEOUT_SECONDS,
    # 0, not 1 — see the verified note above.
    retries={"max_attempts": 0},
)


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class DestinationClientError(Exception):
    """Raised when a destination-account or destination-region client is detected.

    The Solution must never construct a destination-side client (Requirements
    12.2, 13.1).  This exception is raised by
    :meth:`ClientFactory.check_no_destination_client` when a violation is found,
    so the run fails fast before any processing begins.
    """


# ---------------------------------------------------------------------------
# ClientFactory
# ---------------------------------------------------------------------------


class ClientFactory:
    """Creates and caches source-side boto3 clients.

    Clients are cached by ``(service, region)`` so that multiple calls for the
    same service and region return the **same** client object.  This is safe
    because boto3 clients are thread-safe for concurrent API calls and the
    cache itself is guarded by a ``threading.Lock``.

    All three factory methods produce clients scoped to the source account and
    region.  No method accepts ``destination_region`` or
    ``destination_account_id`` parameters — that is the structural guarantee.

    :meth:`check_no_destination_client` is the startup smoke check that makes
    this guarantee machine-verifiable: it inspects every factory method's
    signature for forbidden destination-related parameters, failing fast on the
    first violation found.

    Intended usage::

        factory = ClientFactory()
        factory.check_no_destination_client()   # call once at startup
        s3 = factory.create_s3_client(region="us-east-1")
        athena = factory.create_athena_client(region="us-east-1")
        s3control = factory.create_s3control_client(region="us-east-1")
        # Repeat calls for the same region return the same cached object:
        assert factory.create_s3_client(region="us-east-1") is s3

    Requirements: 12.2, 13.1
    """

    # Parameter names that would indicate a destination-side client.
    # Presence of any of these in a factory method signature is treated as a
    # violation.
    _FORBIDDEN_PARAMS: frozenset[str] = frozenset(
        {"destination_region", "destination_account_id"}
    )

    def __init__(self) -> None:
        # Cache: (service, region) → boto3 client.  Guarded by _lock.
        self._clients: dict[tuple[str, str], Any] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Client constructors — source-side only
    # ------------------------------------------------------------------

    def create_s3_client(self, region: str):
        """Return the cached (or newly created) source-side S3 client.

        Parameters
        ----------
        region:
            AWS region of the *source* bucket.  Must never be a destination
            region (Requirements 12.2, 13.1).

        Returns
        -------
        A boto3 ``s3`` client bound to *region* with the caller's current
        credentials.  The same object is returned on subsequent calls for the
        same *region*.
        """
        return self._get_or_create("s3", region)

    def create_athena_client(self, region: str):
        """Return the cached (or newly created) source-side Athena client.

        Parameters
        ----------
        region:
            AWS region where the S3 Metadata journal (Athena / Iceberg) lives,
            which is always the source region (Requirements 4.1, 12.2, 13.1).

        Returns
        -------
        A boto3 ``athena`` client bound to *region* with the caller's current
        credentials.  The same object is returned on subsequent calls for the
        same *region*.
        """
        return self._get_or_create("athena", region)

    def create_sns_client(self, region: str):
        """Return the cached (or newly created) source-side SNS client.

        Used to publish a Completion_Report to the stack-provisioned
        ``CompletionReportTopic`` (design.md Decision 8). The topic is
        always created in the source account/region — there is no
        region-crossing implication for SNS publish, so this is a
        straightforward same-region client, not a destination-account
        exception (Requirements 4.1, 4.5).

        Parameters
        ----------
        region:
            AWS region of the source account (the same region
            ``CompletionReportTopic`` is provisioned in).

        Returns
        -------
        A boto3 ``sns`` client bound to *region* with the caller's current
        credentials. The same object is returned on subsequent calls for the
        same *region*.
        """
        return self._get_or_create("sns", region)

    def create_s3control_client(self, region: str):
        """Return the cached (or newly created) source-side S3 Control client.

        Used to submit S3 Batch Operations replication jobs in the source
        account (Requirements 7.1, 7.2, 12.2, 13.1).

        Parameters
        ----------
        region:
            AWS region of the source account.

        Returns
        -------
        A boto3 ``s3control`` client bound to *region* with the caller's
        current credentials.  The same object is returned on subsequent calls
        for the same *region*.
        """
        return self._get_or_create("s3control", region)

    # ------------------------------------------------------------------
    # Internal cache
    # ------------------------------------------------------------------

    def _get_or_create(self, service: str, region: str) -> Any:
        """Return a cached client or create and cache a new one.

        Thread-safe: the lock single-flights construction so that two threads
        racing for the same ``(service, region)`` key produce only one client.
        """
        key = (service, region)
        with self._lock:
            client = self._clients.get(key)
            if client is None:
                client = boto3.client(
                    service, region_name=region, config=_CLIENT_CONFIG
                )
                self._clients[key] = client
            return client

    # ------------------------------------------------------------------
    # Startup smoke check
    # ------------------------------------------------------------------

    def check_no_destination_client(self) -> None:
        """Startup guard: raise :exc:`DestinationClientError` if any destination-side
        client is detected in the factory's method signatures.

        Inspects the parameter names of every factory method using
        :mod:`inspect`.  If any parameter is named ``destination_region`` or
        ``destination_account_id``, the factory has been modified to accept
        destination inputs and the guard fails immediately.  This catches
        future accidental additions at the code-structure level, before any
        clients are created.

        Raises
        ------
        DestinationClientError
            On the first violation found.  The message identifies the method
            responsible and cites Requirements 12.2 and 13.1.

        Requirements: 12.2, 13.1
        """
        factory_methods = [
            self.create_s3_client,
            self.create_athena_client,
            self.create_s3control_client,
            self.create_sns_client,
        ]
        for method in factory_methods:
            sig = inspect.signature(method)
            for param_name in sig.parameters:
                if param_name in self._FORBIDDEN_PARAMS:
                    raise DestinationClientError(
                        f"Factory method {method.__name__!r} has forbidden "
                        f"parameter {param_name!r}. "
                        "Destination-side clients must never be constructed "
                        "(Requirements 12.2, 13.1)."
                    )
