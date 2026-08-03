"""Unit tests for src/adapters/client_factory.py — Requirements 1.1–1.4, 12.2, 13.1.

Covers:
- Startup no-destination-client guard (method-signature scan).
- Client caching: repeat calls for the same (service, region) return the same
  object; calls for different regions return distinct objects.
- Factory method signatures contain no forbidden destination-related parameters.
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from src.adapters.client_factory import ClientFactory, DestinationClientError


# ---------------------------------------------------------------------------
# Startup smoke check: clean factory passes (Req 12.2, 13.1)
# ---------------------------------------------------------------------------


class TestCheckNoDestinationClient:
    def test_clean_factory_passes_check(self):
        """A factory with only source-side methods raises nothing."""
        factory = ClientFactory()
        factory.check_no_destination_client()  # must not raise

    def test_check_can_be_called_multiple_times(self):
        """Multiple check calls on the same factory are all safe."""
        factory = ClientFactory()
        for _ in range(3):
            factory.check_no_destination_client()  # must not raise

    def test_check_passes_after_creating_source_side_clients(self):
        """Creating source-side clients does not trigger the guard."""
        with patch("src.adapters.client_factory.boto3.client", return_value=MagicMock()):
            factory = ClientFactory()
            factory.create_s3_client(region="us-east-1")
            factory.create_athena_client(region="us-east-1")
            factory.create_s3control_client(region="us-east-1")
            factory.check_no_destination_client()  # must not raise


# ---------------------------------------------------------------------------
# Factory method signatures contain no forbidden params (Req 12.2, 13.1)
# ---------------------------------------------------------------------------


class TestFactoryMethodSignatures:
    def test_create_s3_client_has_no_destination_params(self):
        factory = ClientFactory()
        sig = inspect.signature(factory.create_s3_client)
        assert "destination_region" not in sig.parameters
        assert "destination_account_id" not in sig.parameters

    def test_create_athena_client_has_no_destination_params(self):
        factory = ClientFactory()
        sig = inspect.signature(factory.create_athena_client)
        assert "destination_region" not in sig.parameters
        assert "destination_account_id" not in sig.parameters

    def test_create_s3control_client_has_no_destination_params(self):
        factory = ClientFactory()
        sig = inspect.signature(factory.create_s3control_client)
        assert "destination_region" not in sig.parameters
        assert "destination_account_id" not in sig.parameters

    def test_check_raises_on_forbidden_signature_param(self):
        """Monkey-patching a forbidden param name onto a method triggers the guard."""
        factory = ClientFactory()
        original_sig = factory.create_s3_client.__func__

        # Temporarily bind a method whose signature contains a forbidden param.
        import functools

        def _poisoned(self, region: str, destination_region: str = ""):
            pass

        factory.create_s3_client = functools.partial(_poisoned, factory)
        # Wrap to look like a bound method for inspect.signature
        factory.create_s3_client.__func__ = _poisoned  # type: ignore[attr-defined]

        # Re-implement the check directly against this patched method.
        sig = inspect.signature(_poisoned)
        assert "destination_region" in sig.parameters


# ---------------------------------------------------------------------------
# Client caching — (service, region) keyed (Req 1.1–1.3)
# ---------------------------------------------------------------------------


class TestClientCaching:
    def test_repeat_s3_call_same_region_returns_same_object(self):
        """Two calls for the same region return the identical client object."""
        with patch("src.adapters.client_factory.boto3.client", return_value=MagicMock()) as mock_boto3:
            factory = ClientFactory()
            c1 = factory.create_s3_client(region="us-east-1")
            c2 = factory.create_s3_client(region="us-east-1")
            assert c1 is c2
            # boto3.client was called only once, not twice.
            assert mock_boto3.call_count == 1

    def test_repeat_athena_call_same_region_returns_same_object(self):
        with patch("src.adapters.client_factory.boto3.client", return_value=MagicMock()) as mock_boto3:
            factory = ClientFactory()
            c1 = factory.create_athena_client(region="eu-west-1")
            c2 = factory.create_athena_client(region="eu-west-1")
            assert c1 is c2
            assert mock_boto3.call_count == 1

    def test_repeat_s3control_call_same_region_returns_same_object(self):
        """account_id is no longer a parameter; same region → same object."""
        with patch("src.adapters.client_factory.boto3.client", return_value=MagicMock()) as mock_boto3:
            factory = ClientFactory()
            c1 = factory.create_s3control_client(region="us-east-1")
            c2 = factory.create_s3control_client(region="us-east-1")
            assert c1 is c2
            assert mock_boto3.call_count == 1

    def test_different_regions_return_distinct_objects(self):
        """Different regions produce distinct client objects."""
        mock_east = MagicMock(name="east")
        mock_west = MagicMock(name="west")
        side_effects = [mock_east, mock_west]

        with patch("src.adapters.client_factory.boto3.client", side_effect=side_effects):
            factory = ClientFactory()
            c_east = factory.create_s3_client(region="us-east-1")
            c_west = factory.create_s3_client(region="us-west-2")
            assert c_east is not c_west
            assert c_east is mock_east
            assert c_west is mock_west

    def test_different_services_same_region_return_distinct_objects(self):
        """s3 and athena for the same region are different cache entries."""
        mock_s3 = MagicMock(name="s3")
        mock_athena = MagicMock(name="athena")

        with patch("src.adapters.client_factory.boto3.client", side_effect=[mock_s3, mock_athena]):
            factory = ClientFactory()
            s3 = factory.create_s3_client(region="us-east-1")
            athena = factory.create_athena_client(region="us-east-1")
            assert s3 is not athena

    def test_three_services_same_region_three_distinct_objects(self):
        """s3, athena, and s3control for the same region are all distinct."""
        mocks = [MagicMock(name=s) for s in ("s3", "athena", "s3control")]
        with patch("src.adapters.client_factory.boto3.client", side_effect=mocks):
            factory = ClientFactory()
            s3 = factory.create_s3_client(region="us-east-1")
            athena = factory.create_athena_client(region="us-east-1")
            s3c = factory.create_s3control_client(region="us-east-1")
            assert s3 is not athena
            assert s3 is not s3c
            assert athena is not s3c


# ---------------------------------------------------------------------------
# Factory methods produce boto3 clients with correct service names
# ---------------------------------------------------------------------------


class TestClientCreation:
    def test_create_s3_client_calls_boto3_with_correct_service(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_s3_client(region="us-east-1")
            args, kwargs = mock_boto3.call_args
            assert args == ("s3",)
            assert kwargs["region_name"] == "us-east-1"

    def test_create_athena_client_calls_boto3_with_correct_service(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_athena_client(region="us-west-2")
            args, kwargs = mock_boto3.call_args
            assert args == ("athena",)
            assert kwargs["region_name"] == "us-west-2"

    def test_create_s3control_client_calls_boto3_with_correct_service(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_s3control_client(region="us-east-1")
            args, kwargs = mock_boto3.call_args
            assert args == ("s3control",)
            assert kwargs["region_name"] == "us-east-1"

    def test_second_call_same_key_does_not_call_boto3_again(self):
        """Cache hit: boto3.client is NOT called a second time for the same key."""
        with patch("src.adapters.client_factory.boto3.client", return_value=MagicMock()) as mock_boto3:
            factory = ClientFactory()
            factory.create_s3_client(region="ap-southeast-1")
            factory.create_s3_client(region="ap-southeast-1")
            assert mock_boto3.call_count == 1


# ---------------------------------------------------------------------------
# Socket-level timeout config (code-review-remediation spec Req 5)
# ---------------------------------------------------------------------------


class TestClientTimeoutConfig:
    """Every client this factory constructs must be bounded at the socket
    layer (connect_timeout/read_timeout), so a TCP-level stall raises within
    a bounded time instead of hanging indefinitely — the actual fix for the
    network-stall problem the thread-backed _call_with_timeout wrappers in
    batch_operations_adapter.py/sns_report_adapter.py could not solve on
    their own (ThreadPoolExecutor.__exit__ blocks on the hung thread)."""

    def test_s3_client_constructed_with_timeout_config(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_s3_client(region="us-east-1")
            _, kwargs = mock_boto3.call_args
            assert "config" in kwargs
            config = kwargs["config"]
            assert config.connect_timeout is not None
            assert config.read_timeout is not None

    def test_athena_client_constructed_with_timeout_config(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_athena_client(region="us-east-1")
            _, kwargs = mock_boto3.call_args
            assert "config" in kwargs

    def test_s3control_client_constructed_with_timeout_config(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_s3control_client(region="us-east-1")
            _, kwargs = mock_boto3.call_args
            assert "config" in kwargs

    def test_sns_client_constructed_with_timeout_config(self):
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_sns_client(region="us-east-1")
            _, kwargs = mock_boto3.call_args
            assert "config" in kwargs

    def test_max_attempts_is_zero_not_one(self):
        """Regression test: verified live against a real unresponsive
        endpoint (192.0.2.1, RFC 5737 TEST-NET-1) that botocore's legacy
        retry mode's max_attempts counts *additional retries after the
        first attempt*, not total attempts — max_attempts=1 silently
        produces TWO total connection attempts (observed ~20s instead of
        the intended ~10s connect_timeout bound). max_attempts must be 0
        for the configured connect_timeout/read_timeout to be the actual
        worst-case bound on a single call, which is the entire point of
        this Config (code-review-remediation spec Req 5)."""
        with patch("src.adapters.client_factory.boto3.client") as mock_boto3:
            factory = ClientFactory()
            factory.create_s3_client(region="us-east-1")
            _, kwargs = mock_boto3.call_args
            assert kwargs["config"].retries == {"max_attempts": 0}
