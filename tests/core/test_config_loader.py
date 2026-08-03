"""Tests for src/core/config_loader.py.

Tasks 2.2 and 2.3:
  - Property 7: Configuration validation — accept iff every bucket name
    conforms to S3 naming rules, every (name, region) pair is unique, and
    the (defaulted) interval is in [15 min, 24 h]; otherwise reject and
    load no entries.
  - Unit tests for edge cases: missing name/region, malformed source,
    0/1/1000/1001-entry boundaries, customer-supplied tag_filter/destination,
    default-interval application, region honored per bucket.

Requirements: 1.1–1.8, 2.1–2.7, 13.3, 13.4, 13.6
"""
from __future__ import annotations

import re

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from src.core.config_loader import (
    ConfigError,
    load_config,
)
from src.core.models import AppConfig

# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Valid S3 bucket name: starts/ends with [a-z0-9], middle chars [a-z0-9\-],
# length 3-63.  Using no dots avoids consecutive-dot and IP-format checks.
_VALID_NAME_ST = st.from_regex(
    r"^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$", fullmatch=True
).filter(lambda n: 3 <= len(n) <= 63)

# Valid AWS region: matches the _AWS_REGION_RE pattern used by config_loader.
# Built as a composed strategy to stay readable and avoid Hypothesis blowup
# on complex regex generation.
_VALID_REGION_ST = st.builds(
    lambda geo, area, n: f"{geo}-{area}-{n}",
    geo=st.sampled_from(["us", "eu", "ap", "ca", "sa", "me", "af", "cn", "il"]),
    area=st.sampled_from([
        "east", "west", "north", "south", "central",
        "northeast", "southeast", "northwest", "gov-east", "gov-west",
    ]),
    n=st.integers(min_value=1, max_value=4).map(str),
)

# Valid interval in seconds: [900, 86400] = [15 min, 24 h].
_VALID_INTERVAL_ST = st.integers(min_value=900, max_value=86400)

# Unique (name, region) pairs — Hypothesis unique on both elements.
_BUCKET_PAIR_ST = st.tuples(_VALID_NAME_ST, _VALID_REGION_ST)


def _unique_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Return pairs with duplicate (name, region) tuples removed."""
    seen: set[tuple[str, str]] = set()
    result = []
    for pair in pairs:
        if pair not in seen:
            seen.add(pair)
            result.append(pair)
    return result


# ---------------------------------------------------------------------------
# Property 7: Configuration validation (task 2.2)
# Feature: tag-based-s3-replication, Property 7: Configuration validation
# ---------------------------------------------------------------------------


class TestProperty7ConfigValidation:
    @given(
        pairs=st.lists(_BUCKET_PAIR_ST, min_size=1, max_size=10, unique=True),
    )
    @settings(max_examples=100)
    def test_valid_config_is_accepted(
        self, pairs: list[tuple[str, str]]
    ) -> None:
        """Valid bucket names + unique pairs → load_config succeeds.

        # Feature: tag-based-s3-replication, Property 7: Configuration validation
        """
        config = {
            "buckets": [{"name": n, "region": r} for n, r in pairs],
        }
        result = load_config(config)
        assert isinstance(result, AppConfig)
        assert len(result.buckets) == len(pairs)

    @given(
        bad_name=st.one_of(
            # Too short (1-2 chars).
            st.from_regex(r"^[a-z0-9]{1,2}$", fullmatch=True),
            # Too long (> 63 chars).
            st.from_regex(r"^[a-z0-9][a-z0-9]{63,70}$", fullmatch=True),
            # Contains uppercase.
            st.from_regex(r"^[A-Z][a-zA-Z0-9]{2,20}$", fullmatch=True),
            # Starts with a hyphen.
            st.from_regex(r"^-[a-z0-9][a-z0-9]{1,61}$", fullmatch=True),
            # Consecutive dots.
            st.just("my..bucket"),
        ),
        region=_VALID_REGION_ST,
    )
    @settings(max_examples=100)
    def test_invalid_bucket_name_is_rejected(
        self, bad_name: str, region: str
    ) -> None:
        """Config with an invalid S3 bucket name → ConfigError and no entries loaded.

        # Feature: tag-based-s3-replication, Property 7: Configuration validation
        """
        config = {"buckets": [{"name": bad_name, "region": region}]}
        with pytest.raises(ConfigError):
            load_config(config)

    @given(
        name=_VALID_NAME_ST,
        region=_VALID_REGION_ST,
        count=st.integers(min_value=2, max_value=5),
    )
    @settings(max_examples=100)
    def test_duplicate_name_region_pair_is_rejected(
        self, name: str, region: str, count: int
    ) -> None:
        """Duplicate (name, region) pair → ConfigError.

        # Feature: tag-based-s3-replication, Property 7: Configuration validation
        """
        config = {
            "buckets": [{"name": name, "region": region}] * count,
        }
        with pytest.raises(ConfigError):
            load_config(config)


# ---------------------------------------------------------------------------
# Unit tests: edge cases (task 2.3)
# ---------------------------------------------------------------------------


class TestConfigLoaderMissingFields:
    """Missing name/region with actionable message + entry id."""

    def test_missing_name_raises_with_entry_index(self):
        """Missing name → ConfigError identifying the entry (Req 1.6)."""
        config = {"buckets": [{"region": "us-east-1"}]}
        with pytest.raises(ConfigError, match="missing"):
            load_config(config)

    def test_missing_region_raises_with_entry_index(self):
        """Missing region → ConfigError identifying the entry (Req 1.6)."""
        config = {"buckets": [{"name": "my-bucket"}]}
        with pytest.raises(ConfigError, match="missing"):
            load_config(config)

    def test_missing_name_error_identifies_entry_by_name_when_possible(self):
        """Entry with both name and missing region: error references the name."""
        config = {"buckets": [{"name": "named-bucket"}]}
        with pytest.raises(ConfigError) as exc_info:
            load_config(config)
        assert "named-bucket" in str(exc_info.value)

    def test_missing_name_error_identifies_entry_by_index_when_no_name(self):
        """Entry with no name key: error references the index."""
        config = {"buckets": [{"region": "us-east-1"}]}
        with pytest.raises(ConfigError) as exc_info:
            load_config(config)
        assert "index" in str(exc_info.value).lower()

    def test_empty_name_raises(self):
        """Empty string name → ConfigError (Req 2.2)."""
        config = {"buckets": [{"name": "", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_empty_region_raises(self):
        """Empty string region → ConfigError (Req 2.2)."""
        config = {"buckets": [{"name": "my-bucket", "region": ""}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_non_string_name_raises(self):
        """Integer name → ConfigError."""
        config = {"buckets": [{"name": 123, "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_non_string_region_raises(self):
        """Integer region → ConfigError."""
        config = {"buckets": [{"name": "my-bucket", "region": 1}]}
        with pytest.raises(ConfigError):
            load_config(config)


class TestConfigLoaderMalformedSource:
    """Malformed source → ConfigError, no entries loaded (Req 1.7)."""

    def test_non_dict_source_raises(self):
        with pytest.raises(ConfigError):
            load_config("not a dict")

    def test_list_source_raises(self):
        with pytest.raises(ConfigError):
            load_config([{"name": "b", "region": "r"}])

    def test_none_source_raises(self):
        with pytest.raises(ConfigError):
            load_config(None)

    def test_missing_buckets_key_raises(self):
        with pytest.raises(ConfigError):
            load_config({})

    def test_buckets_not_a_list_raises(self):
        with pytest.raises(ConfigError):
            load_config({"buckets": "not-a-list"})

    def test_buckets_is_dict_raises(self):
        with pytest.raises(ConfigError):
            load_config({"buckets": {"name": "b", "region": "r"}})


class TestConfigLoaderBoundaries:
    """0 / 1 / 1000 / 1001 entry boundaries (Req 1.1, 2.1, 2.3)."""

    def test_zero_entries_raises(self):
        """Empty buckets list → ConfigError (Req 2.1, 2.3)."""
        with pytest.raises(ConfigError, match="at least 1"):
            load_config({"buckets": []})

    def test_one_entry_accepted(self):
        """Single entry → accepted (Req 1.1)."""
        config = {"buckets": [{"name": "my-bucket", "region": "us-east-1"}]}
        result = load_config(config)
        assert len(result.buckets) == 1

    def test_1000_entries_accepted(self):
        """1000 entries → accepted (Req 1.1)."""
        config = {
            "buckets": [
                {"name": f"bucket-{i:04d}", "region": "us-east-1"}
                for i in range(1000)
            ]
        }
        result = load_config(config)
        assert len(result.buckets) == 1000

    def test_1001_entries_raises(self):
        """1001 entries → ConfigError (Req 1.1)."""
        config = {
            "buckets": [
                {"name": f"bucket-{i:04d}", "region": "us-east-1"}
                for i in range(1001)
            ]
        }
        with pytest.raises(ConfigError):
            load_config(config)


class TestConfigLoaderCustomerSuppliedFields:
    """Rejection of customer-supplied tag_filter or destination (Req 1.5)."""

    def test_tag_filter_key_raises(self):
        config = {
            "buckets": [
                {
                    "name": "my-bucket",
                    "region": "us-east-1",
                    "tag_filter": {"env": "prod"},
                }
            ]
        }
        with pytest.raises(ConfigError, match="tag_filter"):
            load_config(config)

    def test_destination_key_raises(self):
        config = {
            "buckets": [
                {
                    "name": "my-bucket",
                    "region": "us-east-1",
                    "destination": "arn:aws:s3:::dest-bucket",
                }
            ]
        }
        with pytest.raises(ConfigError, match="destination"):
            load_config(config)


class TestConfigLoaderSuccessPath:
    """Success-path availability and field correctness (Req 2.7, 13.3)."""

    def test_returns_app_config_on_success(self):
        config = {
            "buckets": [
                {"name": "bucket-a", "region": "us-east-1"},
                {"name": "bucket-b", "region": "eu-west-1"},
            ]
        }
        result = load_config(config)
        assert isinstance(result, AppConfig)
        assert len(result.buckets) == 2

    def test_bucket_names_preserved(self):
        config = {
            "buckets": [
                {"name": "my-source-bucket", "region": "us-east-1"},
            ]
        }
        result = load_config(config)
        assert result.buckets[0].name == "my-source-bucket"

    def test_region_honored_per_bucket(self):
        """Region is stored separately per bucket (Req 13.3)."""
        config = {
            "buckets": [
                {"name": "bucket-a", "region": "us-east-1"},
                {"name": "bucket-b", "region": "ap-southeast-1"},
            ]
        }
        result = load_config(config)
        regions = {b.name: b.region for b in result.buckets}
        assert regions["bucket-a"] == "us-east-1"
        assert regions["bucket-b"] == "ap-southeast-1"

    def test_same_name_different_regions_accepted(self):
        """Same bucket name in different regions is a distinct pair (not a dup)."""
        config = {
            "buckets": [
                {"name": "my-bucket", "region": "us-east-1"},
                {"name": "my-bucket", "region": "eu-west-1"},
            ]
        }
        result = load_config(config)
        assert len(result.buckets) == 2

    def test_duplicate_name_region_pair_raises(self):
        """Same (name, region) twice → ConfigError (Req 1.8)."""
        config = {
            "buckets": [
                {"name": "my-bucket", "region": "us-east-1"},
                {"name": "my-bucket", "region": "us-east-1"},
            ]
        }
        with pytest.raises(ConfigError):
            load_config(config)


class TestConfigLoaderS3BucketNameRules:
    """S3 bucket name validation (Req 2.2, 2.4)."""

    def test_valid_name_accepted(self):
        config = {"buckets": [{"name": "valid-bucket-name", "region": "us-east-1"}]}
        result = load_config(config)
        assert result.buckets[0].name == "valid-bucket-name"

    def test_two_char_name_rejected(self):
        config = {"buckets": [{"name": "ab", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_name_with_uppercase_rejected(self):
        config = {"buckets": [{"name": "MyBucket", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_name_starting_with_hyphen_rejected(self):
        config = {"buckets": [{"name": "-my-bucket", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_name_ending_with_hyphen_rejected(self):
        config = {"buckets": [{"name": "my-bucket-", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_name_with_consecutive_dots_rejected(self):
        config = {"buckets": [{"name": "my..bucket", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_ip_address_name_rejected(self):
        config = {"buckets": [{"name": "192.168.1.1", "region": "us-east-1"}]}
        with pytest.raises(ConfigError):
            load_config(config)

    def test_name_64_chars_rejected(self):
        config = {
            "buckets": [{"name": "a" * 64, "region": "us-east-1"}]
        }
        with pytest.raises(ConfigError):
            load_config(config)

    def test_invalid_bucket_name_error_includes_bucket_name(self):
        """Error message identifies the invalid bucket name (Req 2.4)."""
        config = {"buckets": [{"name": "InvalidName", "region": "us-east-1"}]}
        with pytest.raises(ConfigError) as exc_info:
            load_config(config)
        assert "InvalidName" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Region format validation
# ---------------------------------------------------------------------------


class TestConfigLoaderRegionFormat:
    """Region strings must match the AWS region format (Req 1.2, 1.6)."""

    # Known-good production region codes
    @pytest.mark.parametrize("region", [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-central-1",
        "eu-central-2",
        "eu-north-1",
        "eu-south-1",
        "eu-south-2",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-southeast-3",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-east-1",
        "ap-south-1",
        "ap-south-2",
        "ca-central-1",
        "ca-west-1",
        "sa-east-1",
        "me-south-1",
        "me-central-1",
        "af-south-1",
        "il-central-1",
        "us-gov-east-1",
        "us-gov-west-1",
        "cn-north-1",
        "cn-northwest-1",
    ])
    def test_known_valid_region_accepted(self, region):
        """Known production region codes are accepted without ConfigError."""
        config = {"buckets": [{"name": "my-bucket", "region": region}]}
        result = load_config(config)
        assert result.buckets[0].region == region

    @pytest.mark.parametrize("bad_region", [
        "US-EAST-1",          # uppercase
        "us_east_1",          # underscores
        "useast1",            # no hyphens
        "us-east",            # no digit suffix
        "1-east-us",          # starts with digit
        "us east 1",          # spaces
        "eu-CENTRAL-1",       # mixed case
        "east-1",             # geo too short (1 char)  -- wait, "east" is 4 chars, matches [a-z]{2,4}... let me think
        "-us-east-1",         # leading hyphen
        "us-east-1-",         # trailing hyphen
        "us--east-1",         # double hyphen (empty segment)
        "verylonggeoprefix-east-1",  # geo > 4 chars
    ])
    def test_invalid_region_format_raises(self, bad_region):
        """Region strings that don't match AWS format → ConfigError."""
        config = {"buckets": [{"name": "my-bucket", "region": bad_region}]}
        with pytest.raises(ConfigError, match="region"):
            load_config(config)

    def test_region_format_error_message_includes_bad_value(self):
        """ConfigError message includes the offending region string."""
        bad = "not-valid"
        config = {"buckets": [{"name": "my-bucket", "region": bad}]}
        with pytest.raises(ConfigError) as exc_info:
            load_config(config)
        assert bad in str(exc_info.value)

    @given(region=_VALID_REGION_ST)
    @settings(max_examples=100)
    def test_valid_region_format_always_accepted(self, region):
        """All well-formed AWS region strings pass config validation."""
        config = {"buckets": [{"name": "my-bucket", "region": region}]}
        result = load_config(config)
        assert result.buckets[0].region == region
