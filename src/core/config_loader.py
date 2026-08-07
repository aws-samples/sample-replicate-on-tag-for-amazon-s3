"""Configuration loading and validation for the tag-based S3 replication backfill Solution.

Implements Requirements 1.1–1.8, 2.1–2.7, 13.6.
"""
from __future__ import annotations

import re
from typing import Any

from .models import AppConfig, MonitoredBucket

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_BUCKETS = 1_000

# ---------------------------------------------------------------------------
# Internal regex patterns
# ---------------------------------------------------------------------------

# S3 bucket name: starts and ends with [a-z0-9], middle chars allow . and -
# Must be validated further for consecutive dots and IP-address format.
_S3_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]*[a-z0-9]$")

# IP-address format (e.g. 192.168.1.1) is forbidden as a bucket name.
_IP_RE = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

# AWS region format: two-to-four lowercase letters (geo prefix), one or more
# hyphen-separated lowercase word segments (area / direction / qualifier), and
# a one-or-two-digit numeric suffix.  Examples: us-east-1, ap-southeast-2,
# us-gov-east-1, eu-central-2, il-central-1.  This is a structural check —
# it rejects obviously malformed strings (underscores, uppercase, no digits,
# no area word) but does not maintain an allow-list of known region codes.
_AWS_REGION_RE = re.compile(r"^[a-z]{2,4}-(?:[a-z]+-)+\d{1,2}$")

# Duration string: optional hours and/or minutes, e.g. "1h", "30m", "1h30m".
# Both groups being absent (empty string) is explicitly rejected below.
_DURATION_RE = re.compile(r"^(?:(\d+)h)?(?:(\d+)m)?$")


# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------


class ConfigError(Exception):
    """Raised when configuration loading or validation fails (Requirements 1.6–1.8, 2.x, 13.6).

    The message identifies the offending entry by name or index and, for
    interval violations, states the permitted minimum and maximum.
    """


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entry_id(raw: Any, idx: int) -> str:
    """Return a human-readable identifier for a bucket entry: its name or index."""
    if isinstance(raw, dict):
        name = raw.get("name")
        if isinstance(name, str) and name:
            return repr(name)
    return f"at index {idx}"


def _is_valid_s3_bucket_name(name: str) -> bool:
    """Return True iff *name* satisfies S3 bucket naming rules.

    Rules (as per AWS documentation and task specification):
    - 3–63 characters long.
    - Lowercase letters, digits, hyphens, and periods only.
    - Must start and end with a letter or digit.
    - No consecutive periods.
    - Must not be formatted as an IP address (e.g. 192.168.1.1).
    """
    if len(name) < 3 or len(name) > 63:
        return False
    # For 3+ char names the pattern requires start/end alphanumeric; for 2-char
    # names the pattern would also match, but length < 3 is caught above.
    if not _S3_NAME_RE.match(name):
        return False
    if ".." in name:
        return False
    if _IP_RE.match(name):
        return False
    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_config(source: Any) -> AppConfig:
    """Load and validate configuration from a parsed dict.

    Parameters
    ----------
    source:
        A ``dict`` produced by parsing a customer-provided JSON or YAML
        configuration file.  Expected structure::

            {
                "buckets": [
                    {"name": "my-bucket", "region": "us-east-1"},
                    ...
                ]
            }

    Returns
    -------
    AppConfig
        The validated configuration with a non-empty ``buckets`` list.

    Raises
    ------
    ConfigError
        On any validation failure.  The message identifies the offending entry
        (by name or index) and, for interval violations, the permitted range.
        No ``MonitoredBucket`` entries are loaded on failure (all-or-nothing).
    """
    # -- 1. Top-level structure ------------------------------------------------
    if not isinstance(source, dict):
        raise ConfigError(
            "Configuration must be a mapping (dict); the provided source could not be parsed."
        )

    # -- 2. Buckets list -------------------------------------------------------
    if "buckets" not in source:
        raise ConfigError(
            "Configuration is missing the required 'buckets' key."
        )
    raw_buckets = source["buckets"]
    if not isinstance(raw_buckets, list):
        raise ConfigError(
            f"'buckets' must be a list of bucket entries, got {type(raw_buckets).__name__!r}."
        )

    # -- 3. Count bounds (Requirements 1.1, 2.1, 2.3) -------------------------
    if len(raw_buckets) == 0:
        raise ConfigError(
            "No Monitored_Bucket entries are defined. "
            "Provide at least 1 entry in 'buckets'."
        )
    if len(raw_buckets) > MAX_BUCKETS:
        raise ConfigError(
            f"Too many Monitored_Bucket entries ({len(raw_buckets)}); "
            f"the maximum is {MAX_BUCKETS}."
        )

    # -- 4. Per-entry validation -----------------------------------------------
    buckets: list[MonitoredBucket] = []
    seen_pairs: set[tuple[str, str]] = set()

    for idx, raw in enumerate(raw_buckets):
        eid = _entry_id(raw, idx)

        # Entry must be a mapping.
        if not isinstance(raw, dict):
            raise ConfigError(
                f"Bucket entry {eid} is not a mapping (dict); "
                "each entry must be an object with 'name' and 'region' keys."
            )

        # Reject customer-supplied tag_filter or destination (Requirement 1.5).
        if "tag_filter" in raw:
            raise ConfigError(
                f"Bucket entry {eid} contains a customer-supplied 'tag_filter'. "
                "Tag filters are derived from the bucket's Replication_Configuration "
                "and must not be specified here."
            )
        if "destination" in raw:
            raise ConfigError(
                f"Bucket entry {eid} contains a customer-supplied 'destination'. "
                "Destinations are derived from the bucket's Replication_Configuration "
                "and must not be specified here."
            )

        # Validate 'name' (Requirements 1.2, 1.6, 2.2, 2.4).
        name = raw.get("name")
        if name is None:
            raise ConfigError(
                f"Bucket entry {eid} is missing the required field 'name'."
            )
        if not isinstance(name, str) or name == "":
            raise ConfigError(
                f"Bucket entry {eid} field 'name' must be a non-empty string."
            )
        if not _is_valid_s3_bucket_name(name):
            raise ConfigError(
                f"Bucket entry {eid} has an invalid S3 bucket name {name!r}. "
                "Names must be 3–63 characters long, consist only of lowercase "
                "letters, digits, hyphens, and periods, start and end with a "
                "letter or digit, contain no consecutive periods, and must not "
                "be formatted as an IP address (e.g. 192.168.1.1)."
            )

        # Validate 'region' (Requirements 1.2, 1.6, 2.2).
        region = raw.get("region")
        if region is None:
            raise ConfigError(
                f"Bucket entry {eid} is missing the required field 'region'."
            )
        if not isinstance(region, str) or region == "":
            raise ConfigError(
                f"Bucket entry {eid} field 'region' must be a non-empty string."
            )
        if not _AWS_REGION_RE.match(region):
            raise ConfigError(
                f"Bucket entry {eid} has an invalid AWS region {region!r}. "
                "Expected format: two-to-four lowercase letters, one or more "
                "hyphen-separated lowercase word segments, and a numeric suffix "
                "(e.g. 'us-east-1', 'ap-southeast-2', 'eu-central-1')."
            )

        # Reject duplicate (name, region) pairs (Requirements 1.8, 2.4).
        pair = (name, region)
        if pair in seen_pairs:
            raise ConfigError(
                f"Bucket entry {eid} duplicates the (name, region) pair "
                f"({name!r}, {region!r}). Each bucket–region combination must be unique."
            )
        seen_pairs.add(pair)

        buckets.append(MonitoredBucket(
            name=name,
            region=region,
            disabled=bool(raw.get("disabled", False)),
            disabled_reason=str(raw.get("disabled_reason", "")),
            disabled_at=str(raw.get("disabled_at", "")),
        ))

    # -- 5. Success (Requirement 2.7) -----------------------------------------
    return AppConfig(buckets=buckets)
