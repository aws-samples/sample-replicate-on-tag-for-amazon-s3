"""Config-writing custom resource handler (deploy/template.yaml ConfigResourceFunction).

Source of truth for the inline Lambda Code.ZipFile; tests/test_template.py
asserts they match and that this file is < 4096 bytes.

CREATE/UPDATE: writes Solution_Config JSON to State_Bucket and seeds a
checkpoint for each new source bucket. DELETE: removes config only.
"""
import json
import os
from datetime import datetime, timezone

import boto3
import cfnresponse  # auto-provided for inline ZipFile Lambdas; do not bundle

s3 = boto3.client("s3")


def _interval_from_minutes(minutes):
    """Derive the processing_interval duration string from a check frequency
    expressed in minutes (e.g. 60 -> "1h", 30 -> "30m")."""
    n = int(minutes)
    if n % 60 == 0:
        return f"{n // 60}h"
    return f"{n}m"


def _seed_checkpoints(bucket, names, kms_key_arn):
    """Write an initial checkpoint for each bucket, skipping if one exists."""
    wm = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for name in names:
        body = json.dumps({
            "source_bucket": name,
            "last_processed_watermark": wm,
            "lease": None,
            "processed_window": [],
        })
        kw = {
            "Bucket": bucket,
            "Key": f"state/{name}.json",
            "Body": body.encode("utf-8"),
            "ContentType": "application/json",
            "IfNoneMatch": "*",
        }
        if kms_key_arn:
            kw["ServerSideEncryption"] = "aws:kms"
            kw["SSEKMSKeyId"] = kms_key_arn
        try:
            s3.put_object(**kw)
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in (
                "PreconditionFailed", "ConditionalRequestConflict",
            ):
                raise


def handler(event, context):
    # Every value written comes from the environment, not the event; see template.
    bucket = os.environ["STATE_BUCKET"]
    key = os.environ["CONFIG_KEY"]
    minutes = os.environ["CHECK_FREQUENCY_MINUTES"]
    try:
        if event.get("StackId") != os.environ["STACK_ID"]:
            cfnresponse.send(event, context, cfnresponse.FAILED,
                             {"Error": "Request rejected"})
            return
        if event["RequestType"] == "Delete":
            s3.delete_object(Bucket=bucket, Key=key)  # idempotent: 204 even if absent
        else:  # Create or Update
            region = os.environ["REGION"]
            names = [n for n in os.environ["SOURCE_BUCKET_NAMES"].split(",") if n]
            config = {
                "buckets": [{"name": n, "region": region} for n in names],
                "processing_interval": _interval_from_minutes(minutes),
            }
            put_kwargs = {
                "Bucket": bucket,
                "Key": key,
                "Body": json.dumps(config).encode("utf-8"),
                "ContentType": "application/json",
            }
            kms_key_arn = os.environ.get("KMS_KEY_ARN", "").strip()
            if kms_key_arn:
                put_kwargs["ServerSideEncryption"] = "aws:kms"
                put_kwargs["SSEKMSKeyId"] = kms_key_arn
            s3.put_object(**put_kwargs)
            # Seed newly added buckets only; every seed write is conditional.
            if event["RequestType"] == "Create":
                to_seed = names
            else:
                old = set(event.get("OldResourceProperties", {}).get("Buckets", []))
                to_seed = [n for n in names if n not in old]
            if to_seed:
                _seed_checkpoints(bucket, to_seed, kms_key_arn)
        freq_seconds = str(int(minutes) * 60)
        cfnresponse.send(
            event, context, cfnresponse.SUCCESS,
            {"CheckFrequencySeconds": freq_seconds},
            physicalResourceId=key,
        )
    except Exception as exc:  # signal failure so the stack fails fast (Req 7.6)
        cfnresponse.send(event, context, cfnresponse.FAILED, {"Error": str(exc)})
        raise
