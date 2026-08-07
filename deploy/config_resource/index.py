"""Config-writing custom resource handler (deploy/template.yaml ConfigResourceFunction).

Source of truth for the inline Lambda Code.ZipFile; tests/test_template.py
asserts they match and that this file is < 4096 bytes.

CREATE/UPDATE: writes Solution_Config JSON to State_Bucket. DELETE: removes it.
"""
import json

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


def handler(event, context):
    props = event.get("ResourceProperties", {})
    bucket = props["StateBucket"]
    key = props["ConfigKey"]
    try:
        if event["RequestType"] == "Delete":
            s3.delete_object(Bucket=bucket, Key=key)  # idempotent: 204 even if absent
        else:  # Create or Update
            region = props["Region"]
            names = props["Buckets"]  # list from CommaDelimitedList parameter
            # Omits any runtime "disabled" flag (see lambda_handler.py).
            config = {
                "buckets": [{"name": n, "region": region} for n in names],
                "processing_interval": _interval_from_minutes(props["CheckFrequencyMinutes"]),
            }
            put_kwargs = {
                "Bucket": bucket,
                "Key": key,
                "Body": json.dumps(config).encode("utf-8"),
                "ContentType": "application/json",
            }
            kms_key_arn = props.get("KmsKeyArn", "").strip()
            if kms_key_arn:
                put_kwargs["ServerSideEncryption"] = "aws:kms"
                put_kwargs["SSEKMSKeyId"] = kms_key_arn
            s3.put_object(**put_kwargs)
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {}, physicalResourceId=key)
    except Exception as exc:  # signal failure so the stack fails fast (Req 7.6)
        cfnresponse.send(event, context, cfnresponse.FAILED, {"Error": str(exc)})
        raise
