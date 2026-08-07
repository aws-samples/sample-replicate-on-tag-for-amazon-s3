#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./deploy/build-package.sh --build-only <out.zip>        # CI mode: local zip, no S3 upload
#   ./deploy/build-package.sh <code-bucket> <code-package-key>  # upload mode (original)
#
# Stages the src/ tree into a zip archive and either saves it locally (--build-only)
# or uploads it to the specified S3 location (upload mode).
#
# The package contains ONLY src/ — no third-party dependencies are bundled
# because boto3 is provided by the Lambda python3.12 runtime.
#
# Content-hashed S3 key (upload mode only): the given <code-package-key> is
# treated as a base name, and the zip's SHA-256 (first 12 hex chars) is
# inserted before the extension, e.g. code-packages/s3-replicate-on-tag.zip
# -> code-packages/s3-replicate-on-tag-<hash>.zip. This guarantees the
# uploaded S3 key changes whenever the code content changes.
#
# Why this matters: deploy/template.yaml's ReplicationLambda.Code.S3Key is a
# !GetAtt of a custom resource that parses the CodeLocation parameter.
# CloudFormation only calls UpdateFunctionCode when a resource PROPERTY
# changes. If the same S3 key is reused across builds (uploading new bytes
# to the old key), the S3Key GetAtt output is identical to the previous
# deploy, CloudFormation sees no property diff on Code, and the Lambda's
# running code is silently NOT refreshed — even though `aws s3 cp`
# succeeded and the stack update reports UPDATE_COMPLETE. Hashing the key
# forces CodeLocation (and therefore Code.S3Key) to change on every content
# change, so a stack update always redeploys the code that was just built.
#
# The resulting CodeLocation is printed on the last line of output so the
# caller can pass it directly as the CodeLocation parameter.

if [[ "${1:-}" == "--build-only" ]]; then
  if [[ $# -lt 2 ]]; then
    echo "Usage: $0 --build-only <out.zip>" >&2
    exit 1
  fi
  OUT_ZIP="$2"
  UPLOAD_MODE=false
elif [[ $# -lt 2 ]]; then
  echo "Usage: $0 <code-bucket> <code-package-key>" >&2
  echo "   or: $0 --build-only <out.zip>" >&2
  exit 1
else
  CODE_BUCKET="$1"
  CODE_PACKAGE_KEY_BASE="$2"
  UPLOAD_MODE=true
fi

PROJ_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$PROJ_ROOT/tmp/deploy/build"
mkdir -p "$TMP_DIR"

rm -rf "$TMP_DIR/staging"  # clean previous build
mkdir -p "$TMP_DIR/staging"

cp -r "$PROJ_ROOT/src" "$TMP_DIR/staging/src"  # copy source tree to archive root

ZIP_FILE="$TMP_DIR/code-package.zip"
rm -f "$ZIP_FILE"

(cd "$TMP_DIR/staging" && zip -r "$ZIP_FILE" src/)  # zip with src/ at root

if [[ "$UPLOAD_MODE" == false ]]; then
  cp "$ZIP_FILE" "$OUT_ZIP"  # copy to caller-specified path, no S3 upload
  echo "Built code package at $OUT_ZIP"
else
  # Insert the zip's content hash before the extension so the S3 key
  # changes whenever the code changes (see header comment for why).
  HASH="$(shasum -a 256 "$ZIP_FILE" | cut -c1-12)"
  if [[ "$CODE_PACKAGE_KEY_BASE" == *.* ]]; then
    CODE_PACKAGE_KEY="${CODE_PACKAGE_KEY_BASE%.*}-${HASH}.${CODE_PACKAGE_KEY_BASE##*.}"
  else
    CODE_PACKAGE_KEY="${CODE_PACKAGE_KEY_BASE}-${HASH}"
  fi

  aws s3 cp "$ZIP_FILE" "s3://${CODE_BUCKET}/${CODE_PACKAGE_KEY}" --region us-west-2 --no-cli-pager  # upload to Code_Bucket
  echo "Uploaded code package to s3://${CODE_BUCKET}/${CODE_PACKAGE_KEY}"
  echo "CodeLocation: s3://${CODE_BUCKET}/${CODE_PACKAGE_KEY}"
fi
