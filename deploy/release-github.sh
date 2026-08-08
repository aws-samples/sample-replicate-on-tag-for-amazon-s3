#!/usr/bin/env bash
set -euo pipefail
# Release the current source tree to the public GitHub mirror.
#
# Usage:
#   ./deploy/release-github.sh <version>
#
# Example:
#   ./deploy/release-github.sh 0.4.0
#
# What it does:
#   1. Syncs source files to the local GitHub clone (respects .github-mirror-ignore).
#   2. Replaces CHANGELOG.md with the public version from this repo.
#   3. Commits, tags, and pushes to GitHub.
#   4. Builds package-<version>.zip, generates package-<version>.zip.sha256.
#   5. Creates a GitHub Release with template.yaml, package-<version>.zip, and
#      package-<version>.zip.sha256.
#   6. Warns if the Release is a draft (which a prior tag deletion causes).
#
# Prerequisites:
#   - gh CLI authenticated with push access to the target repo.
#   - The GitHub clone exists at $GITHUB_REPO_PATH (see below).
#   - The version has a ## [x.y.z] entry in CHANGELOG.md here.

VERSION="${1:?Usage: $0 <version>}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GITLAB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GITHUB_REPO_PATH="${GITHUB_REPO_PATH:-$HOME/Documents/GitHub/sample-replicate-on-tag-for-amazon-s3}"
GITHUB_REPO="aws-samples/sample-replicate-on-tag-for-amazon-s3"

if [[ ! -d "$GITHUB_REPO_PATH/.git" ]]; then
  echo "ERROR: GitHub clone not found at $GITHUB_REPO_PATH" >&2
  echo "  Set GITHUB_REPO_PATH to override." >&2
  exit 1
fi

# Verify version appears in CHANGELOG.md
if ! grep -q "^## \[$VERSION\]" "$GITLAB_ROOT/CHANGELOG.md"; then
  echo "ERROR: No '## [$VERSION]' entry found in CHANGELOG.md" >&2
  exit 1
fi

echo "=== Syncing source to GitHub clone ==="
rsync -av --delete \
  --exclude='.git' \
  --exclude='__pycache__' \
  --exclude='*.egg-info' \
  --exclude='.DS_Store' \
  --exclude='benchmarks/' \
  --exclude='.kiro/' \
  --exclude='.holmes/' \
  --exclude='.hypothesis/' \
  --exclude='.pytest_cache/' \
  --exclude='.ruff_cache/' \
  --exclude='.threatmodel/' \
  --exclude='build/' \
  --exclude='tmp/' \
  --exclude='.gitlab-ci.yml' \
  --exclude='.github-mirror-ignore' \
  --exclude='s3-replicate-on-tag.code-workspace' \
  --exclude='CODE_OF_CONDUCT.md' \
  --exclude='CONTRIBUTING.md' \
  --exclude='LICENSE' \
  "$GITLAB_ROOT/" \
  "$GITHUB_REPO_PATH/"

echo "=== Writing public CHANGELOG.md ==="
cp "$GITLAB_ROOT/CHANGELOG.md" "$GITHUB_REPO_PATH/CHANGELOG.md"

echo "=== Committing ==="
cd "$GITHUB_REPO_PATH"
git add -A
if git diff --cached --quiet; then
  echo "No changes to commit (already up to date)."
else
  git commit -m "Mirror source from internal repo at v$VERSION"
fi

echo "=== Tagging v$VERSION ==="
if git tag -l "v$VERSION" | grep -q "v$VERSION"; then
  echo "Tag v$VERSION already exists, skipping."
else
  git tag -a "v$VERSION" -m "$VERSION"
fi

echo "=== Pushing to GitHub ==="
git push origin main --tags

# The asset carries the version in its filename, so that an operator
# upgrading between releases naturally uploads it to a NEW S3 key and
# therefore supplies a NEW CodeLocation.
#
# Why that matters: ReplicationLambda.Code.S3Key is a !GetAtt of the custom
# resource that parses CodeLocation. CloudFormation only calls
# UpdateFunctionCode when a resource property changes. An operator who
# overwrites one fixed key (the old flat "package.zip") leaves CodeLocation
# byte-identical, so the parser is never re-invoked, S3Key is unchanged, and
# the Lambda keeps running the PREVIOUS release's code while the template
# changes apply and the stack reports UPDATE_COMPLETE. Versioning the
# filename makes the safe path the default rather than something the
# operator has to know to do. build-package.sh's upload mode solves the same
# problem by content-hashing the key.
echo "=== Building package-$VERSION.zip ==="
TMP_DIR="$GITLAB_ROOT/tmp/release-$VERSION"
mkdir -p "$TMP_DIR"
"$GITLAB_ROOT/deploy/build-package.sh" --build-only "$TMP_DIR/package-$VERSION.zip"
# The digest line names the versioned file so `shasum -a 256 -c` works as-is
# in the directory the operator downloaded into.
shasum -a 256 "$TMP_DIR/package-$VERSION.zip" \
  | awk -v n="package-$VERSION.zip" '{print $1 "  " n}' \
  > "$TMP_DIR/package-$VERSION.zip.sha256"

echo "=== Creating GitHub Release ==="
if gh release view "v$VERSION" --repo "$GITHUB_REPO" >/dev/null 2>&1; then
  echo "Release v$VERSION already exists. Uploading assets (overwriting if present)."
  gh release upload "v$VERSION" \
    "$TMP_DIR/package-$VERSION.zip" \
    "$TMP_DIR/package-$VERSION.zip.sha256" \
    "$GITLAB_ROOT/deploy/template.yaml" \
    --repo "$GITHUB_REPO" --clobber
else
  gh release create "v$VERSION" \
    "$TMP_DIR/package-$VERSION.zip" \
    "$TMP_DIR/package-$VERSION.zip.sha256" \
    "$GITLAB_ROOT/deploy/template.yaml" \
    --title "v$VERSION" \
    --notes "See [CHANGELOG.md](CHANGELOG.md) for details." \
    --repo "$GITHUB_REPO"
fi

# Deleting a tag demotes its Release to an unlisted draft, and this script's
# "already exists" branch above does not republish. See
# .kiro/steering/github-release.md for the republish step.
if [[ "$(gh release view "v$VERSION" --repo "$GITHUB_REPO" --json isDraft --jq .isDraft)" == "true" ]]; then
  echo "WARNING: Release v$VERSION is a DRAFT and is not publicly listed." >&2
  echo "  Publish it with:" >&2
  echo "  gh release edit v$VERSION --repo $GITHUB_REPO --draft=false --latest" >&2
fi

echo ""
echo "Done. Release: https://github.com/$GITHUB_REPO/releases/tag/v$VERSION"
echo "Assets: template.yaml, package-$VERSION.zip, package-$VERSION.zip.sha256"
