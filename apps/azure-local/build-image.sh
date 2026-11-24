#!/bin/bash
# Build the de-apps image (apps/docker/Dockerfile) from git-tracked and unignored files only, so
# stray local directories (e.g. an old python/library/) can't break the Dockerfile's python/*/
# glob. Usage: apps/azure-local/build-image.sh [tag]   (default de-apps:local)
set -euo pipefail

tag="${1:-de-apps:local}"
repo="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
ctx="$(mktemp -d)"
trap 'rm -rf "${ctx}"' EXIT

git -C "${repo}" ls-files -co --exclude-standard -z \
    | (cd "${repo}" && COPYFILE_DISABLE=1 tar --null -T - -cf -) \
    | tar -xf - -C "${ctx}"

docker build --platform linux/amd64 -f "${ctx}/apps/docker/Dockerfile" -t "${tag}" "${ctx}"
