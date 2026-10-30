#!/bin/bash
# S3 gateway (versitygw) in front of Azure Blob. versitygw reads these env vars itself.
# AZ_ENDPOINT and AZ_ACCESS_KEY are optional: leave them unset in Azure to use managed identity.

set -e

: "${ROOT_ACCESS_KEY_ID:?}" "${ROOT_SECRET_ACCESS_KEY:?}" "${AZ_ACCOUNT_NAME:?}"

echo "[$(date -Iseconds)] az-blob-gateway starting (account: $AZ_ACCOUNT_NAME)..."
exec versitygw --port ":${GATEWAY_PORT:-7070}" --health /health azure
