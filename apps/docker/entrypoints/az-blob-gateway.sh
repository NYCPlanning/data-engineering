#!/bin/bash
# S3 gateway (versitygw) in front of Azure Blob. versitygw reads ROOT_* and AZ_* itself.
# AZ_ENDPOINT and AZ_ACCESS_KEY are optional: leave them unset in Azure to use managed identity.

set -e

: "${AZ_BLOB_GATEWAY_ACCESS_KEY_ID:?}" "${AZ_BLOB_GATEWAY_SECRET_ACCESS_KEY:?}" "${AZ_ACCOUNT_NAME:?}"
# Clients use the same AZ_BLOB_GATEWAY_* names.
export ROOT_ACCESS_KEY_ID="$AZ_BLOB_GATEWAY_ACCESS_KEY_ID"
export ROOT_SECRET_ACCESS_KEY="$AZ_BLOB_GATEWAY_SECRET_ACCESS_KEY"

echo "[$(date -Iseconds)] az-blob-gateway starting (account: $AZ_ACCOUNT_NAME)..."
exec versitygw --port ":${AZ_BLOB_GATEWAY_PORT:-7070}" --health /health azure
