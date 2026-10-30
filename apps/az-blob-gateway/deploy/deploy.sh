#!/usr/bin/env bash
# Deploy the S3 gateway (versitygw, inv-k0d.12) in front of Azure Blob into an existing
# Container Apps environment, plus the 'az-blob-gateway-doctor' ACA Job. Tracker: inv-k0d.14.
#
#   apps/az-blob-gateway/deploy/deploy.sh --dry-run          # print every az command, change nothing
#   apps/az-blob-gateway/deploy/deploy.sh                    # deploy / update (idempotent)
#   apps/az-blob-gateway/deploy/deploy.sh --checks-only      # only the role + network checks
#   apps/az-blob-gateway/deploy/deploy.sh --assign-roles     # also create missing role assignments
#
# Settings come from env vars, normally via an env file (default: deploy/.env next to this
# script; see .env.example). Role assignments and private endpoint / DNS are CHECK-ONLY by
# default: the script prints exactly what's missing so it can go to IT.
#
# Exit codes: 0 ok, 1 error, 3 deployed (or checked) but something is missing (see summary).
# Works with bash 3.2 (macOS) and the core az CLI (no extensions needed).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DOCTOR_DIR=$(cd "$SCRIPT_DIR/../doctor" && pwd)

DRY_RUN=0
ASSIGN_ROLES=0
CHECKS_ONLY=0
SKIP_DOCTOR=0
ENV_FILE="$SCRIPT_DIR/.env"

usage() {
    sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
    cat <<'EOF'

Options:
  --env-file PATH   settings file (default deploy/.env)
  --dry-run         print the az commands instead of running them (reads are faked too)
  --checks-only     skip image import and app/job create; run the role + network checks
  --assign-roles    create missing role assignments instead of only reporting them
  --skip-doctor     don't build or deploy the az-blob-gateway-doctor job
  -h, --help
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --env-file) ENV_FILE=$2; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --checks-only) CHECKS_ONLY=1; shift ;;
        --assign-roles) ASSIGN_ROLES=1; shift ;;
        --skip-doctor) SKIP_DOCTOR=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
elif [ "$DRY_RUN" = 0 ]; then
    echo "no env file at $ENV_FILE (copy .env.example); using the environment as is" >&2
fi

# ---------------------------------------------------------------- settings + defaults
: "${AZ_SUBSCRIPTION:=}"
: "${AZ_LOCATION:=westus2}"
: "${ACR_NAME:=dcpdecontainers-c8eze3cfayagfcaq}"
: "${GW_IMAGE_SOURCE:=docker.io/versity/versitygw:v1.10.0}"
: "${GW_IMAGE:=versity/versitygw:v1.10.0}"
: "${GW_APP_NAME:=az-blob-gateway}"
: "${GW_IDENTITY_NAME:=az-blob-gateway-id}"
: "${GW_INGRESS:=external}"
: "${GW_CPU:=1.0}"
: "${GW_MEMORY:=2Gi}"
: "${GW_MIN_REPLICAS:=1}"
: "${GW_MAX_REPLICAS:=1}"
: "${GW_WORKLOAD_PROFILE:=}"
: "${GW_ACCESS_KEY_ID:=gateway}"
: "${GW_SECRET_ACCESS_KEY:=}"
: "${GW_SECRET_KV_URL:=}"
: "${GW_DEBUG:=false}"
: "${KEYVAULT_NAME:=}"
: "${KEYVAULT_RESOURCE_GROUP:=}"
: "${STORAGE_RESOURCE_GROUP:=}"
: "${STORAGE_DATA_ROLE_SCOPE:=}"
: "${PRIVATE_DNS_RESOURCE_GROUP:=}"
: "${PRIVATE_DNS_SUBSCRIPTION:=}"
: "${DOCKERHUB_USERNAME:=}"
: "${DOCKERHUB_TOKEN:=}"
: "${DOCTOR_JOB_NAME:=az-blob-gateway-doctor}"
: "${DOCTOR_IMAGE_TAG:=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo latest)}"
: "${DOCTOR_IMAGE:=az-blob-gateway-doctor:$DOCTOR_IMAGE_TAG}"
: "${DOCTOR_BUCKET:=az-blob-gateway-doctor}"
: "${DOCTOR_SIZE:=1G}"
: "${DOCTOR_PART_SIZE:=32MiB}"
: "${DOCTOR_CPU:=1.0}"
: "${DOCTOR_MEMORY:=2Gi}"
: "${DOCTOR_TIMEOUT:=3600}"
: "${ALLOW_PUBLIC_INGRESS:=0}"

MISSING=()  # things IT (or --assign-roles) needs to fix; printed at the end
GW_SECRET_NAME=gw-root-secret

# ---------------------------------------------------------------- helpers
log() { printf '\n== %s\n' "$*" >&2; }
note() { printf '   %s\n' "$*" >&2; }
missing() { MISSING+=("$*"); printf '   MISSING: %s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
lower() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

# Print a command with secrets masked, quoted so it can be copy-pasted.
show_cmd() {
    local out="" a
    for a in "$@"; do
        if [ -n "$GW_SECRET_ACCESS_KEY" ]; then
            a=${a//"$GW_SECRET_ACCESS_KEY"/***}
        fi
        if [ -n "$DOCKERHUB_TOKEN" ]; then
            a=${a//"$DOCKERHUB_TOKEN"/***}
        fi
        case "$a" in
            *[!A-Za-z0-9_./:=@%+,-]* | "") a="'${a//\'/\'\\\'\'}'" ;;
        esac
        out+=" $a"
    done
    printf '   $%s\n' "$out" >&2
}

# Mutating command: printed, and run unless --dry-run.
run() {
    show_cmd "$@"
    [ "$DRY_RUN" = 1 ] && return 0
    "$@"
}

# Read-only query: printed, run, stdout captured by the caller. In --dry-run it prints the
# command and returns the placeholder given as $1 instead of calling Azure.
query() {
    local placeholder=$1; shift
    show_cmd "$@"
    if [ "$DRY_RUN" = 1 ]; then
        printf '%s' "$placeholder"
        return 0
    fi
    "$@"
}

# Like query, but a non-zero exit (e.g. resource not found) yields "" instead of failing.
query_or_empty() {
    local placeholder=$1; shift
    show_cmd "$@"
    if [ "$DRY_RUN" = 1 ]; then
        printf '%s' "$placeholder"
        return 0
    fi
    "$@" 2>/dev/null || true
}

require() {
    local v
    for v in "$@"; do
        if [ -z "${!v:-}" ]; then
            if [ "$DRY_RUN" = 1 ]; then
                eval "$v=\"<$v>\""
                note "$v not set; using placeholder <$v> for the dry run"
            else
                die "$v must be set (see deploy/.env.example)"
            fi
        fi
    done
}

# ---------------------------------------------------------------- 0. preflight
require AZ_RESOURCE_GROUP ACA_ENVIRONMENT STORAGE_ACCOUNT
: "${STORAGE_RESOURCE_GROUP:=$AZ_RESOURCE_GROUP}"
: "${ACR_RESOURCE_GROUP:=}"
: "${ACA_ENVIRONMENT_RESOURCE_GROUP:=$AZ_RESOURCE_GROUP}"
if [ -z "$GW_SECRET_ACCESS_KEY" ] && [ -z "$GW_SECRET_KV_URL" ]; then
    if [ "$DRY_RUN" = 1 ]; then
        GW_SECRET_ACCESS_KEY="<GW_SECRET_ACCESS_KEY>"
    else
        die "set GW_SECRET_ACCESS_KEY (literal, stored as a Container Apps secret) or GW_SECRET_KV_URL (Key Vault secret URL)"
    fi
fi
case "$GW_INGRESS" in external|internal) ;; *) die "GW_INGRESS must be external or internal" ;; esac

log "0. preflight"
command -v az >/dev/null || die "az CLI not found"
if [ -z "$AZ_SUBSCRIPTION" ]; then
    # Pin the current subscription so every command below names it explicitly.
    AZ_SUBSCRIPTION=$(query "<current-subscription-id>" az account show --query id -o tsv) \
        || die "not logged in: run 'az login' first"
fi
note "subscription $AZ_SUBSCRIPTION"
S=(--subscription "$AZ_SUBSCRIPTION")
RG=(-g "$AZ_RESOURCE_GROUP")

# ---------------------------------------------------------------- 1. environment
log "1. Container Apps environment $ACA_ENVIRONMENT"
ENV_INFO=$(query 'true|10.0.0.10|<env-unique>.westus2.azurecontainerapps.io|/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Network/virtualNetworks/<vnet>/subnets/<subnet>|/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.App/managedEnvironments/<env>' \
    az containerapp env show "${S[@]}" -g "$ACA_ENVIRONMENT_RESOURCE_GROUP" -n "$ACA_ENVIRONMENT" \
    --query '[properties.vnetConfiguration.internal, properties.staticIp, properties.defaultDomain, properties.vnetConfiguration.infrastructureSubnetId, id]' -o tsv \
    | tr '\n' '|') || die "can't read environment $ACA_ENVIRONMENT"
IFS='|' read -r ENV_INTERNAL ENV_STATIC_IP ENV_DOMAIN ENV_SUBNET ENV_ID <<<"$ENV_INFO"
note "internal=$ENV_INTERNAL staticIp=$ENV_STATIC_IP defaultDomain=$ENV_DOMAIN"
ENV_VNET_ID=${ENV_SUBNET%/subnets/*}
if [ "$GW_INGRESS" = external ] && [ "$(lower "$ENV_INTERNAL")" != true ] && [ "$ALLOW_PUBLIC_INGRESS" != 1 ]; then
    die "environment is not internal-only, so external ingress would be public. Use an internal environment, or GW_INGRESS=internal (env-only, unreachable from citynet), or ALLOW_PUBLIC_INGRESS=1."
fi
if [ "$GW_INGRESS" = internal ]; then
    note "GW_INGRESS=internal: only apps/jobs in this environment can reach the gateway, not citynet."
fi

# ---------------------------------------------------------------- 2. images
if [ "$CHECKS_ONLY" = 0 ]; then
    log "2. images in ACR $ACR_NAME"
    GW_REPO=${GW_IMAGE%:*}
    GW_TAG=${GW_IMAGE##*:}
    HAVE=$(query_or_empty "" az acr repository show-tags "${S[@]}" -n "$ACR_NAME" --repository "$GW_REPO" \
        --query "contains(@, '$GW_TAG')" -o tsv)
    if [ "$HAVE" = true ]; then
        note "$GW_IMAGE already in $ACR_NAME"
    else
        IMPORT=(az acr import "${S[@]}" -n "$ACR_NAME" --source "$GW_IMAGE_SOURCE" -t "$GW_IMAGE")
        if [ -n "$DOCKERHUB_USERNAME" ]; then
            IMPORT+=(-u "$DOCKERHUB_USERNAME" -p "$DOCKERHUB_TOKEN")
        fi
        run "${IMPORT[@]}"
    fi

    if [ "$SKIP_DOCTOR" = 0 ]; then
        D_REPO=${DOCTOR_IMAGE%:*}
        D_TAG=${DOCTOR_IMAGE##*:}
        HAVE=$(query_or_empty "" az acr repository show-tags "${S[@]}" -n "$ACR_NAME" --repository "$D_REPO" \
            --query "contains(@, '$D_TAG')" -o tsv)
        if [ "$HAVE" = true ]; then
            note "$DOCTOR_IMAGE already in $ACR_NAME"
        else
            # ACR Tasks build: needs no local docker. If the registry blocks Tasks, build and
            # push by hand instead (RUNBOOK.md).
            run az acr build "${S[@]}" -r "$ACR_NAME" -t "$DOCTOR_IMAGE" "$DOCTOR_DIR"
        fi
    fi
fi
ACR_SERVER="$ACR_NAME.azurecr.io"

# ---------------------------------------------------------------- 3. identity
log "3. user-assigned managed identity $GW_IDENTITY_NAME"
ID_INFO=$(query_or_empty "" az identity show "${S[@]}" "${RG[@]}" -n "$GW_IDENTITY_NAME" \
    --query '[id, clientId, principalId]' -o tsv | tr '\n' '|')
if [ -z "$ID_INFO" ]; then
    if [ "$CHECKS_ONLY" = 1 ] && [ "$DRY_RUN" = 0 ]; then
        die "identity $GW_IDENTITY_NAME doesn't exist yet; run without --checks-only first"
    fi
    CREATE_ID=(az identity create "${S[@]}" "${RG[@]}" -n "$GW_IDENTITY_NAME" -l "$AZ_LOCATION"
        --query '[id, clientId, principalId]' -o tsv)
    show_cmd "${CREATE_ID[@]}"
    if [ "$DRY_RUN" = 1 ]; then
        ID_INFO="/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.ManagedIdentity/userAssignedIdentities/$GW_IDENTITY_NAME|<client-id>|<principal-id>"
    else
        ID_INFO=$("${CREATE_ID[@]}" | tr '\n' '|')
        note "created; a new identity can take a minute before role assignments see it"
    fi
fi
IFS='|' read -r MI_ID MI_CLIENT_ID MI_PRINCIPAL_ID <<<"$ID_INFO"
note "clientId=$MI_CLIENT_ID principalId=$MI_PRINCIPAL_ID"

# ---------------------------------------------------------------- 4. role checks
log "4. role assignments for $GW_IDENTITY_NAME (check-only unless --assign-roles)"
STORAGE_INFO=$(query "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT|Disabled|Deny" \
    az storage account show "${S[@]}" -g "$STORAGE_RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" \
    --query '[id, publicNetworkAccess, networkRuleSet.defaultAction]' -o tsv | tr '\n' '|') \
    || die "can't read storage account $STORAGE_ACCOUNT"
IFS='|' read -r STORAGE_ID STORAGE_PUBLIC STORAGE_DEFAULT_ACTION <<<"$STORAGE_INFO"
: "${STORAGE_DATA_ROLE_SCOPE:=$STORAGE_ID}"
ACR_RG_ARGS=()
if [ -n "$ACR_RESOURCE_GROUP" ]; then
    ACR_RG_ARGS=(-g "$ACR_RESOURCE_GROUP")
fi
ACR_ID=$(query "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.ContainerRegistry/registries/$ACR_NAME" \
    az acr show "${S[@]}" "${ACR_RG_ARGS[@]+"${ACR_RG_ARGS[@]}"}" -n "$ACR_NAME" --query id -o tsv)

# All of the identity's assignments in the subscription, inherited ones included: "role<TAB>scope".
ASSIGNMENTS=$(query "" az role assignment list "${S[@]}" --assignee-object-id "$MI_PRINCIPAL_ID" \
    --all --include-inherited --query '[].[roleDefinitionName, scope]' -o tsv)

# has_role ROLE TARGET_SCOPE: true if ROLE is assigned at TARGET_SCOPE or any parent of it.
has_role() {
    local role=$1 target r s
    target=$(lower "$2")
    while IFS=$'\t' read -r r s; do
        [ -z "$r" ] && continue
        s=$(lower "${s%/}")
        if [ "$r" = "$role" ] && { [ "$s" = "" ] || [ "$s" = "$target" ] || [ "${target#"$s"/}" != "$target" ]; }; then
            return 0
        fi
    done <<<"$ASSIGNMENTS"
    return 1
}

# ensure_role ROLE SCOPE WHY
ensure_role() {
    local role=$1 scope=$2 why=$3
    if [ "$DRY_RUN" = 1 ]; then
        note "would check: '$role' on $scope ($why)"
        if [ "$ASSIGN_ROLES" = 1 ]; then
            run az role assignment create "${S[@]}" --assignee-object-id "$MI_PRINCIPAL_ID" \
                --assignee-principal-type ServicePrincipal --role "$role" --scope "$scope"
        fi
        return 0
    fi
    if has_role "$role" "$scope"; then
        note "ok: '$role' on $scope"
        return 0
    fi
    if [ "$ASSIGN_ROLES" = 1 ]; then
        run az role assignment create "${S[@]}" --assignee-object-id "$MI_PRINCIPAL_ID" \
            --assignee-principal-type ServicePrincipal --role "$role" --scope "$scope" -o none
    else
        missing "role '$role' for identity $GW_IDENTITY_NAME (principalId $MI_PRINCIPAL_ID) on scope $scope: $why"
    fi
}

# Blob data access. Contributor (or Owner) on the account, or on a container under it.
if has_role "Storage Blob Data Owner" "$STORAGE_DATA_ROLE_SCOPE"; then
    note "ok: 'Storage Blob Data Owner' covers data access"
else
    ensure_role "Storage Blob Data Contributor" "$STORAGE_DATA_ROLE_SCOPE" "gateway reads/writes blobs"
fi
# Server-side copy needs a user delegation key, which is an account-level permission.
# Contributor/Owner at account scope (or above) include it; otherwise add Delegator.
if [ "$DRY_RUN" = 0 ] && { has_role "Storage Blob Data Contributor" "$STORAGE_ID" || has_role "Storage Blob Data Owner" "$STORAGE_ID"; }; then
    note "ok: data role at account scope includes generateUserDelegationKey (server-side copy)"
else
    ensure_role "Storage Blob Delegator" "$STORAGE_ID" \
        "user delegation key for server-side CopyObject; without it copies silently stream through the gateway"
fi
# Image pull. az containerapp create --registry-identity also tries to add this itself.
if [ "$DRY_RUN" = 1 ] || ! { has_role AcrPull "$ACR_ID" || has_role AcrPush "$ACR_ID" || has_role Contributor "$ACR_ID" || has_role Owner "$ACR_ID"; }; then
    ensure_role AcrPull "$ACR_ID" "pull the gateway and doctor images"
else
    note "ok: identity can pull from $ACR_NAME"
fi
if [ -n "$GW_SECRET_KV_URL" ]; then
    if [ -n "$KEYVAULT_NAME" ]; then
        KV_RG_ARGS=()
        if [ -n "$KEYVAULT_RESOURCE_GROUP" ]; then
            KV_RG_ARGS=(-g "$KEYVAULT_RESOURCE_GROUP")
        fi
        KV_ID=$(query "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/$KEYVAULT_NAME" \
            az keyvault show "${S[@]}" "${KV_RG_ARGS[@]+"${KV_RG_ARGS[@]}"}" -n "$KEYVAULT_NAME" --query id -o tsv)
        ensure_role "Key Vault Secrets User" "$KV_ID" "read $GW_SECRET_KV_URL (RBAC vaults; access-policy vaults aren't checked)"
    else
        note "GW_SECRET_KV_URL set but KEYVAULT_NAME isn't: not checking the identity's Key Vault access"
    fi
fi

# ---------------------------------------------------------------- 5. network checks
log "5. network: storage private endpoint + DNS (check-only)"
note "storage publicNetworkAccess=$STORAGE_PUBLIC networkRuleSet.defaultAction=$STORAGE_DEFAULT_ACTION"
PE=$(query "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Network/privateEndpoints/<pe>	Approved" \
    az storage account show "${S[@]}" -g "$STORAGE_RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" \
    --query "privateEndpointConnections[].[privateEndpoint.id, privateLinkServiceConnectionState.status]" -o tsv)
if [ -z "$PE" ]; then
    if [ "$(lower "$STORAGE_PUBLIC")" = enabled ] && [ "$(lower "$STORAGE_DEFAULT_ACTION")" = allow ]; then
        note "no private endpoint, but public network access is open: the gateway will reach blob over the public endpoint"
    else
        missing "storage account $STORAGE_ACCOUNT has no private endpoint and public access is restricted: the gateway can't reach blob. Needs a blob private endpoint reachable from the ACA env VNet ($ENV_VNET_ID)."
    fi
else
    note "private endpoint connections (id, status):"
    while IFS= read -r line; do note "  $line"; done <<<"$PE"
    if ! printf '%s\n' "$PE" | grep -q 'Approved'; then
        missing "storage private endpoint connection exists but none is Approved"
    fi
fi
if [ -n "$PRIVATE_DNS_RESOURCE_GROUP" ]; then
    DNS_S=(--subscription "${PRIVATE_DNS_SUBSCRIPTION:-$AZ_SUBSCRIPTION}")
    BLOB_ZONE=privatelink.blob.core.windows.net
    A=$(query_or_empty "10.0.1.5" az network private-dns record-set a show "${DNS_S[@]}" \
        -g "$PRIVATE_DNS_RESOURCE_GROUP" -z "$BLOB_ZONE" -n "$STORAGE_ACCOUNT" \
        --query 'aRecords[].ipv4Address' -o tsv)
    if [ -z "$A" ]; then
        missing "private DNS: no A record '$STORAGE_ACCOUNT' in zone $BLOB_ZONE (rg $PRIVATE_DNS_RESOURCE_GROUP)"
    else
        note "ok: $STORAGE_ACCOUNT.$BLOB_ZONE -> $A"
    fi
    LINKED=$(query_or_empty "$ENV_VNET_ID" az network private-dns link vnet list "${DNS_S[@]}" \
        -g "$PRIVATE_DNS_RESOURCE_GROUP" -z "$BLOB_ZONE" --query '[].virtualNetwork.id' -o tsv)
    if printf '%s\n' "$LINKED" | tr '[:upper:]' '[:lower:]' | grep -qxF "$(lower "$ENV_VNET_ID")"; then
        note "ok: $BLOB_ZONE is linked to the env VNet"
    else
        missing "private DNS zone $BLOB_ZONE isn't linked to the ACA env VNet $ENV_VNET_ID (inside the env, blob would resolve to its public IP). Ignore if the VNet uses custom DNS that forwards to a resolver that knows the zone."
    fi
    STAR=$(query_or_empty "$ENV_STATIC_IP" az network private-dns record-set a show "${DNS_S[@]}" \
        -g "$PRIVATE_DNS_RESOURCE_GROUP" -z "$ENV_DOMAIN" -n '*' --query 'aRecords[].ipv4Address' -o tsv)
    if [ "$STAR" = "$ENV_STATIC_IP" ]; then
        note "ok: *.$ENV_DOMAIN -> $ENV_STATIC_IP"
    else
        missing "private DNS: zone $ENV_DOMAIN needs a wildcard A record '*' -> $ENV_STATIC_IP (found '${STAR:-none}' in rg $PRIVATE_DNS_RESOURCE_GROUP), and citynet DNS must resolve it, or desktops can't find the gateway"
    fi
else
    note "PRIVATE_DNS_RESOURCE_GROUP not set: skipping private DNS zone checks."
    note "IT needs: (a) $STORAGE_ACCOUNT.privatelink.blob.core.windows.net resolvable from the env VNet,"
    note "          (b) *.$ENV_DOMAIN -> $ENV_STATIC_IP resolvable from citynet."
    note "The doctor tests both for real: blob_dns (job, --inside) and dns (from citynet)."
fi

# ---------------------------------------------------------------- 6. gateway app
if [ -n "$GW_SECRET_KV_URL" ]; then
    SECRET_SPEC="$GW_SECRET_NAME=keyvaultref:$GW_SECRET_KV_URL,identityref:$MI_ID"
else
    SECRET_SPEC="$GW_SECRET_NAME=$GW_SECRET_ACCESS_KEY"
fi
# versitygw is configured entirely by env vars (its image entrypoint reads VGW_BACKEND), so
# there are no container args. Flag names: versitygw v1.10.0 cmd/versitygw/main.go and
# cmd/internal/gwcli/azure.go. No AZ_ACCESS_KEY -> DefaultAzureCredential -> this identity
# (AZURE_CLIENT_ID picks the user-assigned one). AZ_ACCOUNT_NAME must be set: without it
# versitygw uses AZURE_CLIENT_ID as the account name.
GW_ENV=(
    VGW_BACKEND=azure
    VGW_PORT=:7070
    VGW_HEALTH=/health
    "ROOT_ACCESS_KEY_ID=$GW_ACCESS_KEY_ID"
    "ROOT_SECRET_ACCESS_KEY=secretref:$GW_SECRET_NAME"
    "AZ_ACCOUNT_NAME=$STORAGE_ACCOUNT"
    "AZURE_CLIENT_ID=$MI_CLIENT_ID"
    "VGW_DEBUG=$GW_DEBUG"
)
WP_ARGS=()
if [ -n "$GW_WORKLOAD_PROFILE" ]; then
    WP_ARGS=(-w "$GW_WORKLOAD_PROFILE")
fi

GW_FQDN=""
if [ "$CHECKS_ONLY" = 0 ]; then
    log "6. gateway container app $GW_APP_NAME"
    EXISTS=$(query_or_empty "" az containerapp show "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" --query name -o tsv)
    if [ -z "$EXISTS" ]; then
        run az containerapp create "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" \
            --environment "$ENV_ID" \
            "${WP_ARGS[@]+"${WP_ARGS[@]}"}" \
            --image "$ACR_SERVER/$GW_IMAGE" \
            --registry-server "$ACR_SERVER" --registry-identity "$MI_ID" \
            --user-assigned "$MI_ID" \
            --secrets "$SECRET_SPEC" \
            --env-vars "${GW_ENV[@]}" \
            --ingress "$GW_INGRESS" --target-port 7070 --transport http --allow-insecure false \
            --cpu "$GW_CPU" --memory "$GW_MEMORY" \
            --min-replicas "$GW_MIN_REPLICAS" --max-replicas "$GW_MAX_REPLICAS" \
            -o none
    else
        note "exists: updating in place"
        run az containerapp identity assign "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" --user-assigned "$MI_ID" -o none
        run az containerapp registry set "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" --server "$ACR_SERVER" --identity "$MI_ID" -o none
        run az containerapp secret set "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" --secrets "$SECRET_SPEC" -o none
        run az containerapp ingress update "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" \
            --type "$GW_INGRESS" --target-port 7070 --transport http --allow-insecure false -o none
        run az containerapp update "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" \
            "${WP_ARGS[@]+"${WP_ARGS[@]}"}" \
            --image "$ACR_SERVER/$GW_IMAGE" \
            --set-env-vars "${GW_ENV[@]}" \
            --cpu "$GW_CPU" --memory "$GW_MEMORY" \
            --min-replicas "$GW_MIN_REPLICAS" --max-replicas "$GW_MAX_REPLICAS" \
            -o none
    fi
    GW_FQDN=$(query "$GW_APP_NAME.$ENV_DOMAIN" az containerapp show "${S[@]}" "${RG[@]}" -n "$GW_APP_NAME" \
        --query properties.configuration.ingress.fqdn -o tsv)
    note "gateway URL: https://$GW_FQDN"
fi

# ---------------------------------------------------------------- 7. doctor job
if [ "$CHECKS_ONLY" = 0 ] && [ "$SKIP_DOCTOR" = 0 ]; then
    log "7. doctor job $DOCTOR_JOB_NAME (manual trigger)"
    # Configured by env vars only (doctor.py reads them); change them with
    # 'az containerapp job update --set-env-vars ...' and start the job again.
    DOCTOR_ENV=(
        "GATEWAY_URL=https://$GW_FQDN"
        "GATEWAY_ACCESS_KEY=$GW_ACCESS_KEY_ID"
        "GATEWAY_SECRET_KEY=secretref:$GW_SECRET_NAME"
        "GATEWAY_BUCKET=$DOCTOR_BUCKET"
        "AZ_ACCOUNT_NAME=$STORAGE_ACCOUNT"
        "AZURE_CLIENT_ID=$MI_CLIENT_ID"
        DOCTOR_INSIDE=1
        DOCTOR_JSON=1
        "DOCTOR_SIZE=$DOCTOR_SIZE"
        "DOCTOR_PART_SIZE=$DOCTOR_PART_SIZE"
    )
    EXISTS=$(query_or_empty "" az containerapp job show "${S[@]}" "${RG[@]}" -n "$DOCTOR_JOB_NAME" --query name -o tsv)
    if [ -z "$EXISTS" ]; then
        run az containerapp job create "${S[@]}" "${RG[@]}" -n "$DOCTOR_JOB_NAME" \
            --environment "$ENV_ID" \
            "${WP_ARGS[@]+"${WP_ARGS[@]}"}" \
            --trigger-type Manual \
            --replica-timeout "$DOCTOR_TIMEOUT" --replica-retry-limit 0 \
            --parallelism 1 --replica-completion-count 1 \
            --image "$ACR_SERVER/$DOCTOR_IMAGE" \
            --registry-server "$ACR_SERVER" --registry-identity "$MI_ID" \
            --mi-user-assigned "$MI_ID" \
            --secrets "$SECRET_SPEC" \
            --env-vars "${DOCTOR_ENV[@]}" \
            --cpu "$DOCTOR_CPU" --memory "$DOCTOR_MEMORY" \
            -o none
    else
        note "exists: updating in place"
        run az containerapp job identity assign "${S[@]}" "${RG[@]}" -n "$DOCTOR_JOB_NAME" --user-assigned "$MI_ID" -o none
        run az containerapp job registry set "${S[@]}" "${RG[@]}" -n "$DOCTOR_JOB_NAME" --server "$ACR_SERVER" --identity "$MI_ID" -o none
        run az containerapp job secret set "${S[@]}" "${RG[@]}" -n "$DOCTOR_JOB_NAME" --secrets "$SECRET_SPEC" -o none
        run az containerapp job update "${S[@]}" "${RG[@]}" -n "$DOCTOR_JOB_NAME" \
            --image "$ACR_SERVER/$DOCTOR_IMAGE" \
            --replica-timeout "$DOCTOR_TIMEOUT" --replica-retry-limit 0 \
            --set-env-vars "${DOCTOR_ENV[@]}" \
            --cpu "$DOCTOR_CPU" --memory "$DOCTOR_MEMORY" \
            -o none
    fi
fi

# ---------------------------------------------------------------- summary
log "summary"
if [ -n "$GW_FQDN" ]; then
    note "gateway:   https://$GW_FQDN   (S3 access key $GW_ACCESS_KEY_ID, secret in $GW_SECRET_NAME)"
    note "doctor from citynet:  uv run apps/az-blob-gateway/doctor/doctor.py --endpoint https://$GW_FQDN --access-key $GW_ACCESS_KEY_ID --secret-key ... --json > doctor-citynet.json"
    note "doctor in the VNet:   az containerapp job start ${S[*]} -g $AZ_RESOURCE_GROUP -n $DOCTOR_JOB_NAME"
fi
if [ "${#MISSING[@]}" -gt 0 ]; then
    printf '\nMISSING (send to IT, or rerun with --assign-roles if you have the rights):\n' >&2
    for m in "${MISSING[@]}"; do printf '  - %s\n' "$m" >&2; done
    exit 3
fi
note "nothing missing that this script can see"
