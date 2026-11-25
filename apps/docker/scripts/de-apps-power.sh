#!/usr/bin/env bash
# Start, stop, or show the DE container apps in ace-azu-e2-dcp-de-nonprod.
#
#   apps/docker/scripts/de-apps-power.sh status
#   apps/docker/scripts/de-apps-power.sh stop  [--postgres]   # also stop the Flexible Server
#   apps/docker/scripts/de-apps-power.sh start [--postgres]
#
# Only apps named de-* are touched. The environment also holds other teams' apps
# (e.g. ud-tools-core), which this script never changes. Override with PREFIX=...
#
# Start and stop go through the ARM REST API, because the core az CLI has no
# `containerapp start/stop` without the containerapp extension.
set -euo pipefail

RG=rg-azu-e2-dcp-de-nonprod
ENV_NAME=ace-azu-e2-dcp-de-nonprod
PG_SERVER=psql-azu-e2-dcp-de-nonprod
PREFIX=${PREFIX:-de-}
API_VERSION=2024-03-01
WAIT_SECONDS=${WAIT_SECONDS:-300}

# Start in dependency order (storage gateway, then the runner the webserver connects to);
# stop in reverse. Any other de-* apps go after these.
ORDER=(de-blob-gateway de-dagster-runner de-dagster de-tools)

usage() {
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

list_apps() {
    az containerapp list -g "$RG" \
        --query "[?ends_with(properties.managedEnvironmentId, '/$ENV_NAME') && starts_with(name, '$PREFIX')].name" \
        -o tsv
}

ordered_apps() {
    local found ordered=()
    found=$(list_apps)
    for app in "${ORDER[@]}"; do
        grep -qx "$app" <<<"$found" && ordered+=("$app")
    done
    while read -r app; do
        [[ -z "$app" ]] && continue
        [[ " ${ordered[*]} " == *" $app "* ]] || ordered+=("$app")
    done <<<"$found"
    printf '%s\n' "${ordered[@]}"
}

status() {
    az containerapp list -g "$RG" \
        --query "[?ends_with(properties.managedEnvironmentId, '/$ENV_NAME') && starts_with(name, '$PREFIX')].{app:name, status:properties.runningStatus, revision:properties.latestReadyRevisionName}" \
        -o table
    printf '\nPostgres %s: ' "$PG_SERVER"
    az postgres flexible-server show -g "$RG" -n "$PG_SERVER" --query state -o tsv 2>/dev/null || echo "(no access)"
}

wait_for() {
    local app=$1 want=$2 waited=0 state
    while :; do
        state=$(az containerapp show -g "$RG" -n "$app" --query properties.runningStatus -o tsv)
        if [[ "$state" == "$want" ]]; then
            echo "  $app: $state"
            return 0
        fi
        if (( waited >= WAIT_SECONDS )); then
            echo "  $app: still $state after ${WAIT_SECONDS}s, moving on" >&2
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
    done
}

power() {
    local action=$1 want app id
    [[ "$action" == start ]] && want=Running || want=Stopped
    local apps=()
    while read -r app; do [[ -n "$app" ]] && apps+=("$app"); done < <(ordered_apps)
    if [[ "$action" == stop ]]; then
        local reversed=()
        for ((i=${#apps[@]}-1; i>=0; i--)); do reversed+=("${apps[i]}"); done
        apps=("${reversed[@]}")
    fi
    if (( ${#apps[@]} == 0 )); then
        echo "No ${PREFIX}* apps found in $ENV_NAME" >&2
        exit 1
    fi

    for app in "${apps[@]}"; do
        echo "${action}: $app"
        id=$(az containerapp show -g "$RG" -n "$app" --query id -o tsv)
        az rest --method post --url "https://management.azure.com${id}/${action}?api-version=${API_VERSION}" -o none
        wait_for "$app" "$want"
    done
}

postgres() {
    local action=$1
    echo "${action}: postgres $PG_SERVER (takes a few minutes)"
    az postgres flexible-server "$action" -g "$RG" -n "$PG_SERVER" -o none
}

action=${1:-}
with_pg=false
[[ "${2:-}" == "--postgres" ]] && with_pg=true

case "$action" in
    status) status ;;
    start)
        # Postgres first, so Dagster finds its database when it comes up.
        $with_pg && postgres start
        power start
        status
        ;;
    stop)
        power stop
        $with_pg && postgres stop
        status
        ;;
    *) usage ;;
esac
