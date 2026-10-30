# Blob gateway on Azure: runbook

Deploys the S3 gateway (versitygw v1.10.0, chosen in tracker bead `inv-k0d.12`) in front of
Azure Blob into our Container Apps environment, then tests two paths with the gateway doctor:

- **citynet -> gateway**: the path desktops take. Run the doctor on your citynet machine.
- **ACA job -> gateway -> blob**: the path Azure runners take. Run the `gateway-doctor` ACA Job.

Tracker: `inv-k0d.14`. **Paste every doctor `--json` output onto that bead.**

Files:

| path | what |
|---|---|
| `deploy/deploy.sh` | idempotent az CLI deploy; `--dry-run`, `--checks-only`, `--assign-roles` |
| `deploy/.env.example` | every setting, with defaults; copy to `deploy/.env` (gitignored) |
| `doctor/doctor.py` | single-file doctor (boto3 + stdlib), `uv run doctor.py` anywhere |
| `doctor/Dockerfile` | image for the `gateway-doctor` ACA Job |

Locally, the same doctor runs against the devstack compose (Azurite + versitygw); see the end.

## What you need

- A citynet machine with `az` (core CLI is enough; `az containerapp job logs show` needs the
  `containerapp` extension, see step 5), `bash` (macOS, Linux, WSL or Git Bash) and `uv`.
- `az login` as someone who can create Container Apps, identities and ACR imports in the RG.
  Creating role assignments needs Owner / User Access Administrator; without it, the script
  prints what's missing for IT.
- An existing **internal** Container Apps environment (VNet-integrated, no public IP) in
  US West 2.

## Commands, in order

Run from the repo root.

### 1. Settings

```sh
cp apps/blob-gateway/deploy/.env.example apps/blob-gateway/deploy/.env
$EDITOR apps/blob-gateway/deploy/.env
```

Required: `AZ_RESOURCE_GROUP`, `ACA_ENVIRONMENT`, `STORAGE_ACCOUNT`, and either
`GW_SECRET_ACCESS_KEY` (e.g. `openssl rand -hex 32`) or `GW_SECRET_KV_URL`. Set
`PRIVATE_DNS_RESOURCE_GROUP` if you know where IT keeps the private DNS zones; the script then
checks them as well.

### 2. Dry run, then check-only

```sh
apps/blob-gateway/deploy/deploy.sh --dry-run        # prints every az command, touches nothing
```

Read it over. Then, once the identity exists (step 3 creates it), this runs only the role and
network checks and exits 3 if anything is missing:

```sh
apps/blob-gateway/deploy/deploy.sh --checks-only
```

### 3. Deploy

```sh
apps/blob-gateway/deploy/deploy.sh
```

What it does, in order:

1. Reads the environment and refuses external ingress unless the environment is internal-only.
2. `az acr import`s `docker.io/versity/versitygw:v1.10.0` into our ACR, since the VNet may not
   reach Docker Hub. Then `az acr build`s the doctor image (ACR Tasks, so no local docker).
3. Creates the user-assigned identity `blob-gateway-id`, if it doesn't exist yet.
4. Checks the identity's roles: **Storage Blob Data Contributor** on the account (or on
   `STORAGE_DATA_ROLE_SCOPE`), **Storage Blob Delegator** on the account when Contributor isn't
   at account scope, **AcrPull** on the ACR, and **Key Vault Secrets User** when the secret
   comes from Key Vault. By default these are only checked. With `--assign-roles` the script
   creates the missing ones.
5. Checks the network, without changing it: does the storage account have an approved private
   endpoint, and with `PRIVATE_DNS_RESOURCE_GROUP` set, is there an
   `<account>.privatelink.blob.core.windows.net` A record, is that zone linked to the env VNet,
   and is there a `*.<env default domain>` wildcard pointing at the env's static IP?
6. Creates or updates the container app `blob-gateway`. Ingress is HTTP on target port 7070,
   on its own FQDN, `allowInsecure=false`, min/max replicas 1. versitygw is configured with
   env vars only (`VGW_BACKEND=azure`, `AZ_ACCOUNT_NAME`, `AZURE_CLIENT_ID`). There's no
   `AZ_ACCESS_KEY`, so it authenticates to blob as the managed identity.
7. Creates or updates the manual-trigger job `gateway-doctor`, with `DOCTOR_INSIDE=1`,
   `DOCTOR_JSON=1` and `DOCTOR_SIZE=1G`.

It prints the gateway URL at the end, along with a **MISSING** list to send to IT. If the
list isn't empty, the exit code is 3. Rerun the script whenever you like; it updates in place.
If the role assignments were only just made, wait a few minutes before running the doctor,
because managed-identity tokens are cached for a while.

### 4. Doctor from citynet (desktops' path)

```sh
uv run apps/blob-gateway/doctor/doctor.py \
  --endpoint https://blob-gateway.<env default domain> \
  --access-key gateway --secret-key "$GW_SECRET_ACCESS_KEY" \
  --expect-private --json > doctor-citynet.json
```

That's the quick run: 100 MiB for the large-file layer. When it's green, do the full run with
`--size 5G`. The data is generated in memory, so it doesn't touch the disk. Peak memory is
about part size x concurrency, roughly 160 MiB at the defaults. If you're behind
`HTTPS_PROXY`, add the gateway domain to `NO_PROXY`. The doctor puts your proxy env vars in
the JSON.

### 5. Doctor inside the VNet (Azure runners' path)

```sh
az containerapp job start -g <rg> -n gateway-doctor
az containerapp job execution list -g <rg> -n gateway-doctor -o table   # wait for Succeeded/Failed
az extension add -n containerapp      # once; 'job logs show' is extension-only
az containerapp job logs show -g <rg> -n gateway-doctor --container gateway-doctor --format text --tail 300
```

The report is the one log line that starts with `{"doctor"`. The lines just above it are the
readable summary. For a 5 GB run:

```sh
az containerapp job update -g <rg> -n gateway-doctor --set-env-vars DOCTOR_SIZE=5G
az containerapp job start  -g <rg> -n gateway-doctor
```

Other knobs (env vars on the job): `DOCTOR_PART_SIZE`, `DOCTOR_CONCURRENCY`,
`DOCTOR_SMALL_COUNT`, `DOCTOR_SKIP` (comma list of layers), `DOCTOR_KEEP_GOING=1`.

### 6. Report back

Paste each `--json` output (citynet and job; 1 GB and 5 GB) onto bead `inv-k0d.14`, as is. A
line saying where each one ran from helps.

## Reading the doctor

The layers run in this order, and the doctor stops at the first one that fails unless you pass
`--keep-going`:

```
dns -> tcp -> tls -> health -> [--inside: blob_dns -> mi_token -> blob_direct]
    -> s3_auth -> small_rw -> multipart -> large -> copy -> many_small
```

A pass means the hop works. A warn means it works but something looks off. Each failure prints
a hint. Here's what a failure at each layer usually means, from most to least likely:

| layer | failure usually means |
|---|---|
| `dns` | citynet DNS can't see the env's private zone: `*.<env default domain>` -> env static IP. IT has to add it, or conditionally forward it to Azure DNS. |
| `tcp` | Firewall, NSG or routing between citynet and the env's internal load balancer. Or the wrong port: ingress serves 443, not 7070. |
| `tls` | The hostname doesn't match `*.<env domain>`, or something on the path intercepts TLS (use `--ca-bundle`). |
| `health` | **404**: the app's ingress is `internal` (env-only) or the FQDN is wrong. **301/308**: you used `http://`; S3 clients don't follow the redirect, so use `https://`. **503**: no healthy replica (check `az containerapp logs show -g <rg> -n blob-gateway`). |
| `blob_dns` | `<account>.blob.core.windows.net` resolves to a public IP inside the env: there's no private endpoint, or `privatelink.blob.core.windows.net` isn't linked to the env VNet. |
| `mi_token` | The identity isn't on the job, or `AZURE_CLIENT_ID` is wrong. |
| `blob_direct` | **403 on the container**: no Storage Blob Data Contributor. **Fails on the user delegation key**: there's no Delegator (or Contributor at account scope), so server-side copy will fall back to streaming. **Timeouts**: the network path to blob. |
| `s3_auth` | **SignatureDoesNotMatch / InvalidAccessKeyId**: wrong keys, or a proxy rewrote the host or path. **500**: the gateway can't reach or authenticate to blob (the same causes as the three rows above; check the gateway logs). |
| `small_rw` | **InvalidMetadata**: an Azure metadata key with a hyphen (see below). |
| `large` | **Timeouts near 240 s**: one part took longer than the ACA ingress timeout, so lower `--part-size`. **502/503 mid-run**: the gateway ran out of memory (part size x parts in flight, across all clients), so raise `GW_MEMORY` or lower the part size or concurrency. **413**: there's a body size limit on the path. |
| `copy` | **Fails**: the source is over 5 GiB (`VGW_COPY_OBJECT_THRESHOLD`). **"NOT server-side"** (with `--inside`): the copy worked, but the bytes streamed through the gateway. That's the missing-Delegator case. |
| `many_small` | Per-request overhead or throttling. Compare `put_ms_each` with the devstack bench (about 6.5 ms per PUT). |

### Known kinks (from inv-k0d.12 / inv-k0d.7)

- **Blob private DNS.** If the ACA env VNet can't resolve the account to its private endpoint IP,
  every S3 call fails with 500s from the gateway. Look at `blob_dns` in the job's run first.
- **The Delegator role and silent streaming.** versitygw signs the CopyObject source with a user
  delegation SAS. When it can't get a delegation key, it doesn't fail; it falls back to
  downloading and re-uploading through the gateway. Storage Blob Data Contributor at account
  scope includes `generateUserDelegationKey`. Scoped only to a container, it doesn't, so add
  Storage Blob Delegator on the account. Only the `--inside` copy layer can tell the two cases
  apart, by looking for `x-ms-copy-id` on the destination blob.
- **HTTP vs HTTPS.** Use `https://` with the ACA FQDN. Port 80 redirects, and S3 clients treat
  the redirect as an error. On the backend, server-side copy needs an HTTPS blob endpoint, which
  real Azure always has.
- **Metadata.** Azure metadata keys must be C# identifiers, so `date-created` is rejected
  (`InvalidMetadata`). Use `date_created`. Keys also come back with their first letter
  capitalised, so compare them case-insensitively.
- **No multipart copy.** versitygw's Azure backend doesn't implement UploadPartCopy, so
  `boto3 s3.copy()` on objects over the multipart threshold fails. Use `copy_object`.
- **The 240 s ingress timeout.** Keep each request well under it. With the default 32 MiB parts
  that's easy, even at 1 MiB/s.
- **Gateway memory.** versitygw buffers each uploaded part in RAM. Gateway memory needed is
  roughly part size x concurrent parts across all clients.

## Verified vs. not verified

Verified (2026-10-29, offline):

- az syntax: every subcommand and flag in `deploy.sh` checked against `az --help` from az CLI
  2.90.0 (core modules, no extensions). `deploy.sh --dry-run` reviewed; the create path, the
  update path, `--assign-roles` and `--checks-only` were exercised against a fake `az` with
  bash 3.2. shellcheck is clean.
- versitygw config: flags, env vars and the image entrypoint checked in the v1.10.0 source. The
  env-only configuration (`VGW_BACKEND=azure`, no args) was run against Azurite, and the doctor
  passed.
- Docs: ACA ingress external vs internal (ingress-overview); private DNS for internal
  environments (private-endpoints-with-dns); the ACA managed identity endpoint (managed-identity);
  built-in role actions (built-in-roles/storage).
- Doctor: green against the devstack compose, from the host and as the container on the compose
  network. `--inside` was green against Azurite with OAuth and a mock identity endpoint, and it
  flagged a forced copy fallback as "NOT server-side". The failure paths `dns`, `tcp`, `tls`,
  `health` and `s3_auth` each failed with the right layer.

Only verifiable on Azure:

- That the az commands succeed against our subscription and its policies: ACR import from
  Docker Hub, whether ACR Tasks (`az acr build`) are allowed, and env and job creation in the
  real environment. The `containerapp env show` field names (`vnetConfiguration.internal`,
  `staticIp`, `defaultDomain`) come from the ARM schema and weren't checked against a live env.
- Managed identity -> versitygw -> Blob (DefaultAzureCredential with `AZURE_CLIENT_ID`), and
  the user-delegation SAS on a real account.
- That ACA internal ingress forwards the S3 path, host and SigV4 headers unchanged; that the
  240 s timeout behaves as idle or total; and the body-size limit for 5 GB.
- citynet DNS and routing to the env's internal load balancer, the TLS cert the env serves,
  and any proxy on citynet.
- The private endpoint and DNS for the storage account, as seen from inside the env.
- The container name used by `az containerapp job logs show` (assumed to be the job name, the
  CLI default).
- Real throughput, 5 GB multipart, and the gateway's memory under real load.

If ACR Tasks are blocked, build and push the doctor image by hand:
`docker build -t <acr>.azurecr.io/gateway-doctor:<tag> apps/blob-gateway/doctor && az acr login -n <acr> && docker push ...`,
then run with `DOCTOR_IMAGE=gateway-doctor:<tag>`.

## Local (devstack)

```sh
docker compose -f apps/devstack/docker-compose.yml up -d --wait azurite versitygw
uv run apps/blob-gateway/doctor/doctor.py --endpoint http://127.0.0.1:7070 --access-key devstack --secret-key devstack-secret
docker compose -f apps/devstack/docker-compose.yml --profile doctor run --rm doctor          # same, as the container
docker compose -f apps/devstack/docker-compose.yml --profile doctor down -v                  # Azurite keeps the disk until -v
```
