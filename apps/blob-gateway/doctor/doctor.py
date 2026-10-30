# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3>=1.34"]
# ///
"""Gateway doctor: layered checks for the S3 gateway (versitygw) in front of Azure Blob.

Each layer tests one more network hop or feature, so the first failing layer points at
the broken piece:

  dns -> tcp -> tls -> health -> [inside: blob_dns -> mi_token -> blob_direct]
      -> s3_auth -> small_rw -> multipart -> large -> copy -> many_small

Runs anywhere with `uv run doctor.py ...` (boto3 + stdlib only). Stops at the first
failing layer unless --keep-going. Everything it writes lives under doctor/<run-id>/ in
the bucket and is deleted at the end (the bucket too, if the doctor created it).

  uv run doctor.py --endpoint https://blob-gateway.<env-domain> --access-key ... --secret-key ...
  uv run doctor.py --endpoint http://127.0.0.1:7070 --access-key devstack --secret-key devstack-secret
  uv run doctor.py ... --size 5G --json > doctor.json

Exit code: 0 if no layer failed (warnings allowed), 1 if any layer failed, 2 for usage errors.
Tracker: inv-k0d.14.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import http.client
import ipaddress
import json
import os
import platform
import re
import socket
import ssl
import sys
import threading
import time
import traceback
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION = "1"
MiB = 1024 * 1024
ACA_INGRESS_TIMEOUT_S = 240  # ACA HTTP ingress request timeout (inv-k0d.7)
AZ_STORAGE_VERSION = "2023-11-03"

LAYERS = [
    "dns",
    "tcp",
    "tls",
    "health",
    "blob_dns",
    "mi_token",
    "blob_direct",
    "s3_auth",
    "small_rw",
    "multipart",
    "large",
    "copy",
    "many_small",
]
INSIDE_LAYERS = {"blob_dns", "mi_token", "blob_direct"}

# What a failure at each layer usually means. RUNBOOK.md has the long version.
HINTS = {
    "dns": "Gateway hostname doesn't resolve. On citynet: the ACA environment's private DNS "
    "zone (<env default domain>, wildcard A -> env static IP) isn't visible to this machine's "
    "DNS. Inside ACA: wrong FQDN.",
    "tcp": "Resolves but can't connect. Firewall/NSG/route between here and the ACA env's "
    "internal load balancer, or wrong port (ACA ingress serves 443/80, not the app's 7070).",
    "tls": "TLS handshake/verification failed. Wrong hostname (cert is for *.<env domain>), "
    "TLS interception on the path (pass --ca-bundle), or an http:// endpoint on a TLS port.",
    "health": "Ingress answered but the gateway's /health didn't return 200. 404 from ACA "
    "usually means app ingress is 'internal' (env-only) or the FQDN is wrong; 503 means no "
    "healthy replica (check app logs: bad AZ_* settings crash versitygw at startup).",
    "blob_dns": "Storage account DNS doesn't resolve to a private IP. The blob private "
    "endpoint is missing, or the privatelink.blob.core.windows.net zone isn't linked to the "
    "ACA env's VNet, so traffic goes to the public endpoint (blocked if public access is off).",
    "mi_token": "No managed-identity token. The identity isn't assigned to this app/job, "
    "AZURE_CLIENT_ID is wrong, or (outside ACA) there's no identity endpoint at all.",
    "blob_direct": "Token works but Blob refused or wasn't reachable. 403 "
    "AuthorizationPermissionMismatch = missing Storage Blob Data Contributor; failing user "
    "delegation key = server-side copy will silently stream through the gateway (needs "
    "Contributor at account scope or Storage Blob Delegator). Timeouts = network path to blob.",
    "s3_auth": "Gateway reachable but S3 auth or bucket setup failed. InvalidAccessKeyId / "
    "SignatureDoesNotMatch = wrong keys, or a proxy rewrote the path/host (SigV4 signs both); "
    "if HTTPS_PROXY is set, add the gateway domain to NO_PROXY. "
    "500s here usually mean the gateway can't reach or authenticate to Blob.",
    "small_rw": "Basic PUT/GET/DELETE failed. Look at the error code; InvalidMetadata means a "
    "metadata key isn't a valid C# identifier (no hyphens) for Azure.",
    "multipart": "Multipart mechanics failed (create/upload part/complete/abort).",
    "large": "Large transfer failed or was slow. Timeouts near 240 s = a part took longer than "
    "the ACA ingress timeout: lower --part-size. 413 = a body size limit on the path. "
    "Gateway OOM (502/503 mid-run) = part size x concurrency too big for gateway memory.",
    "copy": "CopyObject failed. Objects over VGW_COPY_OBJECT_THRESHOLD (5 GiB default) are "
    "rejected; multipart copy (UploadPartCopy) isn't supported by versitygw's Azure backend.",
    "many_small": "Many-small-object run failed: per-request overhead or throttling.",
}


# --------------------------------------------------------------------------- helpers


def parse_size(text: str) -> int:
    """'100MB', '5G', '64MiB', '1048576' -> bytes. K/M/G/T are binary (1024) multiples."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?)(?:i?b)?\s*", text, re.IGNORECASE)
    if not m:
        raise argparse.ArgumentTypeError(f"bad size: {text!r} (try 100MB, 5G, 64MiB)")
    mult = {"": 1, "k": 1024, "m": MiB, "g": 1024**3, "t": 1024**4}[m.group(2).lower()]
    return int(float(m.group(1)) * mult)


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n} B"


def mbps(nbytes: int, seconds: float) -> float:
    return round(nbytes / MiB / seconds, 1) if seconds > 0 else 0.0


def err_text(e: BaseException) -> str:
    code = (
        getattr(e, "response", {}).get("Error", {}).get("Code")
        if hasattr(e, "response")
        else None
    )
    status = (
        getattr(e, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")
        if hasattr(e, "response")
        else None
    )
    base = f"{type(e).__name__}: {e}"
    if code or status:
        base = f"[{status} {code}] {base}"
    return base[:600]


class LayerFail(Exception):
    """Raised inside a layer to fail it with a message (and optional details)."""

    def __init__(self, msg: str, **details):
        super().__init__(msg)
        self.details = details


class LayerSkip(Exception):
    pass


class Doctor:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.url = urllib.parse.urlsplit(args.endpoint)
        if self.url.scheme not in ("http", "https") or not self.url.hostname:
            raise SystemExit(
                f"--endpoint must be http(s)://host[:port], got {args.endpoint!r}"
            )
        self.host = self.url.hostname
        self.port = self.url.port or (443 if self.url.scheme == "https" else 80)
        self.run_id = f"{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self.prefix = f"doctor/{self.run_id}/"
        self.results: list[dict] = []
        self.created_bucket = False
        self.s3_ready = False  # set once auth + bucket work; gates cleanup
        self.big_key: str | None = None
        self.big_size = 0
        self.big_sha: str | None = None
        self.copy_dest: str | None = None
        self._s3 = None
        self._mi_token: str | None = None

    # ------------------------------------------------------------------ plumbing

    def log(self, msg: str) -> None:
        print(msg, file=sys.stderr, flush=True)

    def ssl_context(self) -> ssl.SSLContext:
        if self.args.insecure:
            ctx = ssl._create_unverified_context()
        else:
            ctx = ssl.create_default_context(cafile=self.args.ca_bundle)
        return ctx

    @property
    def s3(self):
        if self._s3 is None:
            import boto3
            from botocore.config import Config

            verify: bool | str = True
            if self.args.insecure:
                verify = False
            elif self.args.ca_bundle:
                verify = self.args.ca_bundle
            self._s3 = boto3.client(
                "s3",
                endpoint_url=self.args.endpoint,
                aws_access_key_id=self.args.access_key,
                aws_secret_access_key=self.args.secret_key,
                region_name=self.args.region,
                verify=verify,
                config=Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                    retries={"max_attempts": self.args.retries, "mode": "standard"},
                    connect_timeout=10,
                    read_timeout=ACA_INGRESS_TIMEOUT_S + 60,
                    max_pool_connections=max(
                        self.args.concurrency, self.args.small_threads
                    )
                    + 4,
                ),
            )
        return self._s3

    def run(self) -> int:
        selected = [
            name
            for name in LAYERS
            if (self.args.inside or name not in INSIDE_LAYERS)
            and name not in self.args.skip
        ]
        stopped_at = None
        try:
            for name in LAYERS:
                if name not in selected:
                    reason = (
                        "--skip"
                        if name in self.args.skip
                        else "outside-only run (no --inside)"
                    )
                    self.results.append(
                        {"layer": name, "status": "skip", "detail": reason}
                    )
                    continue
                if stopped_at is not None:
                    self.results.append(
                        {
                            "layer": name,
                            "status": "not_run",
                            "detail": f"stopped at {stopped_at}",
                        }
                    )
                    continue
                res = self.run_layer(name)
                if res["status"] == "fail" and not self.args.keep_going:
                    stopped_at = name
        finally:
            self.cleanup()
        return 1 if any(r["status"] == "fail" for r in self.results) else 0

    def run_layer(self, name: str) -> dict:
        self.log(f"--> {name}")
        t0 = time.monotonic()
        res: dict = {"layer": name}
        try:
            out = getattr(self, f"layer_{name}")() or {}
            res["status"] = out.pop("_status", "pass")
            res.update(out)
        except LayerSkip as e:
            res.update(status="skip", detail=str(e))
        except LayerFail as e:
            res.update(status="fail", error=str(e), **e.details)
        except Exception as e:  # noqa: BLE001 - every unexpected error is a layer failure
            res.update(status="fail", error=err_text(e))
            if self.args.verbose:
                traceback.print_exc()
        res["seconds"] = round(time.monotonic() - t0, 2)
        if res["status"] == "fail":
            res["hint"] = HINTS[name]
        self.results.append(res)
        tag = {"pass": "PASS", "warn": "WARN", "fail": "FAIL", "skip": "SKIP"}[
            res["status"]
        ]
        self.log(
            f"    {tag} {name} ({res['seconds']}s) {res.get('error') or res.get('detail') or ''}"
        )
        return res

    # ------------------------------------------------------------------ network layers

    def layer_dns(self):
        try:
            canon, aliases, ips = socket.gethostbyname_ex(self.host)
        except socket.gaierror as e:
            raise LayerFail(f"cannot resolve {self.host}: {e}") from e
        private = [ip for ip in ips if ipaddress.ip_address(ip).is_private]
        out = {"host": self.host, "ips": ips, "aliases": aliases, "canonical": canon}
        out["detail"] = f"{self.host} -> {', '.join(ips)}"
        if self.args.expect_private and not private:
            out["_status"] = "warn"
            out["detail"] += " (public IP; expected a private VNet address)"
        return out

    def layer_tcp(self):
        t0 = time.monotonic()
        try:
            with socket.create_connection((self.host, self.port), timeout=10):
                pass
        except OSError as e:
            raise LayerFail(f"TCP connect {self.host}:{self.port} failed: {e}") from e
        ms = round((time.monotonic() - t0) * 1000, 1)
        return {"detail": f"{self.host}:{self.port} connect {ms} ms", "connect_ms": ms}

    def layer_tls(self):
        if self.url.scheme != "https":
            return {
                "_status": "warn",
                "detail": "plain http endpoint, TLS not tested. Fine for devstack; on ACA use "
                "https:// (port 80 answers with a redirect, which S3 clients don't follow).",
            }
        ctx = self.ssl_context()
        try:
            with socket.create_connection((self.host, self.port), timeout=10) as sock:
                with ctx.wrap_socket(sock, server_hostname=self.host) as tls:
                    cert = tls.getpeercert() or {}
                    version = tls.version()
        except ssl.SSLCertVerificationError as e:
            raise LayerFail(
                f"certificate verification failed: {e.verify_message or e}"
            ) from e
        except (OSError, ssl.SSLError) as e:
            raise LayerFail(f"TLS handshake failed: {e}") from e

        def name_of(field):
            return ", ".join("=".join(x) for rdn in cert.get(field, ()) for x in rdn)

        out = {
            "tls_version": version,
            "subject": name_of("subject"),
            "issuer": name_of("issuer"),
            "not_after": cert.get("notAfter"),
            "san": [v for k, v in cert.get("subjectAltName", ()) if k == "DNS"][:10],
        }
        out["detail"] = (
            f"{version}, issuer {out['issuer'] or '?'}, expires {out['not_after']}"
        )
        if self.args.insecure:
            out["_status"] = "warn"
            out["detail"] += " (--insecure: not verified)"
        return out

    def http_get(
        self,
        host,
        port,
        scheme,
        path,
        headers=None,
        method="GET",
        body=None,
        timeout=15,
    ):
        if scheme == "https":
            conn = http.client.HTTPSConnection(
                host, port, timeout=timeout, context=self.ssl_context()
            )
        else:
            conn = http.client.HTTPConnection(host, port, timeout=timeout)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            resp = conn.getresponse()
            data = resp.read(64 * 1024)
            return resp.status, dict(resp.getheaders()), data
        finally:
            conn.close()

    def layer_health(self):
        path = self.args.health_path
        try:
            status, headers, body = self.http_get(
                self.host, self.port, self.url.scheme, path
            )
        except OSError as e:
            raise LayerFail(f"GET {path} failed: {e}") from e
        out = {
            "http_status": status,
            "server": headers.get("Server") or headers.get("server"),
        }
        if status in (301, 302, 307, 308):
            raise LayerFail(
                f"GET {path} -> {status} redirect to {headers.get('Location')}: use the https:// URL",
                **out,
            )
        if status != 200:
            raise LayerFail(f"GET {path} -> HTTP {status}: {body[:200]!r}", **out)
        out["detail"] = f"GET {path} -> 200"
        return out

    # ------------------------------------------------------------------ inside-the-VNet layers

    @property
    def blob_host(self) -> str:
        if self.args.blob_endpoint:
            return urllib.parse.urlsplit(self.args.blob_endpoint).hostname or ""
        if not self.args.storage_account:
            raise LayerFail("--inside needs --storage-account (or AZ_ACCOUNT_NAME)")
        return f"{self.args.storage_account}.blob.core.windows.net"

    def layer_blob_dns(self):
        host = self.blob_host
        try:
            canon, aliases, ips = socket.gethostbyname_ex(host)
        except socket.gaierror as e:
            raise LayerFail(f"cannot resolve {host}: {e}") from e
        names = [canon, *aliases]
        privatelink = any("privatelink" in n for n in names)
        private = all(ipaddress.ip_address(ip).is_private for ip in ips)
        out = {
            "host": host,
            "ips": ips,
            "cname_chain": names,
            "privatelink_cname": privatelink,
        }
        if not private:
            raise LayerFail(
                f"{host} -> {', '.join(ips)} is public: no private endpoint DNS in this VNet",
                **out,
            )
        out["detail"] = f"{host} -> {', '.join(ips)} (private)"
        if not privatelink:
            out["_status"] = "warn"
            out["detail"] += ", no privatelink CNAME seen (custom DNS or emulator?)"
        return out

    def layer_mi_token(self):
        resource = "https://storage.azure.com/"
        client_id = self.args.client_id
        ep, hdr = os.environ.get("IDENTITY_ENDPOINT"), os.environ.get("IDENTITY_HEADER")
        if ep and hdr:  # Container Apps / App Service managed identity endpoint
            q = {"resource": resource, "api-version": "2019-08-01"}
            if client_id:
                q["client_id"] = client_id
            u = urllib.parse.urlsplit(ep)
            path = f"{u.path or '/'}?{urllib.parse.urlencode(q)}"
            headers = {"X-IDENTITY-HEADER": hdr}
            source = "IDENTITY_ENDPOINT"
        else:  # VM-style IMDS
            u = urllib.parse.urlsplit(
                "http://169.254.169.254/metadata/identity/oauth2/token"
            )
            q = {"resource": resource, "api-version": "2018-02-01"}
            if client_id:
                q["client_id"] = client_id
            path = f"{u.path}?{urllib.parse.urlencode(q)}"
            headers = {"Metadata": "true"}
            source = "IMDS"
        try:
            status, _, body = self.http_get(
                u.hostname, u.port or (443 if u.scheme == "https" else 80), u.scheme, path,
                headers=headers, timeout=5,
            )  # fmt: skip
        except OSError as e:
            raise LayerFail(
                f"{source} unreachable ({e}); not running with a managed identity?"
            ) from e
        if status != 200:
            raise LayerFail(f"{source} -> HTTP {status}: {body[:300]!r}", source=source)
        token = json.loads(body)["access_token"]
        self._mi_token = token
        claims = {}
        try:
            payload = token.split(".")[1]
            claims = json.loads(
                base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
            )
        except Exception:  # noqa: BLE001 - claims are informational only
            pass
        out = {
            "source": source,
            "client_id": claims.get("appid") or claims.get("azp"),
            "object_id": claims.get("oid"),
            "identity_resource": claims.get("xms_mirid"),
        }
        out["detail"] = f"token from {source} for oid {out['object_id']}"
        return out

    def blob_request(self, method, path, body=None, extra_headers=None):
        if not self._mi_token:
            raise LayerSkip(
                "no managed-identity token (mi_token layer failed or skipped)"
            )
        if self.args.blob_endpoint:
            u = urllib.parse.urlsplit(self.args.blob_endpoint)
            scheme, host, port, base = u.scheme, u.hostname, u.port, u.path.rstrip("/")
        else:
            scheme, host, port, base = "https", self.blob_host, None, ""
        headers = {
            "Authorization": f"Bearer {self._mi_token}",
            "x-ms-version": AZ_STORAGE_VERSION,
            "x-ms-date": dt.datetime.now(dt.UTC).strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }
        headers.update(extra_headers or {})
        port = port or (443 if scheme == "https" else 80)
        return self.http_get(
            host, port, scheme, base + path, headers=headers, method=method, body=body
        )

    def layer_blob_direct(self):
        out: dict = {}
        bucket = self.args.bucket
        status, _, body = self.blob_request("GET", f"/{bucket}?restype=container")
        out["container_status"] = status
        if status == 404:
            out["container"] = "missing (the gateway will create it in s3_auth)"
        elif status != 200:
            raise LayerFail(
                f"GET container {bucket} -> {status}: {body[:300]!r}", **out
            )
        start = dt.datetime.now(dt.UTC).replace(microsecond=0)
        xml = (
            "<?xml version='1.0' encoding='utf-8'?><KeyInfo>"
            f"<Start>{start:%Y-%m-%dT%H:%M:%SZ}</Start>"
            f"<Expiry>{start + dt.timedelta(minutes=15):%Y-%m-%dT%H:%M:%SZ}</Expiry></KeyInfo>"
        ).encode()
        status, _, body = self.blob_request(
            "POST", "/?restype=service&comp=userdelegationkey", body=xml,
            extra_headers={"Content-Type": "application/xml", "Content-Length": str(len(xml))},
        )  # fmt: skip
        out["user_delegation_key_status"] = status
        if status != 200:
            raise LayerFail(
                f"user delegation key -> {status}: {body[:300]!r}. Server-side copy would fall "
                "back to streaming through the gateway.",
                **out,
            )
        out["detail"] = (
            f"container {bucket}: {status if out['container_status'] == 200 else 'missing'}; user delegation key OK"
        )
        return out

    # ------------------------------------------------------------------ S3 layers

    def layer_s3_auth(self):
        resp = self.s3.list_buckets()
        names = [b["Name"] for b in resp.get("Buckets", [])]
        bucket = self.args.bucket
        out = {"bucket_count": len(names)}
        if bucket not in names:
            try:
                self.s3.head_bucket(Bucket=bucket)
            except Exception:  # noqa: BLE001 - absent or not listed: try to create it
                self.s3.create_bucket(Bucket=bucket)
                self.created_bucket = True
        self.s3_ready = True
        out["created_bucket"] = self.created_bucket
        out["detail"] = f"ListBuckets ok ({len(names)}), bucket {bucket} " + (
            "created" if self.created_bucket else "exists"
        )
        return out

    def key(self, name: str) -> str:
        return self.prefix + name

    def layer_small_rw(self):
        body = os.urandom(1024)
        key = self.key("small.bin")
        self.s3.put_object(
            Bucket=self.args.bucket,
            Key=key,
            Body=body,
            Metadata={"doctor_run": self.run_id},
        )
        got = self.s3.get_object(Bucket=self.args.bucket, Key=key)
        data = got["Body"].read()
        if hashlib.sha256(data).digest() != hashlib.sha256(body).digest():
            raise LayerFail("GET returned different bytes than PUT")
        meta = {k.lower(): v for k, v in got.get("Metadata", {}).items()}
        out: dict = {"metadata_returned": got.get("Metadata", {})}
        if meta.get("doctor_run") != self.run_id:
            out["_status"] = "warn"
            out["detail"] = f"metadata didn't round-trip: {got.get('Metadata')}"
        elif "doctor_run" not in got.get("Metadata", {}):
            out["detail"] = (
                "PUT/GET/DELETE ok; metadata key case changed (expected with versitygw on Azure)"
            )
        else:
            out["detail"] = "PUT/GET/DELETE ok"
        self.s3.delete_object(Bucket=self.args.bucket, Key=key)
        try:
            self.s3.head_object(Bucket=self.args.bucket, Key=key)
            raise LayerFail("object still there after DELETE")
        except LayerFail:
            raise
        except Exception as e:  # noqa: BLE001
            if "404" not in str(e) and "Not Found" not in str(e):
                raise
        return out

    def layer_multipart(self):
        b, key = self.args.bucket, self.key("multipart.bin")
        part = 5 * MiB  # S3 minimum part size
        parts_data = [os.urandom(part), os.urandom(part), os.urandom(1 * MiB)]
        mpu = self.s3.create_multipart_upload(Bucket=b, Key=key)
        uid = mpu["UploadId"]
        try:
            parts = []
            for i, data in enumerate(parts_data, start=1):
                r = self.s3.upload_part(
                    Bucket=b, Key=key, UploadId=uid, PartNumber=i, Body=data
                )
                parts.append({"PartNumber": i, "ETag": r["ETag"]})
            listed = self.s3.list_parts(Bucket=b, Key=key, UploadId=uid).get(
                "Parts", []
            )
            if len(listed) != len(parts):
                raise LayerFail(
                    f"ListParts returned {len(listed)} parts, uploaded {len(parts)}"
                )
            self.s3.complete_multipart_upload(
                Bucket=b, Key=key, UploadId=uid, MultipartUpload={"Parts": parts}
            )
        except Exception:
            self.s3.abort_multipart_upload(Bucket=b, Key=key, UploadId=uid)
            raise
        got = self.s3.get_object(Bucket=b, Key=key)["Body"].read()
        if (
            hashlib.sha256(got).hexdigest()
            != hashlib.sha256(b"".join(parts_data)).hexdigest()
        ):
            raise LayerFail("multipart object bytes don't match what was uploaded")
        # Abort path: start an upload, stage one part, abort, check it's gone.
        akey = self.key("aborted.bin")
        uid2 = self.s3.create_multipart_upload(Bucket=b, Key=akey)["UploadId"]
        self.s3.upload_part(
            Bucket=b, Key=akey, UploadId=uid2, PartNumber=1, Body=parts_data[2]
        )
        self.s3.abort_multipart_upload(Bucket=b, Key=akey, UploadId=uid2)
        open_uploads = self.s3.list_multipart_uploads(Bucket=b, Prefix=akey).get(
            "Uploads", []
        )
        out = {
            "detail": "3-part upload + ListParts + complete + abort ok, sha256 match"
        }
        if open_uploads:
            out["_status"] = "warn"
            out["detail"] += f"; aborted upload still listed ({len(open_uploads)})"
        return out

    def layer_large(self):
        b, key = self.args.bucket, self.key("large.bin")
        size, part = self.args.size, self.args.part_size
        nparts = max(1, -(-size // part))
        if nparts > 10000:
            raise LayerFail(f"{nparts} parts > S3 max 10000: raise --part-size")
        sha = hashlib.sha256()
        part_secs: list[float] = []
        retries = 0
        lock = threading.Lock()
        slots = threading.BoundedSemaphore(self.args.concurrency)

        def put_part(i, data):
            t = time.monotonic()
            try:
                r = self.s3.upload_part(
                    Bucket=b, Key=key, UploadId=uid, PartNumber=i, Body=data
                )
            finally:
                slots.release()
            secs = time.monotonic() - t
            with lock:
                nonlocal retries
                part_secs.append(secs)
                retries += r["ResponseMetadata"].get("RetryAttempts", 0)
            return {"PartNumber": i, "ETag": r["ETag"]}

        self.log(
            f"    uploading {human(size)} in {nparts} x {human(part)} parts, {self.args.concurrency} at a time"
        )
        t0 = time.monotonic()
        uid = self.s3.create_multipart_upload(Bucket=b, Key=key)["UploadId"]
        try:
            with ThreadPoolExecutor(self.args.concurrency) as pool:
                futures = []
                for i in range(1, nparts + 1):
                    slots.acquire()  # bounds memory: at most `concurrency` parts in flight
                    n = min(part, size - (i - 1) * part)
                    data = os.urandom(n)
                    sha.update(data)
                    futures.append(pool.submit(put_part, i, data))
                    del data
                parts = sorted(
                    (f.result() for f in as_completed(futures)),
                    key=lambda p: p["PartNumber"],
                )
            self.s3.complete_multipart_upload(
                Bucket=b, Key=key, UploadId=uid, MultipartUpload={"Parts": parts}
            )
        except Exception:
            try:
                self.s3.abort_multipart_upload(Bucket=b, Key=key, UploadId=uid)
            except Exception:  # noqa: BLE001
                pass
            raise
        up_s = time.monotonic() - t0
        self.big_key, self.big_size, self.big_sha = key, size, sha.hexdigest()

        self.log("    downloading + hashing")
        t0 = time.monotonic()
        resp = self.s3.get_object(Bucket=b, Key=key)
        ttfb = time.monotonic() - t0
        dsha, got = hashlib.sha256(), 0
        for chunk in resp["Body"].iter_chunks(8 * MiB):
            dsha.update(chunk)
            got += len(chunk)
        down_s = time.monotonic() - t0
        out = {
            "size": size,
            "part_size": part,
            "parts": nparts,
            "concurrency": self.args.concurrency,
            "upload_s": round(up_s, 1),
            "upload_MiBps": mbps(size, up_s),
            "download_s": round(down_s, 1),
            "download_MiBps": mbps(size, down_s),
            "download_ttfb_s": round(ttfb, 2),
            "part_s_max": round(max(part_secs), 1),
            "part_s_median": round(sorted(part_secs)[len(part_secs) // 2], 1),
            "retries": retries,
            "sha256": self.big_sha,
        }
        if got != size or dsha.hexdigest() != self.big_sha:
            raise LayerFail(
                f"download mismatch: {got} bytes, sha {dsha.hexdigest()} != {self.big_sha}",
                **out,
            )
        out["detail"] = (
            f"{human(size)} up {out['upload_MiBps']} MiB/s, down {out['download_MiBps']} MiB/s, "
            f"slowest part {out['part_s_max']}s, sha256 ok"
        )
        if out["part_s_max"] > ACA_INGRESS_TIMEOUT_S / 2 or retries:
            out["_status"] = "warn"
            out["detail"] += (
                f"; slowest part near the {ACA_INGRESS_TIMEOUT_S}s ingress timeout or retried ({retries})"
            )
        return out

    def layer_copy(self):
        b = self.args.bucket
        if self.big_key:
            src, size, sha = self.big_key, self.big_size, self.big_sha
        else:  # large skipped: copy a fresh 8 MiB object instead
            data = os.urandom(8 * MiB)
            src, size, sha = (
                self.key("copy-src.bin"),
                len(data),
                hashlib.sha256(data).hexdigest(),
            )
            self.s3.put_object(Bucket=b, Key=src, Body=data)
        dst = self.key("copy-dst.bin")
        t0 = time.monotonic()
        self.s3.copy_object(Bucket=b, Key=dst, CopySource={"Bucket": b, "Key": src})
        secs = time.monotonic() - t0
        self.copy_dest = dst
        head = self.s3.head_object(Bucket=b, Key=dst)
        out: dict = {
            "source_size": size,
            "copy_s": round(secs, 2),
            "copy_apparent_MiBps": mbps(size, secs),
        }
        if head["ContentLength"] != size:
            raise LayerFail(
                f"copy has {head['ContentLength']} bytes, source {size}", **out
            )
        if size <= self.args.copy_verify_max:
            dsha = hashlib.sha256()
            for chunk in self.s3.get_object(Bucket=b, Key=dst)["Body"].iter_chunks(
                8 * MiB
            ):
                dsha.update(chunk)
            if dsha.hexdigest() != sha:
                raise LayerFail("copied bytes don't match source", **out)
            out["sha256_verified"] = True
        out["detail"] = f"CopyObject {human(size)} in {out['copy_s']}s"
        # Was it a real Azure server-side copy? Only answerable with direct blob access.
        if self.args.inside and self._mi_token:
            path = "/" + b + "/" + urllib.parse.quote(dst)
            status, headers, _ = self.blob_request("HEAD", path)
            lower = {k.lower(): v for k, v in headers.items()}
            out["blob_copy_status"] = lower.get("x-ms-copy-status")
            if status == 200 and lower.get("x-ms-copy-id"):
                out["server_side"] = True
                out["detail"] += (
                    ", server-side (x-ms-copy-status "
                    + str(lower.get("x-ms-copy-status"))
                    + ")"
                )
            elif status == 200:
                out["server_side"] = False
                raise LayerFail(
                    "CopyObject worked but was NOT server-side: the bytes streamed through the "
                    "gateway (destination blob has no x-ms-copy-id). Usually the gateway identity "
                    "can't get a user delegation key: Storage Blob Data Contributor must be at "
                    "account scope, or add Storage Blob Delegator on the account.",
                    **out,
                )
        else:
            out["detail"] += (
                " (server-side vs streamed not checked: needs --inside + managed identity)"
            )
        return out

    def layer_many_small(self):
        b, n, sz = self.args.bucket, self.args.small_count, self.args.small_size
        bodies = {self.key(f"small/{i:05d}.bin"): os.urandom(sz) for i in range(n)}

        def put(k):
            self.s3.put_object(Bucket=b, Key=k, Body=bodies[k])

        def get(k):
            data = self.s3.get_object(Bucket=b, Key=k)["Body"].read()
            return hashlib.sha256(data).digest() == hashlib.sha256(bodies[k]).digest()

        with ThreadPoolExecutor(self.args.small_threads) as pool:
            t0 = time.monotonic()
            list(pool.map(put, bodies))
            up_s = time.monotonic() - t0
            listed = 0
            for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=b, Prefix=self.key("small/")
            ):
                listed += len(page.get("Contents", []))
            t0 = time.monotonic()
            ok = list(pool.map(get, bodies))
            down_s = time.monotonic() - t0
        out = {
            "count": n,
            "size": sz,
            "threads": self.args.small_threads,
            "put_s": round(up_s, 2),
            "get_s": round(down_s, 2),
            "put_ms_each": round(up_s * 1000 * self.args.small_threads / n, 1),
            "listed": listed,
        }
        if listed != n:
            raise LayerFail(f"listed {listed} objects, wrote {n}", **out)
        if not all(ok):
            raise LayerFail(
                f"{ok.count(False)} objects came back with different bytes", **out
            )
        out["detail"] = (
            f"{n} x {human(sz)}: put {out['put_s']}s, get {out['get_s']}s, list ok, sha256 ok"
        )
        return out

    # ------------------------------------------------------------------ cleanup / report

    def cleanup(self):
        if self.args.keep or not self.s3_ready:
            if self.args.keep:
                self.log(f"--keep: leaving s3://{self.args.bucket}/{self.prefix}")
            return
        b = self.args.bucket
        deleted = 0
        try:
            for up in self.s3.list_multipart_uploads(Bucket=b, Prefix=self.prefix).get(
                "Uploads", []
            ):
                self.s3.abort_multipart_upload(
                    Bucket=b, Key=up["Key"], UploadId=up["UploadId"]
                )
            for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=b, Prefix=self.prefix
            ):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                for i in range(0, len(keys), 1000):
                    self.s3.delete_objects(
                        Bucket=b, Delete={"Objects": keys[i : i + 1000], "Quiet": True}
                    )
                    deleted += len(keys[i : i + 1000])
            if self.created_bucket:
                self.s3.delete_bucket(Bucket=b)
            self.log(
                f"cleanup: deleted {deleted} objects"
                + (f" and bucket {b}" if self.created_bucket else "")
            )
        except Exception as e:  # noqa: BLE001 - cleanup problems are reported, not fatal
            self.log(
                f"cleanup problem (objects may remain under {b}/{self.prefix}): {err_text(e)}"
            )
            self.results.append(
                {"layer": "cleanup", "status": "warn", "detail": err_text(e)}
            )

    def report(self, code: int) -> dict:
        first_fail = next(
            (r["layer"] for r in self.results if r["status"] == "fail"), None
        )
        return {
            "doctor": VERSION,
            "run_id": self.run_id,
            "result": "fail" if code else "pass",
            "first_failure": first_fail,
            "endpoint": self.args.endpoint,
            "bucket": self.args.bucket,
            "mode": "inside" if self.args.inside else "outside",
            "client": {
                "hostname": socket.gethostname(),
                "python": platform.python_version(),
                "boto3": __import__("boto3").__version__,
                # boto3 honours these; the raw socket layers (dns..health) don't.
                "proxy_env": {
                    k: v
                    for k, v in os.environ.items()
                    if k.lower()
                    in ("https_proxy", "http_proxy", "no_proxy", "all_proxy")
                },
            },
            "settings": {
                "size": self.args.size,
                "part_size": self.args.part_size,
                "concurrency": self.args.concurrency,
                "small_count": self.args.small_count,
            },
            "layers": self.results,
        }

    def summary(self, rep: dict) -> str:
        lines = [f"gateway doctor {rep['run_id']}  {rep['endpoint']}  ({rep['mode']})"]
        for r in rep["layers"]:
            if (
                r["status"] == "skip"
                and r["layer"] in INSIDE_LAYERS
                and not self.args.inside
            ):
                continue
            msg = r.get("error") or r.get("detail") or ""
            lines.append(f"  {r['status'].upper():8} {r['layer']:12} {msg}")
        if rep["first_failure"]:
            lines.append(
                f"FIRST FAILURE: {rep['first_failure']}: {HINTS[rep['first_failure']]}"
            )
        lines.append(f"RESULT: {rep['result'].upper()}")
        return "\n".join(lines)


def env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def build_parser() -> argparse.ArgumentParser:
    """Every option also reads an env var, so the ACA Job is configured with env vars only
    (az CLI mangles --args values that start with '--')."""
    env = os.environ.get
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add = p.add_argument
    add("--endpoint", default=env("GATEWAY_URL"), help="gateway URL [GATEWAY_URL]")
    add(
        "--access-key",
        default=env("GATEWAY_ACCESS_KEY") or env("AWS_ACCESS_KEY_ID"),
        help="[GATEWAY_ACCESS_KEY]",
    )
    add(
        "--secret-key",
        default=env("GATEWAY_SECRET_KEY") or env("AWS_SECRET_ACCESS_KEY"),
        help="[GATEWAY_SECRET_KEY]",
    )
    add(
        "--bucket",
        default=env("GATEWAY_BUCKET", "gateway-doctor"),
        help="bucket (= Azure container) to test in; created and removed if missing "
        "[GATEWAY_BUCKET]",
    )
    add("--region", default=env("GATEWAY_REGION", "us-east-1"))
    add("--health-path", default=env("GATEWAY_HEALTH_PATH", "/health"))
    add(
        "--ca-bundle",
        default=env("AWS_CA_BUNDLE"),
        help="PEM CA bundle [AWS_CA_BUNDLE]",
    )
    add("--insecure", action="store_true", help="don't verify TLS (diagnosis only)")
    add(
        "--expect-private",
        action="store_true",
        default=env_flag("DOCTOR_EXPECT_PRIVATE"),
        help="warn if the gateway resolves to a public IP [DOCTOR_EXPECT_PRIVATE]",
    )
    add(
        "--size",
        type=parse_size,
        default=env("DOCTOR_SIZE", "100MiB"),
        help="large-object size, default 100MiB; use 5G on Azure [DOCTOR_SIZE]",
    )
    add(
        "--part-size",
        type=parse_size,
        default=env("DOCTOR_PART_SIZE", "32MiB"),
        help="multipart part size, default 32MiB: each part must finish well inside the "
        "240 s ACA ingress timeout, and the gateway holds whole parts in memory "
        "[DOCTOR_PART_SIZE]",
    )
    add(
        "--concurrency",
        type=int,
        default=int(env("DOCTOR_CONCURRENCY", "4")),
        help="parts in flight, default 4 [DOCTOR_CONCURRENCY]",
    )
    add("--small-count", type=int, default=int(env("DOCTOR_SMALL_COUNT", "200")))
    add("--small-size", type=parse_size, default=env("DOCTOR_SMALL_SIZE", "16KiB"))
    add("--small-threads", type=int, default=16)
    add(
        "--copy-verify-max",
        type=parse_size,
        default="256MiB",
        help="re-download and sha256 the copy only if the source is at most this big",
    )
    add("--retries", type=int, default=3, help="botocore max_attempts")
    add(
        "--inside",
        action="store_true",
        default=env_flag("DOCTOR_INSIDE"),
        help="also check the gateway's backend path from inside the VNet: blob private "
        "DNS, managed identity token, direct blob access + user delegation key "
        "[DOCTOR_INSIDE]",
    )
    add("--storage-account", default=env("AZ_ACCOUNT_NAME"), help="[AZ_ACCOUNT_NAME]")
    add(
        "--blob-endpoint",
        default=env("DOCTOR_BLOB_ENDPOINT"),
        help="override https://<account>.blob.core.windows.net, e.g. Azurite "
        "[DOCTOR_BLOB_ENDPOINT]",
    )
    add(
        "--client-id",
        default=env("AZURE_CLIENT_ID"),
        help="user-assigned managed identity client id [AZURE_CLIENT_ID]",
    )
    add(
        "--skip",
        action="append",
        default=[x for x in env("DOCTOR_SKIP", "").replace(",", " ").split() if x],
        choices=LAYERS,
        metavar="LAYER",
        help=f"skip a layer, repeatable [DOCTOR_SKIP, comma list]: {', '.join(LAYERS)}",
    )
    add(
        "--keep-going",
        action="store_true",
        default=env_flag("DOCTOR_KEEP_GOING"),
        help="run every layer even after a failure [DOCTOR_KEEP_GOING]",
    )
    add("--keep", action="store_true", help="don't delete what the doctor wrote")
    add(
        "--json",
        action="store_true",
        default=env_flag("DOCTOR_JSON"),
        help="print the full JSON report on stdout, one line [DOCTOR_JSON]",
    )
    add("-v", "--verbose", action="store_true", help="tracebacks for failures")
    return p


def main(argv: list[str] | None = None) -> int:
    p = build_parser()
    args = p.parse_args(argv)
    if not args.endpoint:
        p.error("--endpoint (or GATEWAY_URL) is required")
    if not (args.access_key and args.secret_key):
        p.error(
            "--access-key/--secret-key (or GATEWAY_ACCESS_KEY/GATEWAY_SECRET_KEY) are required"
        )
    bad_skip = set(args.skip) - set(LAYERS)
    if bad_skip:
        p.error(f"unknown layer(s) in DOCTOR_SKIP: {sorted(bad_skip)}")
    if args.part_size < 5 * MiB:
        p.error("--part-size must be at least 5MiB (S3 minimum)")
    doc = Doctor(args)
    code = doc.run()
    rep = doc.report(code)
    print(doc.summary(rep), file=sys.stderr, flush=True)
    if args.json:
        print(json.dumps(rep, default=str), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
