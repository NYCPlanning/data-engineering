"""Compare S3 gateways in front of Azure Blob (Azurite) against a direct-to-Azurite baseline.

Runs the inv-k0d.12 PLAN checks against one target per invocation:
  (a) big-file multipart upload (boto3 TransferConfig defaults), then kill-and-resume
  (b) N small objects up/down, timed
  (c) sha256 round trip for everything
  (d) peak memory of the gateway container while streaming (docker stats)
  (e) throughput, compared with `--target azurite` (direct Azure SDK baseline)
  (f) dcpy PathedStorageConnector, s3 type, endpoint=gateway: push/pull/ls/metadata
  (g) server-side CopyObject (single) and managed multipart copy (UploadPartCopy),
      with the gateway's network bytes during the copy, to tell a real server-side
      copy from one streamed through the gateway

Usage (stack from apps/devstack/docker-compose.yml running, host venv):
  python apps/devstack/gateway_bench/bench.py --target versitygw
  python apps/devstack/gateway_bench/bench.py --target s3proxy
  python apps/devstack/gateway_bench/bench.py --target azurite

Everything it writes (temp files, blobs, the bucket/container) is deleted at the end.
"""

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

TARGETS = {
    "versitygw": {"endpoint": "http://127.0.0.1:7070", "container": "devstack-versitygw-1"},
    "s3proxy": {"endpoint": "http://127.0.0.1:7071", "container": "devstack-s3proxy-1"},
    "azurite": {"endpoint": None, "container": "devstack-azurite-1"},
}
S3_KEY, S3_SECRET = "devstack", "devstack-secret"
AZ_CONN = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=https://127.0.0.1:10000/devstoreaccount1;"
)
AZURITE_CA = str(Path(__file__).resolve().parent.parent / ".certs" / "azurite.pem")
PART = 8 * 1024 * 1024  # boto3 TransferConfig default multipart_chunksize
MB = 1024 * 1024


def s3_client(endpoint):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(MB):
            h.update(chunk)
    return h.hexdigest()


_UNITS = {"b": 1, "kb": 1e3, "kib": 1024, "mb": 1e6, "mib": MB, "gb": 1e9, "gib": 1024**3}


def _bytes(s):
    m = re.match(r"([\d.]+)\s*([a-zA-Z]+)", s.strip())
    return float(m.group(1)) * _UNITS[m.group(2).lower()] if m else 0.0


def docker_stats(container):
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}|{{.NetIO}}", container],
        capture_output=True,
        text=True,
    ).stdout.strip()
    mem, net = out.split("|")
    rx, tx = net.split("/")
    return _bytes(mem.split("/")[0]), _bytes(rx), _bytes(tx)


class MemSampler:
    """Polls docker stats for a container; tracks peak memory and net bytes."""

    def __init__(self, container):
        self.container = container
        self.peak = 0.0
        self._stop = threading.Event()

    def __enter__(self):
        self.base_mem, self.rx0, self.tx0 = docker_stats(self.container)
        self.peak = self.base_mem
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            mem, _, _ = docker_stats(self.container)
            self.peak = max(self.peak, mem)

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join()
        mem, rx, tx = docker_stats(self.container)
        self.peak = max(self.peak, mem)
        self.net_mb = (rx - self.rx0 + tx - self.tx0) / MB


def make_big_file(path, size_mb):
    with open(path, "wb") as f:
        for _ in range(size_mb):
            f.write(os.urandom(MB))


# ---------------------------------------------------------------- resume helper (child)
def child_upload(endpoint, bucket, key, path, upload_id_file):
    """Starts a multipart upload and uploads parts one by one until killed."""
    s3 = s3_client(endpoint)
    uid = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    Path(upload_id_file).write_text(uid)
    with open(path, "rb") as f:
        n = 1
        while chunk := f.read(PART):
            s3.upload_part(Bucket=bucket, Key=key, UploadId=uid, PartNumber=n, Body=chunk)
            n += 1
    time.sleep(3600)  # parent kills us before completing


def resume_upload(s3, bucket, key, path):
    """Finds the abandoned upload and finishes it, uploading only missing parts."""
    ups = s3.list_multipart_uploads(Bucket=bucket, Prefix=key).get("Uploads", [])
    assert len(ups) == 1, f"expected one in-progress upload, found {len(ups)}"
    uid = ups[0]["UploadId"]
    have, marker = {}, 0
    while True:
        r = s3.list_parts(Bucket=bucket, Key=key, UploadId=uid, PartNumberMarker=marker)
        for p in r.get("Parts", []):
            have[p["PartNumber"]] = p["ETag"]
        if not r.get("IsTruncated"):
            break
        marker = r["NextPartNumberMarker"]
    found = len(have)
    with open(path, "rb") as f:
        n = 1
        while chunk := f.read(PART):
            if n not in have:
                have[n] = s3.upload_part(
                    Bucket=bucket, Key=key, UploadId=uid, PartNumber=n, Body=chunk
                )["ETag"]
            n += 1
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=uid,
        MultipartUpload={"Parts": [{"PartNumber": k, "ETag": v} for k, v in sorted(have.items())]},
    )
    return found, len(have)


# ---------------------------------------------------------------- s3 gateway run
def run_s3(target, endpoint, container, size_mb, n_small, small_kb, tmp):
    from boto3.s3.transfer import TransferConfig

    s3 = s3_client(endpoint)
    bucket = f"bench-{target}"
    res = {"target": target}
    s3.create_bucket(Bucket=bucket)
    big = tmp / "big.bin"
    try:
        make_big_file(big, size_mb)
        src_sha = sha256_file(big)

        # (a)+(d)+(e) upload with TransferConfig defaults
        cfg = TransferConfig()
        with MemSampler(container) as m:
            t = time.time()
            s3.upload_file(str(big), bucket, "big.bin", Config=cfg)
            up = time.time() - t
        res["big_up_MBps"] = round(size_mb / up, 1)
        res["big_up_peak_mem_MB"] = round(m.peak / MB)
        res["idle_mem_MB"] = round(m.base_mem / MB)

        dl = tmp / "big.dl"
        with MemSampler(container) as m:
            t = time.time()
            s3.download_file(bucket, "big.bin", str(dl), Config=cfg)
            down = time.time() - t
        res["big_down_MBps"] = round(size_mb / down, 1)
        res["big_down_peak_mem_MB"] = round(m.peak / MB)
        res["big_sha_ok"] = sha256_file(dl) == src_sha
        dl.unlink()

        # (a) kill mid-upload, resume from another process
        uid_file = tmp / "uid"
        child = subprocess.Popen(
            [sys.executable, __file__, "--child", endpoint, bucket, "resume.bin", str(big), str(uid_file)]
        )
        try:
            target_parts = (size_mb * MB // PART) // 2
            while True:
                time.sleep(0.5)
                if child.poll() is not None:
                    raise RuntimeError("child exited early")
                if uid_file.exists():
                    r = s3.list_parts(Bucket=bucket, Key="resume.bin", UploadId=uid_file.read_text())
                    # list_parts pages at 1000; we only need "about half"
                    if len(r.get("Parts", [])) >= target_parts:
                        break
        finally:
            child.send_signal(signal.SIGKILL)
            child.wait()
        found, total = resume_upload(s3, bucket, "resume.bin", big)
        s3.download_file(bucket, "resume.bin", str(dl))
        res["resume"] = f"{found}/{total} parts kept"
        res["resume_sha_ok"] = sha256_file(dl) == src_sha
        dl.unlink()
        s3.delete_object(Bucket=bucket, Key="resume.bin")

        # (g) server-side copy: single CopyObject, then managed multipart copy
        with MemSampler(container) as m:
            t = time.time()
            s3.copy_object(Bucket=bucket, Key="_staging/a1/big.bin", CopySource={"Bucket": bucket, "Key": "big.bin"})
            res["copy_object_s"] = round(time.time() - t, 1)
        res["copy_object_gw_net_MB"] = round(m.net_mb)
        res["copy_object_peak_mem_MB"] = round(m.peak / MB)
        h = s3.head_object(Bucket=bucket, Key="_staging/a1/big.bin")
        res["copy_object_size_ok"] = h["ContentLength"] == size_mb * MB
        s3.download_file(bucket, "_staging/a1/big.bin", str(dl))
        res["copy_object_sha_ok"] = sha256_file(dl) == src_sha
        dl.unlink()
        s3.delete_object(Bucket=bucket, Key="_staging/a1/big.bin")
        try:
            with MemSampler(container) as m:
                t = time.time()
                s3.copy({"Bucket": bucket, "Key": "big.bin"}, bucket, "promoted/big.bin", Config=cfg)
                res["multipart_copy_s"] = round(time.time() - t, 1)
            res["multipart_copy_gw_net_MB"] = round(m.net_mb)
            h = s3.head_object(Bucket=bucket, Key="promoted/big.bin")
            res["multipart_copy_size_ok"] = h["ContentLength"] == size_mb * MB
            s3.delete_object(Bucket=bucket, Key="promoted/big.bin")
        except Exception as e:  # noqa: BLE001
            res["multipart_copy_error"] = repr(e)[:300]
        s3.delete_object(Bucket=bucket, Key="big.bin")
        big.unlink()

        # (b)+(c) small objects
        bodies = {f"small/{i:05d}.bin": os.urandom(small_kb * 1024) for i in range(n_small)}
        shas = {k: hashlib.sha256(v).hexdigest() for k, v in bodies.items()}
        with ThreadPoolExecutor(16) as ex:
            t = time.time()
            list(ex.map(lambda kv: s3.put_object(Bucket=bucket, Key=kv[0], Body=kv[1]), bodies.items()))
            res["small_up_s"] = round(time.time() - t, 1)
            pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="small/")
            listed = sum(len(p.get("Contents", [])) for p in pages)
            res["small_listed_ok"] = listed == n_small
            t = time.time()
            got = dict(
                ex.map(lambda k: (k, s3.get_object(Bucket=bucket, Key=k)["Body"].read()), bodies)
            )
            res["small_down_s"] = round(time.time() - t, 1)
        res["small_sha_ok"] = all(hashlib.sha256(got[k]).hexdigest() == shas[k] for k in bodies)
        keys = list(bodies)
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]})

        # (f) dcpy connector, s3 type, endpoint = gateway
        res.update(check_dcpy(endpoint, bucket, tmp))
    finally:
        cleanup_bucket(s3, bucket)
        big.unlink(missing_ok=True)
    return res


def check_dcpy(endpoint, bucket, tmp):
    os.environ["AWS_ACCESS_KEY_ID"] = S3_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector, StorageType

    out = {}
    try:
        conn = PathedStorageConnector.from_storage_kwargs(
            conn_type="gateway",
            _validate_root_path=False,
            storage_backend=StorageType.S3,
            s3_bucket=bucket,
            s3_endpoint_url=endpoint,
            root_folder="dcpy",
        )
        src = tmp / "dcpy_src"
        (src / "sub").mkdir(parents=True)
        (src / "a.csv").write_text("x,y\n1,2\n")
        (src / "sub" / "b.txt").write_text("hello")
        # Keys dcpy really sends (dcpy.utils.s3.generate_metadata) have hyphens, which
        # aren't valid Azure metadata names; check both kinds.
        simple = {"version": "24v1", "commit": "abc123"}
        hyphen = {"date-created": "2026-10-29T00:00:00", "run-url": "https://x"}
        conn.push("one/a.csv", filepath=src / "a.csv", metadata=simple)
        conn.push("tree", filepath=src)
        got = conn.get_metadata("one/a.csv")
        out["dcpy_meta_simple"] = "exact" if got == simple else json.dumps(got, sort_keys=True)
        try:
            conn.push("one/h.csv", filepath=src / "a.csv", metadata=hyphen)
            got = conn.get_metadata("one/h.csv")
            out["dcpy_meta_hyphen"] = "exact" if got == hyphen else json.dumps(got, sort_keys=True)
        except Exception as e:  # noqa: BLE001
            out["dcpy_meta_hyphen"] = "error: " + repr(e)[:160]
        out["dcpy_exists"] = conn.exists("one/a.csv") and not conn.exists("nope.csv")
        out["dcpy_subfolders"] = conn.get_subfolders("tree") == ["sub"]
        dst = tmp / "dcpy_dst"
        conn.pull("tree", dst)
        conn.pull("one/a.csv", tmp / "a_pulled.csv")
        out["dcpy_pull_ok"] = (dst / "sub" / "b.txt").read_text() == "hello" and (
            tmp / "a_pulled.csv"
        ).read_text() == "x,y\n1,2\n"
    except Exception as e:  # noqa: BLE001
        out["dcpy_error"] = repr(e)[:300]
    return out


def cleanup_bucket(s3, bucket):
    for up in s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
        s3.abort_multipart_upload(Bucket=bucket, Key=up["Key"], UploadId=up["UploadId"])
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    s3.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------- direct azurite baseline
def run_azurite(container, size_mb, n_small, small_kb, tmp):
    from azure.storage.blob import BlobServiceClient

    svc = BlobServiceClient.from_connection_string(AZ_CONN, connection_verify=AZURITE_CA)
    cont = svc.create_container("bench-azurite")
    res = {"target": "azurite (direct SDK)"}
    big = tmp / "big.bin"
    try:
        make_big_file(big, size_mb)
        src_sha = sha256_file(big)
        with MemSampler(container) as m:
            t = time.time()
            with open(big, "rb") as f:
                cont.upload_blob("big.bin", f, max_concurrency=10)
            res["big_up_MBps"] = round(size_mb / (time.time() - t), 1)
        res["azurite_peak_mem_MB"] = round(m.peak / MB)
        dl = tmp / "big.dl"
        t = time.time()
        with open(dl, "wb") as f:
            cont.download_blob("big.bin", max_concurrency=10).readinto(f)
        res["big_down_MBps"] = round(size_mb / (time.time() - t), 1)
        res["big_sha_ok"] = sha256_file(dl) == src_sha
        dl.unlink()
        big.unlink()

        bodies = {f"small/{i:05d}.bin": os.urandom(small_kb * 1024) for i in range(n_small)}
        with ThreadPoolExecutor(16) as ex:
            t = time.time()
            list(ex.map(lambda kv: cont.upload_blob(kv[0], kv[1]), bodies.items()))
            res["small_up_s"] = round(time.time() - t, 1)
            t = time.time()
            got = dict(ex.map(lambda k: (k, cont.download_blob(k).readall()), bodies))
            res["small_down_s"] = round(time.time() - t, 1)
        res["small_sha_ok"] = all(got[k] == v for k, v in bodies.items())
    finally:
        cont.delete_container()
        big.unlink(missing_ok=True)
    return res


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child_upload(*sys.argv[2:])
        return
    p = argparse.ArgumentParser()
    p.add_argument("--target", choices=TARGETS, required=True)
    p.add_argument("--size-mb", type=int, default=1024)
    p.add_argument("--small", type=int, default=1000)
    p.add_argument("--small-kb", type=int, default=16)
    a = p.parse_args()
    t = TARGETS[a.target]
    with tempfile.TemporaryDirectory(prefix="gwbench-") as d:
        tmp = Path(d)
        if a.target == "azurite":
            res = run_azurite(t["container"], a.size_mb, a.small, a.small_kb, tmp)
        else:
            res = run_s3(a.target, t["endpoint"], t["container"], a.size_mb, a.small, a.small_kb, tmp)
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
