import json
from datetime import date, datetime, timezone
from pathlib import Path

import migrate_do_to_azure as migrate
import pytest
import pytz
from conftest import DEST_ENDPOINT, SOURCE_ENDPOINT
from moto import mock_aws
from typer.testing import CliRunner

from dcpy.utils import s3

# Moto keeps one set of buckets for both endpoints, so the "DigitalOcean" buckets get their
# own names.
BUCKETS = migrate.Buckets(
    source_recipes="do-recipes",
    source_private="do-private",
    source_publishing="do-publishing",
    dest_recipes="edm-recipes",
    dest_publishing="edm-publishing",
)
BUCKET_ARGS = [
    "--source-recipes-bucket", BUCKETS.source_recipes,
    "--source-private-bucket", BUCKETS.source_private,
    "--source-publishing-bucket", BUCKETS.source_publishing,
]  # fmt: skip

TEMPLATES = {
    "dcp_projects.yml": """
id: dcp_projects
ingestion:
  source:
    type: s3
    bucket: edm-private
    key: db-zap/{{ version }}/dcp_projects/dcp_projects_internal.csv
""",
    "dob_cofos.yml": """
id: dob_cofos
ingestion:
  source:
    type: s3
    bucket: edm-private
    key: dob_cofos/{{ version }}/dob_cofos.csv
""",
    # Not valid YAML before jinja renders it: read with the regex fallback.
    "dcp_projectbbls_cy.yml": """
id: dcp_projectbbls_cy
ingestion:
  source:
    type: s3
    bucket: edm-recipes
    key: inbox/dcp/dcp_projectbbls_cy/{{ version }}/Multiple BBL {{ version }} CDBG.xlsx
  file_format:
    sheet_name: {{ version }} CDBG
""",
}

SOURCE_OBJECTS = {
    BUCKETS.source_recipes: {
        "datasets/dpr_parks/20240101/dpr_parks.parquet": b"old parks",
        "datasets/dpr_parks/20250101/dpr_parks.parquet": b"new parks!",
        "datasets/dpr_parks/20250101/config.json": b'{"id": "dpr_parks", "version": "20250101"}',
        "datasets/dpr_parks/latest/dpr_parks.parquet": b"new parks!",
        "datasets/dpr_parks/latest/config.json": b'{"id": "dpr_parks", "version": "20250101"}',
        "datasets/dcp_mappluto/24v1/dcp_mappluto.sql": b"pluto",
        "raw/dpr_parks/20240101T000000/parks.zip": b"raw-old",
        "raw/dpr_parks/20250101T000000/parks.zip": b"raw-new",
        "inbox/dcp/dcp_projectbbls_cy/2024/Multiple BBL 2024 CDBG.xlsx": b"xlsx",
        "inbox/dcp/someone_elses_file.csv": b"stray",
        "library/old_stuff.txt": b"not mapped",
    },
    BUCKETS.source_private: {
        "datasets/dcp_zoning_private/20250101/file.parquet": b"private",
        "db-zap/20250101/dcp_projects/dcp_projects_internal.csv": b"zap",
        "db-zap/20250101/dcp_projects/notes.txt": b"not read",
        "dob_cofos/2025-06/dob_cofos.csv": b"cofos",
    },
    BUCKETS.source_publishing: {
        "db-pluto/publish/24v1/pluto.zip": b"pluto zip",
        "db-facilities/draft/25v1/facdb.csv": b"facdb",
    },
}


@pytest.fixture
def templates(tmp_path: Path) -> Path:
    folder = tmp_path / "templates"
    folder.mkdir()
    for name, text in TEMPLATES.items():
        (folder / name).write_text(text)
    return folder


@pytest.fixture
def stores(endpoints):
    """A fake DigitalOcean (source.test) and gateway (dest.test), with source data."""
    with mock_aws():
        source = s3.client(endpoint=SOURCE_ENDPOINT)
        dest = s3.client(
            endpoint=DEST_ENDPOINT,
            access_key_id="az-key",
            secret_access_key="az-secret",
        )
        for bucket, objects in SOURCE_OBJECTS.items():
            source.create_bucket(Bucket=bucket)
            for key, body in objects.items():
                source.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/x-test",
                    Metadata={"date-created": "2024-01-01T00:00:00-05:00"}
                    if "20240101" in key
                    else {"run-url": "https://example.test/run/1"},
                )
        for bucket in (BUCKETS.dest_recipes, BUCKETS.dest_publishing):
            dest.create_bucket(Bucket=bucket)
        yield source, dest


def _areas():
    return migrate.mapping(BUCKETS, "ingest", "inbox")


def _plan(stores, templates, **selection):
    return _full_plan(stores, templates, **selection).items


def _full_plan(stores, templates, *, catalog=None, trust_catalog=False, **selection):
    source, dest = stores
    return migrate.plan(
        source,
        dest,
        _areas(),
        migrate.Selection(**selection),
        migrate.inbox_sources(templates),
        workers=2,
        catalog=catalog,
        trust_catalog=trust_catalog,
    )


def _keys(bucket: str, prefix: str = "") -> list[str]:
    dest = s3.client(endpoint=DEST_ENDPOINT, access_key_id="x", secret_access_key="y")
    response = dest.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return sorted(o["Key"] for o in response.get("Contents", []))


def run(templates: Path, *args: str):
    return CliRunner().invoke(
        migrate.app,
        [*BUCKET_ARGS, "--templates-dir", str(templates), "--workers", "2", *args],
    )


# --- mapping ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "area, source_key, dest_bucket, dest_key",
    [
        ("datasets", "datasets/dpr_parks/20250101/a.parquet", "edm-recipes", "ingest/datasets/dpr_parks/20250101/a.parquet"),
        ("raw", "raw/dpr_parks/20250101T000000/a.zip", "edm-recipes", "ingest/raw/dpr_parks/20250101T000000/a.zip"),
        ("private", "datasets/dcp_zoning_private/1/a.parquet", "edm-recipes", "ingest/datasets/dcp_zoning_private/1/a.parquet"),
        ("inbox", "db-zap/20250101/dcp_projects/a.csv", "edm-recipes", "inbox/db-zap/20250101/dcp_projects/a.csv"),
        ("recipes_inbox", "inbox/dcp/x/2024/a.xlsx", "edm-recipes", "inbox/dcp/x/2024/a.xlsx"),
        ("publishing", "db-pluto/publish/24v1/pluto.zip", "edm-publishing", "db-pluto/publish/24v1/pluto.zip"),
    ],
)  # fmt: skip
def test_mapping(area, source_key, dest_bucket, dest_key):
    row = migrate.mapping(migrate.Buckets(), "ingest", "inbox")[area]
    assert row.dest_bucket == dest_bucket
    assert migrate.dest_key(row, source_key) == dest_key


def test_mapping_source_buckets():
    areas = migrate.mapping(migrate.Buckets(), "ingest", "inbox")
    assert {n: a.source_bucket for n, a in areas.items()} == {
        "datasets": "edm-recipes",
        "raw": "edm-recipes",
        "private": "edm-private",
        "inbox": "edm-private",
        "recipes_inbox": "edm-recipes",
        "publishing": "edm-publishing",
    }


def test_mapping_root_folders_follow_arguments():
    areas = migrate.mapping(migrate.Buckets(), "/azure/", "landing")
    assert (
        migrate.dest_key(areas["datasets"], "datasets/a/1/f") == "azure/datasets/a/1/f"
    )
    assert migrate.dest_key(areas["inbox"], "db-zap/f") == "landing/db-zap/f"


def test_dest_key_outside_area():
    with pytest.raises(ValueError):
        migrate.dest_key(_areas()["datasets"], "raw/a/1/f")


def test_root_folders_from_env(monkeypatch):
    for name in ("AZ_RECIPES_BUCKET_ROOT_FOLDER", "RECIPES_BUCKET_ROOT_FOLDER", "INBOX_ROOT_FOLDER"):  # fmt: skip
        monkeypatch.delenv(name, raising=False)
    assert migrate.recipes_root_folder_from_env() == "ingest"
    assert migrate.inbox_root_folder_from_env() == "inbox"
    monkeypatch.setenv("RECIPES_BUCKET_ROOT_FOLDER", "shared")
    assert migrate.recipes_root_folder_from_env() == "shared"
    monkeypatch.setenv("AZ_RECIPES_BUCKET_ROOT_FOLDER", "az")
    assert migrate.recipes_root_folder_from_env() == "az"
    monkeypatch.setenv("INBOX_ROOT_FOLDER", "in")
    assert migrate.inbox_root_folder_from_env() == "in"


def test_inbox_sources(templates):
    sources = {s.template_id: s for s in migrate.inbox_sources(templates)}
    assert set(sources) == {"dcp_projects", "dob_cofos", "dcp_projectbbls_cy"}
    zap = sources["dcp_projects"]
    assert zap.static_prefix == "db-zap/"
    assert zap.matches("db-zap/20250101/dcp_projects/dcp_projects_internal.csv")
    assert not zap.matches("db-zap/20250101/dcp_projects/notes.txt")
    cdbg = sources["dcp_projectbbls_cy"]
    assert cdbg.bucket == "edm-recipes"
    assert cdbg.matches("inbox/dcp/dcp_projectbbls_cy/2024/Multiple BBL 2024 CDBG.xlsx")


def test_real_templates_parse():
    sources = migrate.inbox_sources(migrate.DEFAULT_TEMPLATES_DIR)
    assert any(s.bucket == "edm-private" for s in sources)


# --- selection -------------------------------------------------------------------------------


def _selected(items, area=None):
    return sorted(i.source.key for i in items if area is None or i.source.area == area)


def test_select_everything(stores, templates):
    items = _plan(stores, templates)
    assert _selected(items, "datasets") == sorted(
        k for k in SOURCE_OBJECTS["do-recipes"] if k.startswith("datasets/")
    )
    assert _selected(items, "private") == [
        "datasets/dcp_zoning_private/20250101/file.parquet"
    ]
    # Only what templates read; private datasets/ belongs to its own area.
    assert _selected(items, "inbox") == [
        "db-zap/20250101/dcp_projects/dcp_projects_internal.csv",
        "dob_cofos/2025-06/dob_cofos.csv",
    ]
    assert _selected(items, "recipes_inbox") == [
        "inbox/dcp/dcp_projectbbls_cy/2024/Multiple BBL 2024 CDBG.xlsx"
    ]
    assert len(_selected(items, "publishing")) == 2
    assert "library/old_stuff.txt" not in _selected(items)
    assert {i.status for i in items} == {"copy"}


def test_unmatched_inbox_files_are_reported(stores, templates):
    source, dest = stores
    unmatched: dict = {}
    migrate.plan(
        source,
        dest,
        _areas(),
        migrate.Selection(areas=("inbox", "recipes_inbox")),
        migrate.inbox_sources(templates),
        unmatched=unmatched,
    )
    assert unmatched["inbox"].sample == ["db-zap/20250101/dcp_projects/notes.txt"]
    assert unmatched["recipes_inbox"].sample == ["inbox/dcp/someone_elses_file.csv"]


def test_include_unmatched_inbox(stores, templates):
    items = _plan(stores, templates, areas=("inbox",), include_unmatched_inbox=True)
    assert "db-zap/20250101/dcp_projects/notes.txt" in _selected(items)
    assert not any(k.startswith("datasets/") for k in _selected(items))


@pytest.mark.parametrize("pattern", ["dpr_parks", "dpr_*"])
def test_select_by_dataset(stores, templates, pattern):
    items = _plan(stores, templates, areas=("datasets", "raw"), datasets=(pattern,))
    assert {i.source.dataset_id for i in items} == {"dpr_parks"}
    assert len(_selected(items, "raw")) == 2


def test_select_inbox_by_template_id(stores, templates):
    items = _plan(stores, templates, datasets=("dob_cofos",))
    assert _selected(items) == ["dob_cofos/2025-06/dob_cofos.csv"]


def test_select_publishing_by_product(stores, templates):
    items = _plan(stores, templates, areas=("publishing",), datasets=("db-pluto",))
    assert _selected(items) == ["db-pluto/publish/24v1/pluto.zip"]


def test_datasets_file(tmp_path):
    path = tmp_path / "ids.txt"
    path.write_text("dpr_parks\n\n# a comment\ndcp_*  # trailing\n")
    assert migrate.read_datasets_file(path) == ["dpr_parks", "dcp_*"]


def test_latest_only(stores, templates):
    items = _plan(stores, templates, areas=("datasets", "raw"), latest_only=True)
    assert _selected(items, "datasets") == [
        "datasets/dpr_parks/20250101/config.json",
        "datasets/dpr_parks/20250101/dpr_parks.parquet",
        "datasets/dpr_parks/latest/config.json",
        "datasets/dpr_parks/latest/dpr_parks.parquet",
        # No latest/: nothing says which version, so nothing is picked.
    ]
    assert _selected(items, "raw") == ["raw/dpr_parks/20250101T000000/parks.zip"]


def test_versions_since():
    area = _areas()["datasets"]

    def obj(version, day):
        return migrate.SourceObject(
            area="datasets",
            key=f"datasets/a/{version}/f",
            size=1,
            etag="e",
            last_modified=datetime(2025, 1, day, tzinfo=timezone.utc),
            dataset_id="a",
            version=version,
        )

    objects = [obj("v1", 1), obj("v2", 10), obj("latest", 10)]
    selection = migrate.Selection(versions_since=date(2025, 1, 5))
    kept = migrate.filter_versions(None, area, objects, selection, workers=1)
    assert sorted(o.version for o in kept) == ["latest", "v2"]


# --- dry run and execute ---------------------------------------------------------------------


def test_dry_run_copies_nothing(stores, templates):
    result = run(templates)
    assert result.exit_code == 0, result.output
    assert "Dry run: nothing copied" in result.output
    assert "Total to copy:" in result.output
    assert "do-recipes/datasets/dpr_parks/" in result.output
    assert "-> edm-recipes/ingest/datasets/" in result.output
    assert _keys("edm-recipes") == []
    assert _keys("edm-publishing") == []


def test_execute_copies_with_azure_metadata(stores, templates):
    source, dest = stores
    result = run(templates, "--execute")
    assert result.exit_code == 0, result.output

    assert "ingest/datasets/dpr_parks/20250101/dpr_parks.parquet" in _keys(
        "edm-recipes"
    )
    assert "ingest/datasets/dcp_zoning_private/20250101/file.parquet" in _keys("edm-recipes")  # fmt: skip
    assert "ingest/raw/dpr_parks/20240101T000000/parks.zip" in _keys("edm-recipes")
    assert "inbox/db-zap/20250101/dcp_projects/dcp_projects_internal.csv" in _keys("edm-recipes")  # fmt: skip
    assert "inbox/dcp/dcp_projectbbls_cy/2024/Multiple BBL 2024 CDBG.xlsx" in _keys("edm-recipes")  # fmt: skip
    assert _keys("edm-publishing") == sorted(SOURCE_OBJECTS["do-publishing"])

    # date-created renamed, value kept
    old = dest.get_object(
        Bucket="edm-recipes", Key="ingest/datasets/dpr_parks/20240101/dpr_parks.parquet"
    )
    assert old["Body"].read() == b"old parks"
    assert old["ContentType"] == "application/x-test"
    assert old["Metadata"]["date_created"] == "2024-01-01T00:00:00-05:00"
    assert "date-created" not in old["Metadata"]

    # run-url renamed; date_created backfilled from DigitalOcean's LastModified
    key = "datasets/dpr_parks/20250101/dpr_parks.parquet"
    new = dest.get_object(Bucket="edm-recipes", Key=f"ingest/{key}")
    original = source.head_object(Bucket="do-recipes", Key=key)
    assert new["Metadata"]["run_url"] == "https://example.test/run/1"
    assert (
        new["Metadata"]["date_created"]
        == original["LastModified"]
        .astimezone(pytz.timezone("America/New_York"))
        .isoformat()
    )
    assert new["Metadata"][migrate.SOURCE_ETAG] == original["ETag"].strip('"')


def test_rerun_skips_present_objects(stores, templates):
    source, dest = stores
    assert run(templates, "--execute").exit_code == 0
    items = _plan(stores, templates)
    assert {i.status for i in items} == {"present"}
    result = run(templates, "--execute")
    assert "Total to copy: 0 objects" in result.output


def test_resumes_after_partial_copy(stores, templates):
    assert (
        run(templates, "--execute", "-a", "datasets", "-d", "dpr_parks").exit_code == 0
    )
    items = _plan(stores, templates, areas=("datasets",))
    assert {i.source.dataset_id for i in items if i.status == "copy"} == {
        "dcp_mappluto"
    }


def test_conflicts_left_alone_without_overwrite(stores, templates):
    source, dest = stores
    key = "ingest/datasets/dcp_mappluto/24v1/dcp_mappluto.sql"
    dest.put_object(Bucket="edm-recipes", Key=key, Body=b"something else")
    result = run(templates, "--execute", "-a", "datasets")
    assert result.exit_code == 0, result.output
    assert "Conflicts" in result.output
    assert dest.get_object(Bucket="edm-recipes", Key=key)["Body"].read() == b"something else"  # fmt: skip

    assert run(templates, "--execute", "-a", "datasets", "--overwrite").exit_code == 0
    assert dest.get_object(Bucket="edm-recipes", Key=key)["Body"].read() == b"pluto"


def test_verify_checks_stored_source_etag(stores, templates):
    source, dest = stores
    assert (
        run(templates, "--execute", "-a", "datasets", "-d", "dpr_parks").exit_code == 0
    )
    key = "ingest/datasets/dpr_parks/20250101/dpr_parks.parquet"
    # Same size, different bytes, as a copy made some other way might be.
    dest.put_object(Bucket="edm-recipes", Key=key, Body=b"new parks?", Metadata={migrate.SOURCE_ETAG: "x"})  # fmt: skip
    config = "ingest/datasets/dpr_parks/20250101/config.json"
    body = dest.get_object(Bucket="edm-recipes", Key=config)["Body"].read()
    dest.put_object(Bucket="edm-recipes", Key=config, Body=body)  # no source_etag

    source_client, dest_client = stores
    items = migrate.plan(
        source_client,
        dest_client,
        _areas(),
        migrate.Selection(areas=("datasets",), datasets=("dpr_parks",)),
        [],
        verify=True,
    ).items
    status = {i.dest_key: i.status for i in items}
    assert status[key] == "conflict"
    assert status[config] == "unverified"
    assert status["ingest/datasets/dpr_parks/20240101/dpr_parks.parquet"] == "present"


def test_multipart_copy_streams_large_objects(stores, templates):
    source, dest = stores
    body = b"x" * (6 * 1024 * 1024)
    source.put_object(Bucket="do-recipes", Key="datasets/big/1/big.bin", Body=body)
    result = run(templates, "--execute", "-a", "datasets", "-d", "big", "--part-size-mb", "5")  # fmt: skip
    assert result.exit_code == 0, result.output
    copied = dest.get_object(Bucket="edm-recipes", Key="ingest/datasets/big/1/big.bin")
    assert copied["Body"].read() == body
    assert "-" in copied["ETag"]  # multipart
    assert "date_created" in copied["Metadata"]


# --- clients and the denylist ----------------------------------------------------------------


def test_denylist_blocking_source_fails_clearly(stores, templates, monkeypatch):
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "source.test")
    result = run(templates)
    assert result.exit_code == 2
    assert "unset DCPY_S3_ENDPOINT_DENYLIST" in result.output
    assert "do-secret" not in result.output


def test_denylist_blocking_destination_fails(stores, templates, monkeypatch):
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "dest.test")
    result = run(templates)
    assert result.exit_code == 2
    assert "denied" in result.output


def test_unrelated_denylist_is_fine(stores, templates, monkeypatch):
    monkeypatch.setenv("DCPY_S3_ENDPOINT_DENYLIST", "nyc3.digitaloceanspaces.com")
    assert run(templates).exit_code == 0


def test_same_source_and_destination_refused(endpoints, monkeypatch):
    monkeypatch.setenv("AZ_S3_ENDPOINT", SOURCE_ENDPOINT)
    with pytest.raises(migrate.MigrationConfigError, match="same host"):
        migrate.clients(None, None)


def test_missing_destination_credentials(endpoints, monkeypatch):
    monkeypatch.delenv("AZ_S3_SECRET_ACCESS_KEY")
    with pytest.raises(migrate.MigrationConfigError, match="AZ_S3_SECRET_ACCESS_KEY"):
        migrate.clients(None, None)


def test_ca_bundle_only_for_destination(endpoints, monkeypatch):
    seen = []
    monkeypatch.setattr(
        migrate.s3, "client", lambda **kwargs: seen.append(kwargs) or kwargs
    )
    migrate.clients(None, "/etc/city-ca.pem")
    source, dest = seen
    assert source.get("verify") is None
    assert dest["verify"] == "/etc/city-ca.pem"
    assert dest["endpoint"] == DEST_ENDPOINT


# --- reruns: folders, markers, latest/, conflicts ---------------------------------------------


def _put(client, bucket: str, objects: dict[str, bytes], metadata=None) -> None:
    for key, body in objects.items():
        client.put_object(Bucket=bucket, Key=key, Body=body, Metadata=metadata or {})


def _config(version: str, timestamp: str | None = None) -> bytes:
    config: dict = {"id": "dpr_parks", "version": version}
    if timestamp:
        config["run_details"] = {"timestamp": timestamp}
    return json.dumps(config).encode()


def _body(key: str, bucket: str = "edm-recipes") -> bytes:
    dest = s3.client(endpoint=DEST_ENDPOINT, access_key_id="x", secret_access_key="y")
    return dest.get_object(Bucket=bucket, Key=key)["Body"].read()


# A version folder with several files; config.json is the biggest, so largest-first alone
# would send it first.
MULTI = {
    "datasets/multi/1/config.json": b'{"id": "multi", "version": "1", "pad": "' + b"x" * 200 + b'"}',
    **{f"datasets/multi/1/part{n}.parquet": b"p" * (10 + n) for n in range(6)},
}  # fmt: skip


@pytest.fixture
def record_copies(monkeypatch):
    """Dest keys in the order copies finished, and a set of source keys to fail."""
    copied: list[str] = []
    fail: set[str] = set()
    original = migrate.copy_one

    def recording(source, dest, areas, item, config, attempts=3):
        if item.source.key in fail:
            raise RuntimeError("interrupted")
        original(source, dest, areas, item, config, attempts=1)
        copied.append(item.dest_key)

    monkeypatch.setattr(migrate, "copy_one", recording)
    return copied, fail


def test_config_json_copied_last(stores, templates, record_copies):
    source, dest = stores
    copied, _ = record_copies
    _put(source, "do-recipes", MULTI)
    _put(
        source,
        "do-publishing",
        {
            "db-pluto/publish/25v1/build_metadata.json": b"{}",
            "db-pluto/publish/25v1/pluto.zip": b"z" * 50,
            "db-pluto/publish/25v1/gis/pluto.gdb.zip": b"g" * 40,
        },
    )
    result = run(templates, "--execute", "-a", "datasets", "-a", "publishing", "-d", "multi", "-d", "db-pluto", "--workers", "4")  # fmt: skip
    assert result.exit_code == 0, result.output

    folder = [k for k in copied if k.startswith("ingest/datasets/multi/1/")]
    assert len(folder) == len(MULTI)
    assert folder[-1] == "ingest/datasets/multi/1/config.json"
    publish = [k for k in copied if k.startswith("db-pluto/publish/25v1/")]
    assert publish[-1] == "db-pluto/publish/25v1/build_metadata.json"
    assert "Folders completed: 3 of 3." in result.output
    assert "materialize `version_catalog`" in result.output


def test_interrupted_folder_is_redone(stores, templates, record_copies):
    source, dest = stores
    copied, fail = record_copies
    _put(source, "do-recipes", MULTI)
    fail.add("datasets/multi/1/part3.parquet")
    result = run(templates, "--execute", "-a", "datasets", "-d", "multi")
    assert result.exit_code == 1
    assert "ingest/datasets/multi/1/config.json" not in _keys("edm-recipes")
    assert "Folders completed: 0 of 1" in result.output

    fail.clear()
    plan = _full_plan(stores, templates, areas=("datasets",), datasets=("multi",))
    (folder,) = plan.folders
    assert folder.status == "copy" and "resuming" in folder.reason
    assert sorted(i.source.key for i in plan.items if i.status == "copy") == [
        "datasets/multi/1/config.json",
        "datasets/multi/1/part3.parquet",
    ]
    copied.clear()
    assert run(templates, "--execute", "-a", "datasets", "-d", "multi").exit_code == 0
    assert copied == [
        "ingest/datasets/multi/1/part3.parquet",
        "ingest/datasets/multi/1/config.json",
    ]


def test_data_files_without_config_json_are_redone(stores, templates):
    source, dest = stores
    _put(source, "do-recipes", MULTI)
    # Every data file there, from some earlier copy, but no config.json: not complete.
    _put(
        dest, "edm-recipes", {f"ingest/{k}": v for k, v in MULTI.items() if "part" in k}
    )
    plan = _full_plan(stores, templates, areas=("datasets",), datasets=("multi",))
    assert [i.source.key for i in plan.items if i.status == "copy"] == [
        "datasets/multi/1/config.json"
    ]
    assert plan.folders[0].status == "copy"


class Recorder:
    """A client that records the calls made through it."""

    def __init__(self, client):
        self._client = client
        self.calls: list[tuple[str, str]] = []

    def get_paginator(self, name):
        paginator = self._client.get_paginator(name)
        recorder = self

        class Paginator:
            def paginate(self, **kwargs):
                recorder.calls.append((name, kwargs.get("Prefix", "")))
                return paginator.paginate(**kwargs)

        return Paginator()

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr):
            return attr

        def call(*args, **kwargs):
            self.calls.append((name, kwargs.get("Key", kwargs.get("Prefix", ""))))
            return attr(*args, **kwargs)

        return call


def _catalog_for(dest, prefix: str = "ingest/datasets/") -> dict:
    """A fake catalog: a row for every version folder with a config.json at the destination."""
    from types import SimpleNamespace

    rows = {}
    for o in migrate._list(dest, "edm-recipes", prefix):
        parts = o["Key"][len(prefix) :].split("/")
        if len(parts) == 3 and parts[2] == "config.json" and parts[1] != "latest":
            location = f"s3://edm-recipes/{prefix}{parts[0]}/{parts[1]}/"
            rows[location] = SimpleNamespace(location=location, details={}, archived_at=None)  # fmt: skip
    return rows


@pytest.mark.parametrize("mode", ["listing", "catalog", "trusted catalog"])
def test_present_folder_skipped_without_copying(stores, templates, record_copies, mode):
    source, dest = stores
    copied, _ = record_copies
    _put(source, "do-recipes", MULTI)
    assert run(templates, "--execute", "-a", "datasets", "-d", "multi").exit_code == 0
    copied.clear()

    recorder = Recorder(dest)
    plan = migrate.plan(
        source,
        recorder,
        _areas(),
        migrate.Selection(areas=("datasets",), datasets=("multi",)),
        [],
        catalog=None if mode == "listing" else _catalog_for(dest),
        trust_catalog=mode == "trusted catalog",
    )
    assert {i.status for i in plan.items} == {"present"}
    assert plan.folders[0].status == "present"
    lists = [c for c in recorder.calls if c[0] == "list_objects_v2"]
    heads = [c for c in recorder.calls if c[0] == "head_object"]
    if mode == "listing":
        assert len(lists) == 1
    else:
        assert lists == []
    # Never a data file: at most the marker.
    assert heads == ([] if mode == "trusted catalog" else [("head_object", "ingest/datasets/multi/1/config.json")])  # fmt: skip
    assert not [c for c in recorder.calls if c[0] in ("get_object", "upload_fileobj")]

    migrate.execute(source, dest, _areas(), plan.items, workers=2, part_size_mb=16)
    assert copied == []


def _signature(plan):
    return (
        sorted((i.dest_key, i.status) for i in plan.items),
        sorted((f.dest_prefix, f.status, f.reason, tuple(f.deletions)) for f in plan.folders),
    )  # fmt: skip


def test_catalog_and_listing_plan_the_same(stores, templates):
    source, dest = stores
    _put(source, "do-recipes", MULTI)
    assert run(templates, "--execute", "-a", "datasets", "-a", "private").exit_code == 0
    # A stale catalog: it hasn't seen 20250101 copied, and still has multi/1, which is gone.
    catalog = _catalog_for(dest)
    del catalog["s3://edm-recipes/ingest/datasets/dpr_parks/20250101/"]
    dest.delete_object(Bucket="edm-recipes", Key="ingest/datasets/multi/1/config.json")
    # New at DigitalOcean since, and an interrupted folder.
    _put(source, "do-recipes", {
        "datasets/dpr_parks/20260101/dpr_parks.parquet": b"newest",
        "datasets/dpr_parks/20260101/config.json": _config("20260101", "2099-06-01T00:00:00-05:00"),
        "datasets/dpr_parks/latest/config.json": _config("20260101", "2099-06-01T00:00:00-05:00"),
    })  # fmt: skip
    # Azure ingested a version DigitalOcean also has.
    _put(source, "do-private", {"datasets/dcp_zoning_private/26d/config.json": b'{"id": "z", "version": "26d"}'})  # fmt: skip
    _put(dest, "edm-recipes", {"ingest/datasets/dcp_zoning_private/26d/config.json": b'{"id": "z", "version": "26d", "azure": 1}'})  # fmt: skip

    areas = ("datasets", "private")
    listed = _full_plan(stores, templates, areas=areas)
    cataloged = _full_plan(stores, templates, areas=areas, catalog=catalog)
    assert listed.presence == {"datasets": "listing", "private": "listing"}
    assert cataloged.presence == {"datasets": "catalog", "private": "catalog"}
    assert _signature(listed) == _signature(cataloged)
    status = {f.dest_prefix: f.status for f in listed.folders}
    assert status == {
        "ingest/datasets/dpr_parks/20250101/": "present",
        "ingest/datasets/dpr_parks/20260101/": "copy",
        "ingest/datasets/dpr_parks/latest/": "replace",
        "ingest/datasets/multi/1/": "copy",
        "ingest/datasets/dcp_zoning_private/26d/": "conflict",
    }


def test_load_catalog(monkeypatch):
    from types import SimpleNamespace

    from dcpy.connectors.edm import catalog

    monkeypatch.delenv(catalog.CATALOG_PG_URL_ENV_VAR, raising=False)
    assert migrate.load_catalog("edm-recipes", ["ingest/datasets/"]) is None

    monkeypatch.setenv(catalog.CATALOG_PG_URL_ENV_VAR, "postgresql://nowhere/de_apps")

    def unreachable(*args, **kwargs):
        raise RuntimeError("timeout")

    monkeypatch.setattr(catalog, "query", unreachable)
    assert migrate.load_catalog("edm-recipes", ["ingest/datasets/"]) is None

    rows = [
        SimpleNamespace(location="s3://edm-recipes/ingest/datasets/a/1/"),
        SimpleNamespace(location="s3://edm-recipes/datasets/a/1/"),  # DigitalOcean's
        SimpleNamespace(location=None),
    ]
    monkeypatch.setattr(catalog, "query", lambda *a, **k: rows)
    found = migrate.load_catalog("edm-recipes", ["ingest/datasets/"])
    assert list(found) == ["s3://edm-recipes/ingest/datasets/a/1/"]


def _latest_folder(plan):
    return next(f for f in plan.folders if f.kind == "latest")


def test_latest_same_version_skipped(stores, templates):
    assert (
        run(templates, "--execute", "-a", "datasets", "-d", "dpr_parks").exit_code == 0
    )
    folder = _latest_folder(_full_plan(stores, templates, areas=("datasets",), datasets=("dpr_parks",)))  # fmt: skip
    assert folder.status == "present" and folder.reason == "same version 20250101"


def test_latest_replaced_when_digitalocean_newer(stores, templates, record_copies):
    source, dest = stores
    copied, _ = record_copies
    latest = "ingest/datasets/dpr_parks/latest/"
    _put(dest, "edm-recipes", {
        f"{latest}config.json": _config("20240101", "2024-01-01T00:00:00-05:00"),
        f"{latest}dpr_parks.sql": b"stale",
    })  # fmt: skip
    folder = _latest_folder(_full_plan(stores, templates, areas=("datasets",), datasets=("dpr_parks",)))  # fmt: skip
    assert folder.status == "replace" and folder.reason == "20240101 -> 20250101"
    assert folder.deletions == [f"{latest}dpr_parks.sql"]

    result = run(templates, "--execute", "-a", "datasets", "-d", "dpr_parks")
    assert result.exit_code == 0, result.output
    assert _keys("edm-recipes", latest) == [f"{latest}config.json", f"{latest}dpr_parks.parquet"]  # fmt: skip
    assert json.loads(_body(f"{latest}config.json"))["version"] == "20250101"
    assert [k for k in copied if k.startswith(latest)][-1] == f"{latest}config.json"


def test_latest_kept_when_azure_newer(stores, templates):
    source, dest = stores
    key = "ingest/datasets/dpr_parks/latest/config.json"
    _put(dest, "edm-recipes", {key: _config("20260101", "2099-01-01T00:00:00-05:00")})
    result = run(templates, "--execute", "-a", "datasets", "-d", "dpr_parks")
    assert result.exit_code == 0, result.output
    assert "kept: azure latest newer: 20260101" in result.output
    assert json.loads(_body(key))["version"] == "20260101"
    # The version folders still copy.
    assert "ingest/datasets/dpr_parks/20250101/config.json" in _keys("edm-recipes")


def test_latest_azure_date_from_catalog(stores, templates):
    from types import SimpleNamespace

    source, dest = stores
    key = "ingest/datasets/dpr_parks/latest/config.json"
    # Its config says it's old; the catalog's row for that version says otherwise.
    _put(dest, "edm-recipes", {key: _config("20260101", "2020-01-01T00:00:00-05:00")})
    location = "s3://edm-recipes/ingest/datasets/dpr_parks/20260101/"
    row = SimpleNamespace(location=location, details={"archived_at_iso": "2099-01-01T00:00:00-05:00"}, archived_at=None)  # fmt: skip
    selection = {"areas": ("datasets",), "datasets": ("dpr_parks",)}
    assert (
        _latest_folder(_full_plan(stores, templates, **selection)).status == "replace"
    )
    folder = _latest_folder(_full_plan(stores, templates, catalog={location: row}, **selection))  # fmt: skip
    assert folder.status == "kept"


def test_version_conflict_reported_and_never_overwritten(stores, templates):
    source, dest = stores
    prefix = "ingest/datasets/dpr_parks/20250101/"
    azure = {f"{prefix}config.json": _config("20250101", "2025-01-02T00:00:00-05:00"), f"{prefix}dpr_parks.parquet": b"azure's own"}  # fmt: skip
    _put(dest, "edm-recipes", azure)
    for flag in ([], ["--overwrite"]):
        result = run(templates, "--execute", "-a", "datasets", "-d", "dpr_parks", *flag)
        assert result.exit_code == 0, result.output
        assert "Version conflicts" in result.output
        assert f"edm-recipes/{prefix}" in result.output
        for key, body in azure.items():
            assert _body(key) == body


def test_inbox_same_size_new_etag_recopied(stores, templates):
    source, dest = stores
    assert run(templates, "--execute", "-a", "inbox").exit_code == 0
    key = "db-zap/20250101/dcp_projects/dcp_projects_internal.csv"
    source.put_object(Bucket="do-private", Key=key, Body=b"zaq")  # same size as b"zap"
    items = {i.source.key: i.status for i in _plan(stores, templates, areas=("inbox",))}
    assert items == {key: "copy", "dob_cofos/2025-06/dob_cofos.csv": "present"}
    assert run(templates, "--execute", "-a", "inbox").exit_code == 0
    assert _body(f"inbox/{key}") == b"zaq"


def test_inbox_written_at_azure_is_a_conflict(stores, templates):
    source, dest = stores
    key = "inbox/dob_cofos/2025-06/dob_cofos.csv"
    dest.put_object(Bucket="edm-recipes", Key=key, Body=b"azure")  # no source_etag
    items = {i.dest_key: i.status for i in _plan(stores, templates, areas=("inbox",))}
    assert items[key] == "conflict"


def test_unstamped_version_same_size_different_bytes_is_a_conflict(stores, templates):
    source, dest = stores
    key = "ingest/datasets/dpr_parks/20250101/config.json"
    body = b'{"id": "dpr_parks", "version": "20250102"}'  # as long as DigitalOcean's
    assert len(body) == len(SOURCE_OBJECTS["do-recipes"]["datasets/dpr_parks/20250101/config.json"])  # fmt: skip
    dest.put_object(Bucket="edm-recipes", Key=key, Body=body)
    plan = _full_plan(stores, templates, areas=("datasets",), datasets=("dpr_parks",))
    folder = next(f for f in plan.folders if f.dest_prefix.endswith("/20250101/"))
    assert folder.status == "conflict" and folder.complete
