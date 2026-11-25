"""Run from the repo root: pytest apps/dagster/tests"""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from dagster import materialize

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingest.assets import make_ingest_asset  # noqa: E402
from ingest.resources import LocalStorageResource  # noqa: E402

TEMPLATE = "bpl_libraries"


@pytest.fixture
def fake_ingest(monkeypatch):
    seen: dict = {}

    def ingest(*, staging_dir: Path, **kwargs):
        seen["staging_dir"] = staging_dir
        (staging_dir / "out.parquet").write_text("x")
        if seen.get("fail"):
            raise RuntimeError("boom")
        return [SimpleNamespace(version="20260101")]

    monkeypatch.setattr("dcpy.lifecycle.ingest.run.ingest", ingest)
    monkeypatch.delenv("DCPY_KEEP_STAGING", raising=False)
    return seen


def _run(tmp_path, push=True):
    from dagster import DagsterInstance

    from ingest.partitions import dynamic_partitions_def_name

    asset = make_ingest_asset(TEMPLATE)
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions(dynamic_partitions_def_name(TEMPLATE), ["20260101"])
    return materialize(
        [asset],
        partition_key="20260101",
        instance=instance,
        resources={"local_storage": LocalStorageResource(base_path=str(tmp_path))},
        run_config={"ops": {f"ingest_{TEMPLATE}": {"config": {"push": push}}}},
        raise_on_error=False,
    )


def _metadata(result):
    mat = result.asset_materializations_for_node(f"ingest_{TEMPLATE}")[0]
    return mat.metadata


def test_staging_removed_on_success(tmp_path, fake_ingest):
    result = _run(tmp_path)
    assert result.success
    assert not fake_ingest["staging_dir"].exists()
    assert list(tmp_path.iterdir()) == []  # empty parents are gone too
    assert "output_path" not in _metadata(result)


def test_staging_removed_on_failure(tmp_path, fake_ingest):
    fake_ingest["fail"] = True
    result = _run(tmp_path)
    assert not result.success
    assert not fake_ingest["staging_dir"].exists()


def test_staging_kept_when_asked(tmp_path, fake_ingest, monkeypatch):
    monkeypatch.setenv("DCPY_KEEP_STAGING", "true")
    result = _run(tmp_path)
    assert fake_ingest["staging_dir"].exists()
    assert _metadata(result)["output_path"].value == str(fake_ingest["staging_dir"])


def test_staging_kept_without_push(tmp_path, fake_ingest):
    result = _run(tmp_path, push=False)
    assert fake_ingest["staging_dir"].exists()
    assert "output_path" in _metadata(result)


def test_other_runs_staging_survives(tmp_path, fake_ingest):
    sibling = tmp_path / "ingest" / TEMPLATE / "20260101" / "other-run"
    sibling.mkdir(parents=True)
    _run(tmp_path)
    assert sibling.exists()
    assert not fake_ingest["staging_dir"].exists()
