"""build_prod_snapshot: the legacy-outputs preflight and the overwrite guard."""

import sys
from pathlib import Path

import pytest
import typer

sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

import build_prod_snapshot as bps  # noqa: E402


def test_expected_files_cover_flat_files_ldf_and_district_gdb():
    expected = bps.expected_files("26d")
    for f in ["SND.txt", "BronxLION.dat", "LDF.dat", "LDF.header"]:
        assert f in expected
    assert "v26D_Districts.gdb.zip" in expected
    # exports with no prod file to compare (compare_file off) aren't expected
    off = {d.file_name for d in bps.pdl.datasets_by_name.values() if not d.compare_file}
    assert not off & set(expected) - {"LDF.dat", "LDF.header"}


def test_found_files_lists_only_top_level_files(monkeypatch):
    keys = ["cscl_etl/26d/", "cscl_etl/26d/SND.txt", "cscl_etl/26d/x.gdb/a.gdbtable"]
    monkeypatch.setattr(
        bps.s3, "list_objects", lambda bucket, prefix: [{"Key": k} for k in keys]
    )
    assert bps.found_files("26d") == {"SND.txt"}


def test_missing_inputs_lists_missing_files_and_lion_gdb(monkeypatch):
    expected = bps.expected_files("26d")
    monkeypatch.setattr(bps, "found_files", lambda v: set(expected) - {"SND.txt"})
    monkeypatch.setattr(bps, "url_exists", lambda url: False)
    assert bps.missing_inputs("26d") == [
        "cscl_etl/26d/SND.txt",
        bps.LION_GDB_URL.format(version="26d"),
    ]
    monkeypatch.setattr(bps, "found_files", lambda v: set(expected))
    monkeypatch.setattr(bps, "url_exists", lambda url: True)
    assert bps.missing_inputs("26d") == []


def test_build_refuses_to_replace_an_existing_snapshot(monkeypatch):
    monkeypatch.setattr(bps.s3, "object_exists", lambda bucket, key: True)
    monkeypatch.setattr(bps, "build", lambda *a: pytest.fail("must not build"))
    with pytest.raises(typer.Exit):
        bps._build("26c", Path("/tmp"), upload=True, overwrite=False)


def test_check_snapshot_fails_when_missing(monkeypatch):
    monkeypatch.setattr(bps.s3, "object_exists", lambda bucket, key: False)
    with pytest.raises(typer.Exit):
        bps._check_snapshot("26d")
    monkeypatch.setattr(bps.s3, "object_exists", lambda bucket, key: True)
    bps._check_snapshot("26d")
