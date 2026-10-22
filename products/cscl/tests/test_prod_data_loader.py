"""prod_data_loader: fixed-width parsing and the `head` command."""

import sys
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

import prod_data_loader as pdl  # noqa: E402


@pytest.fixture
def dat_file(tmp_path):
    layout = tmp_path / "layout.csv"
    layout.write_text("field_name,start_index,end_index\nboro,1,1\nsegmentid,2,8\n")
    dat = tmp_path / "Test.dat"
    dat.write_text("".join(f"1{i:07d}\n" for i in range(15)) + "\x1a")
    return dat, layout


def test_parse_file_max_records(dat_file):
    dat, layout = dat_file
    assert len(pdl.parse_file(dat, layout)) == 15
    df = pdl.parse_file(dat, layout, max_records=10)
    assert len(df) == 10
    assert df.iloc[0].to_dict() == {"boro": "1", "segmentid": "0000000"}


def test_head_uses_the_files_formatting_csv(dat_file, monkeypatch):
    dat, layout = dat_file
    monkeypatch.setitem(
        pdl.datasets_by_filename,
        dat.name,
        pdl.OutputDataset("test", dat.name, "dat", formatting_path=layout),
    )
    result = CliRunner().invoke(pdl.app, ["head", dat.name, "-f", str(dat.parent)])
    assert result.exit_code == 0, result.output
    assert "segmentid" in result.output


def test_head_rejects_a_file_without_formatting():
    with pytest.raises(typer.BadParameter):
        pdl.head("not_in_recipe.dat", save_to_csv=False, folder=Path("."))
