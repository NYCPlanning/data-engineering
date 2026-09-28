from pathlib import Path
from unittest import mock

import pytest

from dcpy.lifecycle.scripts import distribute_pluto_to_dof

VERSION = "26v2"
CSV = "bbl,council,latitude,longitude,schooldist\n1000010010,1,40.6892494,-74.0445004,02\n"
FOLDERS = ["FromDOF (ptsaxway@finance.nyc.gov)", "ToDOF"]


def _published_csv(tmp_path: Path) -> Path:
    csv_path = tmp_path / "pluto.csv"
    csv_path.write_text(CSV)
    return csv_path


def _run(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    folder: str = "ToDOF",
    subfolders: list[str] = FOLDERS,
):
    """Run the script against a fake published csv, returning the mocked connector."""
    pushed = {}

    def record(key, filepath):
        pushed["key"] = key
        pushed["contents"] = Path(filepath).read_text()

    axway = mock.MagicMock()
    axway.get_subfolders.return_value = subfolders
    axway.push.side_effect = record
    registry = mock.MagicMock()
    registry.__getitem__.return_value = axway

    with (
        mock.patch.object(
            distribute_pluto_to_dof.publishing,
            "download_file",
            return_value=_published_csv(tmp_path),
        ) as download,
        mock.patch.object(distribute_pluto_to_dof, "connectors", registry),
    ):
        distribute_pluto_to_dof.run(version=VERSION, folder=folder, dry_run=dry_run)
    return axway, pushed, download


def test_pushes_csv_to_dof_folder(tmp_path):
    _, pushed, _ = _run(tmp_path)

    assert pushed["key"] == "ToDOF/pluto.csv"
    assert pushed["contents"] == CSV


def test_reads_the_csv_06_export_writes(tmp_path):
    _, _, download = _run(tmp_path)

    assert download.call_args.args[1] == "dof/pluto.csv"


def test_nested_folder_lists_its_own_parent(tmp_path):
    axway, pushed, _ = _run(tmp_path, folder="outbound/ToDOF", subfolders=["ToDOF"])

    axway.get_subfolders.assert_called_once_with("outbound")
    assert pushed["key"] == "outbound/ToDOF/pluto.csv"


def test_unknown_folder_is_refused(tmp_path):
    with pytest.raises(ValueError, match="No folder 'ToDOF'"):
        _run(tmp_path, subfolders=["FromDOF (ptsaxway@finance.nyc.gov)"])


def test_dry_run_lists_folders_but_does_not_push(tmp_path):
    axway, pushed, _ = _run(tmp_path, dry_run=True)

    axway.get_subfolders.assert_called_once_with(".")
    axway.push.assert_not_called()
    assert pushed == {}
