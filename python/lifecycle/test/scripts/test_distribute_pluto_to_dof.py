from pathlib import Path
from unittest import mock
from zipfile import ZipFile

import pytest

from dcpy.lifecycle.scripts import distribute_pluto_to_dof

VERSION = "26v2"
CSV = "bbl,council\n1000010010,1\n"
FOLDERS = ["FromDOF (ptsaxway@finance.nyc.gov)", "ToDOF"]


def _published_zip(tmp_path: Path) -> Path:
    zip_path = tmp_path / "pluto.zip"
    with ZipFile(zip_path, "w") as z:
        z.writestr("pluto.csv", CSV)
        z.writestr("version.txt", VERSION)
    return zip_path


def _run(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    folder: str = "ToDOF",
    subfolders: list[str] = FOLDERS,
):
    """Run the script against a fake published zip, returning the mocked connector."""
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
            return_value=_published_zip(tmp_path),
        ),
        mock.patch.object(distribute_pluto_to_dof, "connectors", registry),
    ):
        distribute_pluto_to_dof.run(version=VERSION, folder=folder, dry_run=dry_run)
    return axway, pushed


def test_pushes_csv_to_dof_folder(tmp_path):
    _, pushed = _run(tmp_path)

    assert pushed["key"] == "ToDOF/pluto.csv"
    assert pushed["contents"] == CSV


def test_version_txt_is_not_pushed(tmp_path):
    axway, _ = _run(tmp_path)

    assert axway.push.call_count == 1
    assert axway.push.call_args.kwargs["filepath"].name == "pluto.csv"


def test_nested_folder_lists_its_own_parent(tmp_path):
    axway, pushed = _run(tmp_path, folder="outbound/ToDOF", subfolders=["ToDOF"])

    axway.get_subfolders.assert_called_once_with("outbound")
    assert pushed["key"] == "outbound/ToDOF/pluto.csv"


def test_unknown_folder_is_refused(tmp_path):
    with pytest.raises(ValueError, match="No folder 'ToDOF'"):
        _run(tmp_path, subfolders=["FromDOF (ptsaxway@finance.nyc.gov)"])


def test_dry_run_lists_folders_but_does_not_push(tmp_path):
    axway, pushed = _run(tmp_path, dry_run=True)

    axway.get_subfolders.assert_called_once_with(".")
    axway.push.assert_not_called()
    assert pushed == {}
