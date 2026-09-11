from pathlib import Path
from unittest import mock
from zipfile import ZipFile

from dcpy.lifecycle.scripts import distribute_pluto_to_dof

VERSION = "26v2"
CSV = "bbl,council\n1000010010,1\n"


def _published_zip(tmp_path: Path) -> Path:
    zip_path = tmp_path / "pluto.zip"
    with ZipFile(zip_path, "w") as z:
        z.writestr("pluto.csv", CSV)
        z.writestr("version.txt", VERSION)
    return zip_path


def _run(tmp_path: Path, *, dry_run: bool, folder: str = "ToDOF"):
    """Run the script against a fake published zip, returning the mocked registry."""
    pushed = {}

    def record(key, filepath):
        pushed["key"] = key
        pushed["contents"] = Path(filepath).read_text()

    connectors = mock.MagicMock()
    connectors.push.__getitem__.return_value.push.side_effect = record

    with (
        mock.patch.object(
            distribute_pluto_to_dof.publishing,
            "download_file",
            return_value=_published_zip(tmp_path),
        ),
        mock.patch.object(distribute_pluto_to_dof, "connectors", connectors),
    ):
        distribute_pluto_to_dof.run(version=VERSION, folder=folder, dry_run=dry_run)
    return connectors, pushed


def test_pushes_csv_to_dof_folder(tmp_path):
    connectors, pushed = _run(tmp_path, dry_run=False)

    connectors.push.__getitem__.assert_called_once_with("axway")
    assert pushed["key"] == "ToDOF/pluto.csv"
    assert pushed["contents"] == CSV


def test_folder_is_configurable(tmp_path):
    _, pushed = _run(tmp_path, dry_run=False, folder="ToDOF (ptsaxway@finance.nyc.gov)")

    assert pushed["key"] == "ToDOF (ptsaxway@finance.nyc.gov)/pluto.csv"


def test_dry_run_does_not_push(tmp_path):
    connectors, pushed = _run(tmp_path, dry_run=True)

    connectors.push.__getitem__.assert_not_called()
    assert pushed == {}
