from pathlib import Path
import pytest

from dcpy.connectors.filesystem import Connector


@pytest.fixture()
def tmp_file(tmp_path: Path) -> Path:
    file = tmp_path / "test.csv"
    file.touch()
    return file


class TestConnector:
    connector = Connector()

    def test_path(self):
        cwd = Path(".")
        file = "test"
        assert self.connector._path(file) == Path(file)
        assert self.connector._path(file, cwd) == cwd / file
        assert Connector(path=cwd)._path(file) == cwd / file

    def test_subfolder(self, tmp_path: Path):
        assert self.connector.get_subfolders(str(tmp_path)) == []
        folder1 = "folder"
        filename = "file.txt"
        (tmp_path / folder1).mkdir()
        (tmp_path / folder1 / filename).touch()
        (tmp_path / filename).touch()
        assert self.connector.get_subfolders(str(tmp_path)) == ["folder"]

    def test_exists(self, tmp_path: Path):
        filepath = tmp_path / "file.txt"
        assert not self.connector.exists(str(filepath))
        filepath.touch()
        assert self.connector.exists(str(filepath))

    def test_pull(self, tmp_file: Path, tmp_path: Path):
        file = tmp_path / "test2.csv"
        assert not file.exists()
        self.connector.pull(key=str(tmp_file), destination_path=file)
        assert file.exists()

    # next two are a little duplicated with _path test
    # but this is the more important functionality to test
    def test_pull_drive_kwarg(self, tmp_file: Path, tmp_path: Path):
        filename = "test2.csv"
        file = tmp_path / filename
        assert not file.exists()
        self.connector.pull(
            key=tmp_file.name, destination_path=file, path_prefix=tmp_path
        )
        assert file.exists()

    def test_pull_drive_with_path(self, tmp_file: Path, tmp_path: Path):
        connector = Connector(path=tmp_path)
        filename = "test2.csv"
        file = tmp_path / filename
        assert not file.exists()
        connector.pull(key=tmp_file.name, destination_path=file)
        assert file.exists()

    def test_push(self, tmp_file: Path, tmp_path: Path):
        output_file = tmp_path / "test2.csv"
        assert not output_file.exists()
        self.connector.push(key=str(output_file), filepath=tmp_file)
        assert output_file.exists()

    def test_push_with_subfolder(self, tmp_file: Path, tmp_path: Path):
        output_file = tmp_path / "subfolder" / "test2.csv"
        assert not output_file.exists()
        self.connector.push(key=str(output_file), filepath=tmp_file)
        assert output_file.exists()

    # next two are a little duplicated with _path test
    # but this is the more important functionality to test
    def test_push_drive_kwarg(self, tmp_file: Path, tmp_path: Path):
        key = "test2.csv"
        output_file = tmp_path / key
        assert not output_file.exists()
        self.connector.push(key=key, filepath=tmp_file, path_prefix=tmp_path)
        assert output_file.exists()

    def test_push_drive_with_path(self, tmp_file: Path, tmp_path: Path):
        connector = Connector(path=tmp_path)
        key = "test2.csv"
        output_file = tmp_path / key
        assert not output_file.exists()
        connector.push(key=key, filepath=tmp_file)
        assert output_file.exists()
