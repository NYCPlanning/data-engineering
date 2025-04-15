import shutil
from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.connectors.registry import StorageConnector


class Connector(StorageConnector):
    conn_type: str = "drive"
    path: Path | None = None

    def _path(self, key: str, drive_path: Path | None = None) -> Path:
        if self.path:
            path = self.path / key
        elif drive_path:
            path = drive_path / key
        else:
            path = Path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def exists(self, key, drive_path: Path | None = None) -> bool:
        return self._path(key, drive_path).exists()

    def get_subfolders(self, prefix: str, drive_path: Path | None = None) -> list[str]:
        path = self._path(prefix, drive_path)
        return [str(p.relative_to(path)) for p in path.glob("*") if p.is_dir()]

    def pull(
        self,
        key: str,
        destination_path: Path,
        *,
        drive_path: Path | None = None,
        **kwargs,
    ) -> dict:
        source_path = self._path(key, drive_path)
        if source_path != destination_path:
            shutil.copy(source_path, destination_path)
        return {"path": destination_path}

    def _push(
        self,
        key: str,
        *,
        filepath: Path,
        drive_path: Path | None = None,
        overwrite: bool = False,
        **kwargs,
    ) -> dict:
        path = self._path(key, drive_path)

        if path.exists() and not overwrite:
            raise Exception(f"'{path}' already exists.")

        logger.info(f"Copying {filepath} to {path}")
        shutil.copy(filepath, path)

        return {"path": path}

    def push(self, key: str, **kwargs) -> dict:
        return self._push(key, **kwargs)
