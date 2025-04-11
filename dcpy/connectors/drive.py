import shutil
from pathlib import Path

from dcpy.utils.logging import logger
from dcpy.connectors.registry import StorageConnector


class Connector(StorageConnector):
    conn_type: str = "drive"
    path: Path | None = None

    def _path(self, key: str, drive_path: Path | None = None) -> Path:
        if self.path:
            return self.path / key
        elif drive_path:
            return drive_path / key
        else:
            return Path(key)

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
