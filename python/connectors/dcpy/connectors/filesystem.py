import shutil
from pathlib import Path

from dcpy.connectors.registry import StorageConnector
from dcpy.utils.logging import logger


class Connector(StorageConnector):
    conn_type: str = "filesystem"
    path: Path | None = None

    def _path(self, key: str, path_prefix: Path | None = None) -> Path:
        if self.path:
            path = self.path / key
        elif path_prefix:
            path = path_prefix / key
        else:
            path = Path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def exists(self, key, path_prefix: Path | None = None) -> bool:
        return self._path(key, path_prefix).exists()

    def get_subfolders(self, prefix: str, path_prefix: Path | None = None) -> list[str]:
        path = self._path(prefix, path_prefix)
        return [str(p.relative_to(path)) for p in path.glob("*") if p.is_dir()]

    def pull(
        self,
        key: str,
        destination_path: Path,
        *,
        path_prefix: Path | None = None,
        **kwargs,
    ) -> dict:
        source_path = self._path(key, path_prefix)
        if source_path != destination_path:
            shutil.copy(source_path, destination_path)
        if destination_path.is_file():
            return {"path": destination_path}
        else:
            return {"path": destination_path / source_path.name}

    def _push(
        self,
        key: str,
        *,
        filepath: Path,
        path_prefix: Path | None = None,
        overwrite: bool = False,
        **kwargs,
    ) -> dict:
        path = self._path(key, path_prefix)

        if path.exists() and not overwrite:
            raise Exception(f"'{path}' already exists.")

        logger.info(f"Copying {filepath} to {path}")
        shutil.copy(filepath, path)

        return {"path": path}

    def push(self, key: str, **kwargs) -> dict:
        return self._push(key, **kwargs)
