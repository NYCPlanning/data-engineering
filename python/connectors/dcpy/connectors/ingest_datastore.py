import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
from pydantic import BaseModel

from dcpy.connectors.edm.models import DatasetType
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector
from dcpy.connectors.registry import VersionedConnector
from dcpy.lifecycle.ingest.models import SparseConfig
from dcpy.utils.logging import logger

config_filename = "config.json"


class Connector(VersionedConnector, arbitrary_types_allowed=True):
    conn_type: str = "ingest_datastore"
    storage: PathedStorageConnector

    def _push(
        self,
        key: str,
        *,
        version: str,
        acl: str | None = None,
        filepath: Path,
        config: BaseModel,
        overwrite: bool = False,
        latest: bool = False,
        **kwargs,
    ) -> dict:
        dest_folder_path = f"{key}/{version}"

        dest_key = f"{key}/{version}/{config_filename}"
        if self.storage.exists(dest_key) and not overwrite:
            raise Exception(
                f"Archived dataset '{dest_key}' already exists for connector {self.conn_type}, cannot overwrite"
            )

        if overwrite and self.storage.exists(dest_folder_path):
            self._set_aside_library_archive(key, version, acl=acl)

        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / config_filename
            with open(config_path, "w") as f:
                f.write(
                    json.dumps(
                        config.model_dump(exclude_none=True, mode="json"), indent=4
                    )
                )
            # Pushed as a folder so it replaces whatever was there, for the same reason
            # as `latest` (see push_latest): builds pick a file type from this folder.
            version_dir = Path(tmp_dir) / version
            version_dir.mkdir()
            shutil.copy(filepath, version_dir / filepath.name)
            shutil.copy(config_path, version_dir / config_filename)
            self.storage.push(dest_folder_path, filepath=version_dir, acl=acl)

            if latest:
                self.push_latest(
                    key, filepath=filepath, config_path=config_path, acl=acl
                )
        return {}

    def _set_aside_library_archive(
        self, key: str, version: str, acl: str | None = None
    ) -> None:
        """Move a library-era archive at this version to `<version>_library/`.

        An ingest archive being overwritten is simply replaced. A library one is kept,
        since its files (including its parquet) have no other copy.
        """
        folder = f"{key}/{version}"
        if self.storage.exists(f"{folder}/{config_filename}") and not self._is_library(
            key, version
        ):
            return
        dest = f"{folder}_library"
        logger.info(f"Moving library archive {folder} to {dest}")
        with TemporaryDirectory() as tmp_dir:
            local = Path(tmp_dir) / "library"
            self.storage.pull(folder, local)
            self.storage.push(dest, filepath=local, acl=acl, merge=True)

    def push_latest(
        self,
        key: str,
        *,
        filepath: Path,
        config_path: Path,
        acl: str | None = None,
    ) -> dict:
        """Point `latest` at one archive's files.

        Pushed as a folder so it replaces whatever was there. Writing the files
        individually leaves everything else in place, and a library-era .sql sitting
        beside a new parquet wins file-type resolution, so consumers silently keep
        reading the older archive.
        """
        with TemporaryDirectory() as tmp_dir:
            latest_dir = Path(tmp_dir) / "latest"
            latest_dir.mkdir()
            shutil.copy(filepath, latest_dir / filepath.name)
            shutil.copy(config_path, latest_dir / config_filename)
            self.storage.push(f"{key}/latest", filepath=latest_dir, acl=acl)
        return {}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        return self._push(key, version=version, **kwargs)

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        filename = kwargs.get(
            "filename",
            f"{key}.{DatasetType(kwargs.get('file_type', 'parquet')).to_extension()}",
        )
        # Versions are immutable, so a local copy from an earlier pull is reused. Pulls
        # land under a temp name first, so an interrupted download is never mistaken for one.
        local_path = destination_path / filename
        if local_path.exists():
            logger.info(f"Using cached {local_path}")
            return {"path": local_path}
        partial_path = local_path.with_name(f"{local_path.name}.partial")
        self.storage.pull(f"{key}/{version}/{filename}", partial_path)
        partial_path.replace(local_path)
        return {"path": local_path}

    def list_versions(self, key: str, *, sort_desc: bool = True, **kwargs) -> list[str]:
        """This is maybe a problem in my plan"""
        return sorted(self.storage.get_subfolders(key), reverse=sort_desc)

    def _get_config_obj(self, key: str, version: str) -> dict:
        with TemporaryDirectory() as tmp_dir:
            self.storage.pull(
                f"{key}/{version}/{config_filename}", destination_path=Path(tmp_dir)
            )
            with open(Path(tmp_dir) / config_filename, "r", encoding="utf-8") as raw:
                return yaml.safe_load(raw.read())

    def get_sparse_config(self, key: str, version: str) -> SparseConfig:
        return SparseConfig(**self._get_config_obj(key, version))

    def _is_library(self, key: str, version: str) -> bool:
        """
        DCP-specific check for backwards compatibility - checks if dataset was
        archived by our old tool, `library`

        Not intended for general use in dcpy
        """
        config_dict = self._get_config_obj(key, version)
        return "dataset" in config_dict

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.get_sparse_config(key, "latest").version

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return self.storage.exists(f"{key}/{version}")

    def get_name(self, key: str, version: str) -> str | None:
        return self.get_sparse_config(key, version).name

    def __str__(self) -> str:
        return f"ingest datastore connector at {self.storage.storage}"
