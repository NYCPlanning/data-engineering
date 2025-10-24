import json
from pathlib import Path
from pydantic import BaseModel
from tempfile import TemporaryDirectory
import yaml

from dcpy.connectors.registry import VersionedConnector
from dcpy.connectors.hybrid_pathed_storage import PathedStorageConnector
from dcpy.models.connectors.edm.recipes import DatasetType
from dcpy.models.lifecycle.ingest import SparseConfig

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

        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / config_filename
            with open(config_path, "w") as f:
                f.write(
                    json.dumps(
                        config.model_dump(exclude_none=True, mode="json"), indent=4
                    )
                )
            self.storage.push(
                f"{dest_folder_path}/{filepath.name}",
                filepath=filepath,
                acl=acl,
            )
            self.storage.push(
                f"{dest_folder_path}/config.json",
                filepath=config_path,
                acl=acl,
            )

            if latest:
                latest_folder_path = f"{key}/latest"
                self.storage.push(
                    f"{latest_folder_path}/{filepath.name}",
                    filepath=filepath,
                    acl=acl,
                )
                self.storage.push(
                    f"{latest_folder_path}/config.json",
                    filepath=config_path,
                    acl=acl,
                )
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
        return self.storage.pull(
            f"{key}/{version}/{filename}",
            destination_path,
        )

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

    def __str__(self) -> str:
        return f"ingest datastore connector at {self.storage.storage}"
