import json
from pathlib import Path
from tempfile import TemporaryDirectory
import yaml

from dcpy.connectors.registry import (
    StorageConnector,
    VersionedConnector,
)
from dcpy.models.lifecycle import ingest

config_filename = "config.json"


class Connector(VersionedConnector):
    conn_type: str = "ingest_datastore"
    storage: StorageConnector

    def _push(
        self,
        key: str,
        *,
        version: str,
        filepath: Path,
        config: ingest.Config,
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
                acl=config.archival.acl,
            )
            self.storage.push(
                f"{dest_folder_path}/config.json",
                filepath=config_path,
                acl=config.archival.acl,
            )

            if latest:
                latest_folder_path = f"{key}/latest"
                self.storage.push(
                    f"{latest_folder_path}/{filepath.name}",
                    filepath=filepath,
                    acl=config.archival.acl,
                )
                self.storage.push(
                    f"{latest_folder_path}/config.json",
                    filepath=config_path,
                    acl=config.archival.acl,
                )
        return {}

    def push_versioned(self, key: str, version: str, **kwargs) -> dict:
        return self._push(key, version=version, **kwargs)

    def pull_versioned(
        self, key: str, version: str, destination_path: Path, **kwargs
    ) -> dict:
        return self.storage.pull(
            f"{key}/{version}/{key}.parquet",  # TODO a little hacky
            destination_path / key / version,
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

    def get_config(self, key: str, version: str) -> ingest.Config:
        return ingest.Config(**self._get_config_obj(key, version))

    def try_get_config(self, key: str, version: str) -> ingest.Config | None:
        """for backwards compatibility"""
        obj = self._get_config_obj(key, version)
        if "dataset" not in obj:  # very specific to library
            return ingest.Config(**obj)
        else:
            return None

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self._get_config_obj(key, "latest")["version"]

    def version_exists(self, key: str, version: str, **kwargs) -> bool:
        return self.storage.exists(f"{key}/{version}/{config_filename}")
