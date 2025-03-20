import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Tuple
import yaml

from dcpy.connectors.registry import (
    StorageConnector,
    VersionedConnector,
    ConnectorRegistry,
)
from dcpy.models.lifecycle import ingest

config_filename = "config.json"


def _conf_to_push_args(conf: dict | None) -> Tuple[Path, ingest.Config]:
    assert (
        conf
        and isinstance(conf.get("config"), ingest.Config)
        and isinstance(conf.get("filepath"), Path)
    )
    return conf["filepath"], conf["config"]


class Connector(VersionedConnector):
    conn_type = "ingest_datastore"
    _storage: StorageConnector

    def __init__(self, storage_type: str, registry: ConnectorRegistry):
        sub = registry.get_subregistry(StorageConnector)
        self._storage = sub[storage_type]

    def push(self, key: str, version: str, push_conf: dict | None = {}) -> dict:
        dest_folder_path = f"{key}/{version}"
        path, config = _conf_to_push_args(push_conf)
        assert push_conf

        dest_key = f"{key}/{version}/{config_filename}"
        if self._storage.exists(dest_key) and not push_conf.get("overwrite"):
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
            self._storage.push(
                f"{dest_folder_path}/{path.name}",
                {"filepath": path, "acl": config.archival.acl},
            )
            self._storage.push(
                f"{dest_folder_path}/config.json",
                {"filepath": config_path, "acl": config.archival.acl},
            )

            if push_conf.get("latest"):
                latest_folder_path = f"{key}/latest"
                self._storage.push(
                    f"{latest_folder_path}/{path.name}",
                    {"filepath": path, "acl": config.archival.acl},
                )
                self._storage.push(
                    f"{latest_folder_path}/config.json",
                    {"filepath": config_path, "acl": config.archival.acl},
                )
        return {}

    def pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        pull_conf: dict | None = {},
    ) -> dict:
        raise NotImplementedError

    def list_versions(self, key: str, sort_desc: bool = True) -> list[str]:
        """This is maybe a problem in my plan"""
        suffixes = self._storage.list_with_prefix("key")
        return [Path(s).root for s in suffixes]

    def _get_config_obj(self, key: str, version: str) -> dict:
        with TemporaryDirectory() as tmp_dir:
            self._storage.pull(f"{key}/{version}/{config_filename}", Path(tmp_dir))
            with open(Path(tmp_dir) / config_filename, "r", encoding="utf-8") as raw:
                return yaml.safe_load(raw.read())

    def get_config(self, key: str, version: str):
        return ingest.Config(**self._get_config_obj(key, version))

    def query_latest_version(self, key: str, conf: dict | None = None) -> str:
        return self._get_config_obj(key, "latest")["version"]

    def version_exists(self, key: str, version: str) -> bool:
        return self._storage.exists(f"{key}/{version}/{config_filename}")

    def data_local_sub_path(
        self, key: str, version: str, pull_conf: Any | None = None
    ) -> Path:
        raise NotImplementedError()
