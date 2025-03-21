from pathlib import Path

from dcpy.models.connectors import socrata
from dcpy.connectors.socrata import extract
from dcpy.connectors.registry import NonVersionedConnector


class Connector(NonVersionedConnector):
    conn_type = "socrata"

    def push(self, key: str, push_conf: dict | None = {}) -> dict:
        raise NotImplementedError("Sorry :)")

    def pull(
        self, key: str, destination_path: Path, org: socrata.Org, format: str, **kwargs
    ) -> dict:
        source = extract.Source(type="socrata", org=org, uid=key, format=format)
        extract.download(source, destination_path)
        return {"path": destination_path}

    def get_current_version(self, key: str, **conf) -> str:
        source = extract.Source(
            type="socrata", org=conf["org"], uid=key, format=conf["format"]
        )
        return extract.get_version(source)

    def version_exists(self, key: str, version: str) -> bool:
        return False
