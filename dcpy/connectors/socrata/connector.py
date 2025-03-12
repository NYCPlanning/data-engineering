from pathlib import Path

from dcpy.connectors.registry import NonVersionedConnector
from dcpy.connectors.socrata import extract


class Connector(NonVersionedConnector):
    conn_type = "socrata"

    def push(self, key: str, push_conf: dict | None = {}) -> dict:
        raise NotImplementedError("Sorry :)")

    def pull(
        self,
        key: str,
        destination_path: Path,
        pull_conf: dict | None = None,
    ) -> dict:
        pull_conf = pull_conf or {}
        source = extract.Source(
            type="socrata", org=pull_conf["org"], uid=key, format=pull_conf["format"]
        )
        extract.download(source, destination_path)
        return {"path": destination_path}

    def get_current_version(self, key: str, **conf) -> str:
        source = extract.Source(
            type="socrata", org=conf["org"], uid=key, format=conf["format"]
        )
        return extract.get_version(source)

    def version_exists(self, key: str, version: str) -> bool:
        return False
