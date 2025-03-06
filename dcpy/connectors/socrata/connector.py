from pathlib import Path

from dcpy.connectors import VersionedConnector
from dcpy.connectors.socrata import extract, publish


class Connector(VersionedConnector):
    conn_type = "socrata"

    def push(self, key: str, version, push_conf: dict | None = {}) -> dict:
        raise NotImplementedError("Sorry :)")

    def pull(
        self,
        key: str,
        version: str,
        destination_path: Path,
        **pull_conf,
    ) -> dict:
        source = extract.Source(
            type="socrata", org=pull_conf["org"], uid=key, format=pull_conf["format"]
        )
        extract.download(source, destination_path)
        return {"path": destination_path}

    def list_versions(self, key: str, sort_desc: bool = True) -> list[str]:
        return [self.query_latest_version(key)]

    def query_latest_version(self, key: str, **conf) -> str:
        source = extract.Source(
            type="socrata", org=conf["org"], uid=key, format=conf["format"]
        )
        return extract.get_version(source)

    def version_exists(self, key: str, version: str) -> bool:
        return False
