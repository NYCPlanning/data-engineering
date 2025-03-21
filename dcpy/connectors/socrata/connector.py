from pathlib import Path

from dcpy.models.connectors import socrata
from dcpy.connectors.registry import Connector
from dcpy.connectors.socrata import extract


class SocrataConnector(Connector):
    conn_type: str = "socrata"

    def push(self, key: str, **kwargs) -> dict:
        raise NotImplementedError("Sorry :)")

    def pull(  # type: ignore[override]
        self,
        key: str,
        destination_path: Path,
        *,
        org: socrata.Org,
        format: socrata.ValidSourceFormats,
        **kwargs,
    ) -> dict:
        source = extract.Source(type="socrata", org=org, uid=key, format=format)
        extract.download(source, destination_path)
        return {"path": destination_path}

    def get_latest_version(  # type: ignore[override]
        self,
        key: str,
        *,
        org: socrata.Org,
        format: socrata.ValidSourceFormats,
        **kwargs,
    ) -> str:
        source = extract.Source(type="socrata", org=org, uid=key, format=format)
        return extract.get_version(source)
