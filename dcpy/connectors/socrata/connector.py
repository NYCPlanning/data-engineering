from pathlib import Path

from dcpy.models.connectors import socrata
from dcpy.connectors.registry import Connector
from dcpy.connectors.socrata import extract


class SocrataConnector(Connector):
    conn_type: str = "socrata"

    def push(self, key: str, **kwargs) -> dict:
        raise NotImplementedError()

    def _pull(
        self,
        key: str,
        destination_path: Path,
        *,
        org: socrata.Org,
        format: socrata.ValidSourceFormats,
        **kwargs,
    ) -> dict:
        source = extract.Source(type="socrata", org=org, uid=key, format=format)
        extension = "shp.zip" if format == "shapefile" else format
        filepath = destination_path / f"{key}.{extension}"
        extract.download(source, filepath)
        return {"path": filepath}

    def pull(self, key: str, destination_path: Path, **kwargs) -> dict:
        return self._pull(key, destination_path, **kwargs)

    def get_date_last_updated(
        self,
        key: str,
        *,
        org: socrata.Org,
        format: socrata.ValidSourceFormats,
        **kwargs,
    ) -> str:
        source = extract.Source(type="socrata", org=org, uid=key, format=format)
        return extract.get_version(source)

    def get_latest_version(self, key: str, **kwargs) -> str:
        return self.get_date_last_updated(key, **kwargs)
