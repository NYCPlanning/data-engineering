from datetime import datetime
import requests

from dcpy.connectors.edm import recipes


class Socrata:
    @classmethod
    def get_base_url(cls, source: recipes.ExtractConfig.Source.Socrata):
        return f"https://{source.org.server}/api"

    @classmethod
    def get_url(cls, source: recipes.ExtractConfig.Source.Socrata):
        base_url = cls.get_base_url(source)
        match source.format:
            case "csv":
                url = f"{base_url}/views/{source.uid}/rows.csv"
            case "geojson":
                url = f"{base_url}/geospatial/{source.uid}?method=export&format=GeoJSON"
            case "shapefile":
                url = (
                    f"{base_url}/geospatial/{source.uid}?method=export&format=Shapefile"
                )
            case format:
                raise Exception(f"Unsupported socrata format: '{format}'")
        return url

    @classmethod
    def get_version(cls, source: recipes.ExtractConfig.Source.Socrata):
        base_url = cls.get_base_url(source)
        resp = requests.get(f"{base_url}/views/{source.uid}.json")
        resp.raise_for_status()
        version = datetime.fromtimestamp(resp.json()["rowsUpdatedAt"]).strftime(
            "%Y%m%d"
        )
        return version
