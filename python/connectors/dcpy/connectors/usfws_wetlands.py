"""USFWS National Wetlands Inventory, downloaded by HU8 watershed.

NWI publishes a geodatabase zip per watershed and per state. A few watersheds cover NYC at a
fraction of the statewide file's size, so a pull downloads each one and concatenates their
wetlands layers.
"""

import zipfile
from email.utils import parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

BASE_URL = "https://documentst.ecosphere.fws.gov/wetlands/downloads/watershed"
TIMEOUT = (10, 300)


def zip_url(watershed: str) -> str:
    return f"{BASE_URL}/HU8_{watershed}_Watershed.zip"


class USFWSWetlandsConnector(Pull):
    conn_type: str = "usfws_wetlands"

    def get_latest_version(self, key: str, *, watersheds: list[str], **kwargs) -> str:
        """Date the most recently published watershed zip was last modified."""
        dates = []
        for watershed in watersheds:
            resp = requests.head(zip_url(watershed), timeout=TIMEOUT)
            resp.raise_for_status()
            dates.append(parsedate_to_datetime(resp.headers["Last-Modified"]))
        return max(dates).strftime("%Y%m%d")

    def pull(
        self, key: str, destination_path: Path, *, watersheds: list[str], **kwargs
    ) -> dict:
        import geopandas as gpd

        frames = []
        # Ingest treats every folder in destination_path as a dataset, so extract elsewhere.
        with TemporaryDirectory() as tmp:
            for watershed in watersheds:
                resp = requests.get(zip_url(watershed), timeout=TIMEOUT)
                resp.raise_for_status()
                with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                    zf.extractall(Path(tmp) / watershed)
                name = f"HU8_{watershed}"
                gdf = gpd.read_file(
                    Path(tmp) / watershed / f"{name}_Watershed" / f"{name}.gdb",
                    layer=f"{name}_Wetlands",
                )
                logger.info(f"{len(gdf)} wetlands in watershed {watershed}")
                frames.append(gdf)

        path = destination_path / f"{key}.parquet"
        gpd.GeoDataFrame(pd.concat(frames, ignore_index=True)).to_parquet(
            path, index=False
        )
        return {"path": path}
