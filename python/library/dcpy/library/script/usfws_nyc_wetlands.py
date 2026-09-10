import zipfile
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import geopandas as gpd
import pandas as pd
import requests

from .scriptor import ScriptorInterface

# USFWS has datasets available by state or watershed
# NYS dataset is much larger than we need
# These four watersheds completely contains NYC
WATERSHEDS = [
    "02030101",
    "02030102",
    "02030104",
    "02030202",
]


class Scriptor(ScriptorInterface):
    def ingest(self) -> gpd.GeoDataFrame:
        df = gpd.GeoDataFrame()
        with TemporaryDirectory() as tmpdir:
            for ws in WATERSHEDS:
                resp = requests.get(
                    f"https://documentst.ecosphere.fws.gov/wetlands/downloads/watershed/HU8_{ws}_Watershed.zip",
                    stream=True,
                )
                with zipfile.ZipFile(BytesIO(resp.content)) as zip_ref:
                    zip_ref.extractall(Path(tmpdir) / ws)
            for ws in WATERSHEDS:
                _df: gpd.GeoDataFrame = gpd.read_file(
                    Path(tmpdir) / ws / f"HU8_{ws}_Watershed" / f"HU8_{ws}.gdb",
                    layer=f"HU8_{ws}_Wetlands",
                )
                df = gpd.GeoDataFrame(pd.concat((df, _df)))
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = "/tmp/usfws_nyc_wetlands.parquet"
        df.to_parquet("/tmp/usfws_nyc_wetlands.parquet", index=False)
        return local_path
