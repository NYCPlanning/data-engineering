import pandas as pd

from . import get_json_content, df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        data = get_json_content(self.source["path"])

        df = pd.DataFrame(data)
        df = df[["TrackerID", "FMSID", "Title", "TotalFunding", "Locations"]]
        df["Locations"] = df["Locations"].apply(lambda x: x.get("Location"))
        df2 = df.drop(columns=["Locations"]).join(df["Locations"].explode().to_frame())
        horiz_exploded = pd.json_normalize(df2["Locations"].to_list())
        horiz_exploded.index = df2.index
        df3 = pd.concat([df2, horiz_exploded], axis=1).drop(columns=["Locations"])
        df3 = df3.rename(
            columns={
                "TrackerID": "proj_id",
                "FMSID": "fmsid",
                "Title": "desc",
                "TotalFunding": "total_funding",
                "ParkID": "park_id",
                "Latitude": "lat",
                "Longitude": "lon",
            }
        )
        df3 = df3[
            ["proj_id", "fmsid", "desc", "total_funding", "park_id", "lat", "lon"]
        ]
        return df3

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
