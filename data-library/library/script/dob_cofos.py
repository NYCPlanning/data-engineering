# from asyncio.windows_events import NULL
import pandas as pd

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    @property
    def previous_version(self) -> str:
        return self.__dict__["config"]["dataset"]["info"]["previous_version"]

    def ingest(self) -> pd.DataFrame:
        print(self.previous_version)
        df = pd.read_csv(self.path, dtype=str)
        df.insert(0, "v", self.version)
        # add the extra column and assign the missing columns to None
        df.insert(df.shape[1], "docstatus", None)
        df["CofO Status"] = None
        # clean the job number by removing the -1 flag
        df["Job number"] = df["Job number"].apply(lambda x: x.split("-")[0])
        # mapping the four values to the T-TCO and C-CO logics
        df["CofilingtypeLabel"] = df["CofilingtypeLabel"].map(
            {
                "Initial": "T- TCO",
                "Renewal With Change": "T- TCO",
                "Renewal Without Change": "T- TCO",
                "Final": "C- CO",
            }
        )
        return df

    def previous(self) -> pd.DataFrame:
        url = (
            f"s3://edm-recipes/datasets/dob_cofos/{self.previous_version}/dob_cofos.csv"
        )
        return pd.read_csv(url, dtype=str)

    def runner(self) -> str:
        new = self.ingest()
        previous = self.previous()
        new.columns = previous.columns
        df = pd.concat([previous, new])
        df = df.drop_duplicates()
        local_path = df_to_tempfile(df)
        return local_path
