# from asyncio.windows_events import NULL
import pandas as pd

from dcpy.connectors.edm import recipes

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    @property
    def previous_version(self) -> str:
        version = self.source.get("previous_version")
        if version is None:
            raise Exception(
                "Cofos requires a previous version specified in its yml input."
            )
        return str(version)

    def ingest(self) -> pd.DataFrame:
        df = pd.read_csv(self.source["path"], dtype=str)
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
        previous_dataset = recipes.Dataset(
            id=self.name,
            version=self.previous_version,
        )
        return recipes.read_df(previous_dataset)

    def runner(self) -> str:
        previous = self.previous()
        new = self.ingest()
        new.columns = previous.columns
        df = pd.concat([previous, new])
        df = df.drop_duplicates()
        local_path = df_to_tempfile(df)
        return local_path
