import os
from zipfile import ZipFile

import pandas as pd
import requests

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        r = requests.get(self.source["path"], stream=True)
        with open(f"pad{self.version}.zip", "wb") as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        with ZipFile(f"pad{self.version}.zip", "r") as zip:
            zip.extract("bobaadr.txt")
            df = pd.read_csv("bobaadr.txt", dtype=str)
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        os.remove(f"pad{self.version}.zip")
        os.remove("bobaadr.txt")
        return local_path
