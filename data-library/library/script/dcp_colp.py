## Not typically used, but was once needed when input data was malformed (usecode as int rather than varchar, so missing leading zeroes)

import pandas as pd
from zipfile import ZipFile
import requests
import math

from . import df_to_tempfile
from .scriptor import ScriptorInterface


def myfunc(n):
    if math.isnan(n):
        return ""
    else:
        return str(math.floor(n))


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        r = requests.get(self.path, stream=True)
        with open(f"nyc_colp_csv_{self.version}.zip", "wb") as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        with ZipFile(f"nyc_colp_csv_{self.version}.zip", "r") as zip:
            zip.extract(f"colp_{self.version}.csv")
            df = pd.read_csv(f"colp_{self.version}.csv")
        df["USECODE"] = df["USECODE"].apply(lambda i: f"{i:04d}")
        df["CATEGORY"] = df["CATEGORY"].apply(myfunc)
        df["EXPANDCAT"] = df["EXPANDCAT"].apply(myfunc)

        print(df[df["USETYPE"] == "OFFICE"]["CATEGORY"])
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
