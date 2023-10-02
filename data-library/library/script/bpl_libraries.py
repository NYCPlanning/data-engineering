import pandas as pd

from . import get_json_content, df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        content = get_json_content(self.path)
        data = []
        for i in content["locations"]:
            data.append(i["data"])
        df = pd.DataFrame.from_dict(data, orient="columns")
        df.loc[df["position"] == "", "position"] = ","
        df["latitude"] = df.position.apply(lambda x: x.split(",")[0].strip())
        df["longitude"] = df.position.apply(lambda x: x.split(",")[1].strip())
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
