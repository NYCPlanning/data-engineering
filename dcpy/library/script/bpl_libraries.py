import pandas as pd

from . import df_to_tempfile, get_json_content
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        content = get_json_content(self.source["path"])
        data = []
        for i in content["locations"]:
            data.append(i["data"])
        df = pd.DataFrame.from_records(data)
        df.loc[df["position"] == "", "position"] = ","
        df["latitude"] = df.position.apply(lambda x: x.split(",")[0].strip())
        df["longitude"] = df.position.apply(lambda x: x.split(",")[1].strip())
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
