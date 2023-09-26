import pandas as pd

from . import get_json_content, df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        records = get_json_content(self.path)["locations"]
        data = []
        for i in records:
            parsed = dict(
                lon=str(i["geolocation"]["coordinates"][0]),
                lat=str(i["geolocation"]["coordinates"][1]),
                name=i["name"],
                zipcode=i["postal_code"],
                address=i["street_address"],
                locality=i["locality"],
                region=i["region"],
            )
            data.append(parsed)
        df = pd.DataFrame.from_dict(data, orient="columns")
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
