import pandas as pd

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        
    @property
    def get_columns(self):
        if self.version == "2010":
            return "A:AP"
        if self.version == "2020":
            return "A:H,AQ:BX"
        raise ValueError("2010 and 2020 are only supported versions for decennial census data")

    def ingest(self) -> pd.DataFrame:
        df = pd.read_excel(self.path, sheet_name="2010, 2020, and Change", usecols=self.get_columns, skiprows=3)
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
