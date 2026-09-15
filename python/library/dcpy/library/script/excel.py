import pandas as pd

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def runner(self) -> str:
        df = pd.read_excel(self.source["path"], sheet_name=self.source["sheet_name"])
        local_path = df_to_tempfile(df)
        return local_path
