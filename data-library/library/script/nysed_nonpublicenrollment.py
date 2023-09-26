import pandas as pd

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        version_in_xlsx_tab = self.version[2:]  # e.g. "2022-23" -> "22-23"
        return pd.read_excel(
            self.path, sheet_name=f"NonPubEnroll_byGrade_{version_in_xlsx_tab}"
        )

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
