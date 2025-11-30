import pandas as pd

from dcpy.utils import s3

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        data = s3.get_file_as_stream(
            "edm-private",
            f"dob_now/dob_now_permits/DOB_Now_Permit_Filing_Data_for_DCP_{self.version}.csv",
        )
        df = pd.read_csv(data, encoding="cp1252", sep="\t")
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
