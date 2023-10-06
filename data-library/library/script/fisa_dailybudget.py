from pathlib import Path
import pandas as pd
import re

from . import df_to_tempfile
from .scriptor import ScriptorInterface

HEADERS = [
    "RCAT_CD",
    "RCLS_CD",
    "ATYP_CD",
    "MNG_DPT_CD",
    "CPTL_PROJ_ID",
    "BUD_OBJ_CD",
    "AU_CD",
    "FNDG_DPT_CD",
    "CMTMNT_AM",
    "OBLGTNS_AM",
    "ADPT_AM",
    "PENC_AM",
    "ENC_AM",
    "ACRD_EXP_AM",
    "CASH_EXP_AM",
    "UCOMIT_AM",
    "ACTU_EXP_AM",
    "TBL_LAST_DT",
]


def dtype(column):
    if column < 8:
        return str
    else:
        return object


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def _read_df(self, file: Path) -> pd.DataFrame | None:
        match = re.match("^AIBL_DLY_BUD_96L1_(\d+)$", file.stem)
        if match:
            timestamp = match.group(1)
            df = pd.read_csv(file, delimiter="|", header=None, dtype=str)
            df.drop(18, axis=1, inplace=True)
            df.columns = pd.Index(HEADERS)
            df["fisa_version"] = timestamp
            return df
        else:
            return None

    def _dedupe(self, df: pd.DataFrame) -> pd.DataFrame:
        sorted = df.sort_values(by=["fisa_version", "TBL_LAST_DT"], ascending=False)
        return sorted.drop_duplicates(["RCAT_CD", "RCLS_CD", "ATYP_CD"])

    def ingest(self) -> pd.DataFrame:
        df = pd.DataFrame(columns=HEADERS + ["fisa_version"])
        count = 0
        path = Path(self.path)
        if not path.is_dir():
            raise Exception(f"Directory '{path}' does not exist.")
        for file in path.glob("AIBL_DLY_BUD_96L1_*.asc"):
            _df = self._read_df(file)
            if _df is not None:
                df = pd.concat((_df, df), ignore_index=True)
                # not every time for speed reasons, not at the end for memory reasons
                if count % 10 == 0:
                    df = self._dedupe(df)
                count += 1

        df = self._dedupe(df)
        return df.sort_values(by="TBL_LAST_DT")

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
