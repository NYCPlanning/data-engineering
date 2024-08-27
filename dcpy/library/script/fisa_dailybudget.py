from pathlib import Path
import pandas as pd
import re

from dcpy.connectors.edm import recipes
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
    @property
    def previous_version(self) -> str:
        version = self.source["previous_version"]
        if version is None:
            raise Exception(
                "fisa_dailybudget requires a previous version specified in its yml input."
            )
        return str(version)

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
        previous_dataset = recipes.Dataset(
            id=self.name,
            version=self.previous_version,
        )
        previous = recipes.read_df(previous_dataset, dtype=str)
        new = self._read_df(Path(self.source["path"]))
        df = pd.concat((previous, new), ignore_index=True)
        df = self._dedupe(df)
        return df.sort_values(by="TBL_LAST_DT")

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
