from pathlib import Path
import pandas as pd
import re

from dcpy.connectors.edm import recipes
from . import df_to_tempfile
from .scriptor import ScriptorInterface

HEADERS = [
    "rcat_cd",
    "rcls_cd",
    "atyp_cd",
    "mng_dpt_cd",
    "cptl_proj_id",
    "bud_obj_cd",
    "au_cd",
    "fndg_dpt_cd",
    "cmtmnt_am",
    "oblgtns_am",
    "adpt_am",
    "penc_am",
    "enc_am",
    "acrd_exp_am",
    "cash_exp_am",
    "ucomit_am",
    "actu_exp_am",
    "tbl_last_dt",
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
        sorted = df.sort_values(by=["fisa_version", "tbl_last_dt"], ascending=False)
        return sorted.drop_duplicates(["rcat_cd", "rcls_cd", "atyp_cd"])

    def ingest(self) -> pd.DataFrame:
        print(f"{self.previous_version=}")
        previous_dataset = recipes.Dataset(
            id=self.name,
            version=self.previous_version,
        )
        previous = recipes.read_df(previous_dataset)
        new = self._read_df(Path(self.source["path"]))
        df = pd.concat((previous, new), ignore_index=True)
        df = self._dedupe(df)
        return df.sort_values(by="tbl_last_dt")

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
