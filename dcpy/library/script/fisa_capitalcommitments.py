import pandas as pd

from . import df_to_tempfile
from .scriptor import ScriptorInterface

HEADERS = [
    "cycle_fy",
    "cycle_name",
    "budget_proj_type",
    "budget_line_id",
    "agency_class_cd",
    "unit_of_appr",
    "budget_line_descr",
    "managing_agcy_cd",
    "project_id",
    "short_descr",
    "object",
    "object_name",
    "fcst_cnx_amt",
    "fcst_cex_amt",
    "fcst_st_amt",
    "fcst_fd_amt",
    "fcst_pv_amt",
    "planned_commit_date",
    "typ_category",
    "typ_category_name",
]


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        df = pd.read_csv(
            self.source["path"], encoding="iso-8859-1", delimiter="|", header=None
        )
        df.drop(20, axis=1, inplace=True)
        df.columns = pd.Index(HEADERS)
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
