import pandas as pd

from . import df_to_tempfile


class Scriptor:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def reader(self, id: str) -> pd.DataFrame:
        return pd.read_csv(f"https://data.cityofnewyork.us/api/views/{id}/rows.csv")

    @property
    def sites(self) -> pd.DataFrame:
        return self.reader("y9si-s7ab")

    @property
    def contracts(self) -> pd.DataFrame:
        return self.reader("2bvn-ky2h")

    @property
    def providers(self) -> pd.DataFrame:
        return self.reader("x882-mwt5")

    @property
    def programs(self) -> pd.DataFrame:
        return self.reader("3t6s-yb67")

    def ingest(self) -> pd.DataFrame:
        return (
            self.sites.merge(
                self.contracts, on=["provider_id", "program_id", "contract_id"]
            )
            .merge(self.providers, on=["provider_id"])
            .merge(self.programs, on=["program_id"])
        )

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
