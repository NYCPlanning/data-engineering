import pandas as pd

from .scriptor import ScriptorInterface
from . import df_to_tempfile
from dotenv import load_dotenv

# Load environmental variables
load_dotenv()


class Scriptor(ScriptorInterface):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        df = pd.read_csv(self.path, encoding="ISO-8859-1")
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
