# from asyncio.windows_events import NULL
import pandas as pd

from . import df_to_tempfile
from .scriptor import ScriptorInterface

from dcpy.utils.logging import logger


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(
            {
                "": [],
                "": [],
            }
        )

        df.insert(0, "v", self.version)
        df["old_column"] = None
        return df

    def runner(self) -> str:
        df = self.ingest()
        logger.info(f"Ingested data with shape {df.shape}")
        local_path = df_to_tempfile(df)
        return local_path
