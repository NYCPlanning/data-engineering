import pandas as pd

from dcpy.utils import s3
from dcpy.utils.logging import logger

from . import df_to_tempfile
from .scriptor import ScriptorInterface


def _skip_malformed_row(bad_line: list[str]) -> None:
    """Handle a row whose field count doesn't match the header.

    DOB NOW's export delimits fields with "|" but doesn't escape/quote "|"
    characters embedded within a field (e.g. a pipe-separated list of related
    addresses for a filing spanning multiple structures/BINs). That shifts
    every field after the embedded pipe, so the row can't be trusted - drop
    it rather than silently loading misaligned data.
    """
    job_filing_number = bad_line[0] if bad_line else "unknown"
    logger.warning(
        f"Dropping dob_now_applications row with unexpected field count "
        f"({len(bad_line)}) for job_filing_number={job_filing_number}: likely "
        "an unescaped '|' embedded in a source field"
    )
    return None


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        data = s3.get_file_as_stream(
            "edm-private",
            f"dob_now/dob_now_job_applications/DOB_Now_Job_Filing_Data_for_DCP_{self.version}.csv",
        )
        df = pd.read_csv(
            data,
            encoding="cp1252",
            sep="|",
            engine="python",
            on_bad_lines=_skip_malformed_row,
        )
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
