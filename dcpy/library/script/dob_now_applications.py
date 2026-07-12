import re
from typing import Callable

import pandas as pd

from dcpy.utils import s3
from dcpy.utils.logging import logger

from . import df_to_tempfile
from .scriptor import ScriptorInterface

NYC_BOROUGHS = {"MANHATTAN", "BROOKLYN", "BRONX", "QUEENS", "STATEN ISLAND"}

# Columns a filer actually types free text into - the only places an unescaped
# "|" can realistically originate. Purely numeric/computed columns (floor
# area, FAR, parking counts, ...) are never candidates: nothing types a stray
# pipe into a dropdown or a calculated field.
FREE_TEXT_CANDIDATE_COLUMNS = [
    "Owner's Business Name",
    "Owner's Street Name",
    "Filing Representative Business Name",
    "Filing Representative Street Name",
    "JobDescription",
]

# Cheap, closed-form checks on columns that are reliably populated (not
# blank) for most filings, used to confirm a repair candidate didn't shift
# real data out of place. Deliberately stops before the tail of the schema
# (floor area, FAR, parking counts, ...): those columns are so often blank
# that a shift among them is invisible to a format check, so they can't help
# localize anything - see MAX_AMBIGUOUS_SPREAD below for how that's handled.
ANCHOR_COLUMNS = [
    ("Borough", lambda v: v.upper() in NYC_BOROUGHS),
    ("Block", lambda v: v == "" or v.isdigit()),
    ("LOT", lambda v: v == "" or v.isdigit()),
    ("Bin", lambda v: v == "" or v.isdigit()),
    ("State", lambda v: v == "" or bool(re.fullmatch(r"[A-Za-z]{2}", v))),
    ("Zip", lambda v: v == "" or v.isdigit()),
    (
        "Filing Representative State",
        lambda v: v == "" or bool(re.fullmatch(r"[A-Za-z]{2}", v)),
    ),
    ("Filing Representative Zip", lambda v: v == "" or v.isdigit()),
    ("Latitude", lambda v: v == "" or bool(re.fullmatch(r"-?\d*\.?\d+", v))),
    ("Longitude", lambda v: v == "" or bool(re.fullmatch(r"-?\d*\.?\d+", v))),
    (
        "Approved (Date)",
        lambda v: v == "" or bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)),
    ),
    ("Filing Date", lambda v: v == "" or bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v))),
    (
        "Current Status Date",
        lambda v: v == "" or bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)),
    ),
    ("SignoffDate", lambda v: v == "" or bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v))),
    (
        "PermitIssuedDate",
        lambda v: v == "" or bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", v)),
    ),
    ("JobType", lambda v: v == "" or not v.isdigit()),
]

# If more than one free-text column produces an anchor-passing repair, and
# they're this far apart or more, treat it as unresolved rather than guess -
# see the module docstring below for why a *small* spread is fine to accept.
MAX_AMBIGUOUS_SPREAD = 5


def _make_bad_line_handler(
    header: list[str],
) -> Callable[[list[str]], list[str] | None]:
    """Build a handler for rows whose field count doesn't match the header.

    DOB NOW's export delimits fields with "|" but doesn't escape/quote "|"
    characters typed into free-text fields (JobDescription, Owner's Business
    Name, ...), e.g. a filer writing "reason | detail". That shifts every
    field after the embedded pipe.

    Since the export schema is fixed-width, repair doesn't need fuzzy
    alignment: try merging the extra fields back into each free-text column
    in turn (FREE_TEXT_CANDIDATE_COLUMNS) and keep any merge where the row's
    anchor columns (ANCHOR_COLUMNS) still look valid. If more than one
    candidate validates, that's fine as long as they're close together - e.g.
    Owner's Business Name and Owner's Street Name sit next to each other and
    are both free text, so we sometimes can't tell which absorbed the extra
    pipe, but every anchor column downstream comes out correct regardless of
    which of the two we pick. If the validating candidates are far apart
    (MAX_AMBIGUOUS_SPREAD), that signals real ambiguity we can't resolve, and
    if none validate, the row is corrupted some other way - both fall back to
    dropping rather than silently loading misaligned data.
    """
    n_expected = len(header)
    candidate_positions = sorted(
        header.index(name) for name in FREE_TEXT_CANDIDATE_COLUMNS if name in header
    )
    anchor_checks = [
        (header.index(name), check) for name, check in ANCHOR_COLUMNS if name in header
    ]

    def is_plausible(row: list[str]) -> bool:
        return all(check(row[idx]) for idx, check in anchor_checks)

    def handler(bad_line: list[str]) -> list[str] | None:
        job_filing_number = bad_line[0] if bad_line else "unknown"
        n_actual = len(bad_line)
        extra = n_actual - n_expected

        if extra > 0:
            passing = []
            for i in candidate_positions:
                end = i + extra + 1
                if end > n_actual:
                    continue
                candidate = bad_line[:i] + ["|".join(bad_line[i:end])] + bad_line[end:]
                if is_plausible(candidate):
                    passing.append((i, candidate))

            if passing:
                positions = [i for i, _ in passing]
                if max(positions) - min(positions) <= MAX_AMBIGUOUS_SPREAD:
                    merged_idx, repaired = passing[0]
                    logger.warning(
                        f"Repaired dob_now_applications row with unexpected field "
                        f"count ({n_actual}) for job_filing_number={job_filing_number}: "
                        f"merged extra '|'-delimited fragments back into "
                        f"{header[merged_idx]!r}"
                    )
                    return repaired

                logger.warning(
                    f"Dropping dob_now_applications row with unexpected field count "
                    f"({n_actual}) for job_filing_number={job_filing_number}: repair "
                    f"candidates span multiple unrelated columns "
                    f"({[header[i] for i in positions]}), too ambiguous to trust"
                )
                return None

        logger.warning(
            f"Dropping dob_now_applications row with unexpected field count "
            f"({n_actual}) for job_filing_number={job_filing_number}: likely an "
            "unescaped '|' embedded in a source field, and no free-text column "
            "repair produced a plausible row"
        )
        return None

    return handler


class Scriptor(ScriptorInterface):
    def ingest(self) -> pd.DataFrame:
        data = s3.get_file_as_stream(
            "edm-private",
            f"dob_now/dob_now_job_applications/DOB_Now_Job_Filing_Data_for_DCP_{self.version}.csv",
        )
        header = data.readline().decode("cp1252").rstrip("\r\n").split("|")
        data.seek(0)
        df = pd.read_csv(
            data,
            encoding="cp1252",
            sep="|",
            engine="python",
            on_bad_lines=_make_bad_line_handler(header),
        )
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
