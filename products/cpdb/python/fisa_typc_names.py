"""Write seeds/fisa_typc_names.csv: every ten-year plan category code and name in each
archived version of fisa_capitalcommitments.

A build only loads one FISA version, but a category code's name changes between plans
(and in FY22 two RE codes shared one name), so matching TYCS category names to codes
needs every plan's own pairs. Rerun when a new FISA version is archived:

    python products/cpdb/python/fisa_typc_names.py
"""

import tempfile
from pathlib import Path

import pandas as pd

from dcpy.connectors.edm import recipes
from dcpy.connectors.edm.models import Dataset, DatasetType

DATASET = "fisa_capitalcommitments"
COLUMNS = [
    "cycle_fy",
    "cycle_name",
    "budget_proj_type",
    "typ_category",
    "typ_category_name",
]
SEED_PATH = Path(__file__).parent.parent / "seeds" / "fisa_typc_names.csv"


def _read_pg_dump(path: Path) -> pd.DataFrame:
    """Read the table data out of a plain-format pg_dump's COPY block."""
    lines = path.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith("COPY "))
    header = lines[start]
    names = [
        c.strip().strip('"')
        for c in header[header.index("(") + 1 : header.index(")")].split(",")
    ]
    end = lines.index("\\.", start)
    rows = [line.split("\t") for line in lines[start + 1 : end]]
    return pd.DataFrame(rows, columns=names).replace("\\N", None)


def _read(version: str, cache_dir: Path) -> pd.DataFrame:
    ds = Dataset(id=DATASET, version=version)
    file_types = recipes.get_file_types(ds)
    # 2021-2023 versions were only archived as pg_dumps
    if file_types & {DatasetType.parquet, DatasetType.csv}:
        return recipes.read_df(ds, local_cache_dir=cache_dir)[COLUMNS]
    ds.file_type = DatasetType.pg_dump
    return _read_pg_dump(recipes.fetch_dataset(ds, target_dir=cache_dir))[COLUMNS]


def main() -> None:
    frames = []
    with tempfile.TemporaryDirectory() as cache_dir:
        for version in sorted(recipes.get_all_versions(DATASET)):
            df = _read(version, Path(cache_dir)).astype("string")
            df = df.apply(lambda s: s.str.strip())
            df.insert(0, "version", version)
            frames.append(df.dropna(subset=["typ_category"]).drop_duplicates())
    names = (
        pd.concat(frames)
        .rename(columns={"typ_category": "typc", "typ_category_name": "typc_name"})
        .sort_values(["version", "budget_proj_type", "typc"])
    )
    names.to_csv(SEED_PATH, index=False)
    print(f"wrote {len(names)} rows from {len(frames)} versions to {SEED_PATH}")


if __name__ == "__main__":
    main()
