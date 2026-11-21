"""Export the CD Profiles product tables to CSV for the CD Profiles app.

Writes every model in models/product/cd_profiles/ to data/exports/cd_profiles/<model>.csv. Run after
`dbt build`, which writes the tables and target/manifest.json:

    python scripts/export_cd_profiles.py
"""

import json
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).parent.parent
DATABASE = PROJECT_DIR / "data" / "analysis_growth.duckdb"
MANIFEST = PROJECT_DIR / "target" / "manifest.json"
OUTPUT_DIR = PROJECT_DIR / "data" / "exports" / "cd_profiles"


def main() -> None:
    nodes = json.loads(MANIFEST.read_text())["nodes"].values()
    models = sorted(
        node["name"]
        for node in nodes
        if node["resource_type"] == "model"
        and node["fqn"][1:3] == ["product", "cd_profiles"]
    )
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DATABASE), read_only=True) as con:
        for model in models:
            path = OUTPUT_DIR / f"{model}.csv"
            con.sql(f"COPY (SELECT * FROM {model}) TO '{path}' (HEADER)")
            print(path)


if __name__ == "__main__":
    main()
