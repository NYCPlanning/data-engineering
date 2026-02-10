import os
import shutil
from pathlib import Path

import pandas as pd
from jinja2 import Template
from sqlalchemy import create_engine, text

from python.admin_geographies import generate_all_admin_geographies

OUTPUT_DIR = Path(__file__).parent.parent / "projects_in_geographies"

SQL_TEMPLATE_PATH = (
    Path(__file__).parent.parent / "sql_templates/projects_in_geographies.sql"
)

# SQL Template
with open(SQL_TEMPLATE_PATH, "r") as f:
    SQL_TEMPLATE_TEXT = f.read()


def create_table(
    geography_type: str,
    table_name: str,
    geography_id: str,
    geography_name: str,
) -> None:
    print(f"creating table {table_name} ...")
    # render templated sql
    sql_rendered = Template(SQL_TEMPLATE_TEXT).render(
        geography_type=geography_type,
        table_name=table_name,
        geography_id=geography_id,
        geography_name=geography_name,
    )
    # execute SQL
    engine = create_engine(os.environ["BUILD_ENGINE"])
    with engine.begin() as conn:
        conn.execute(text(sql_rendered))


def export_table(table_name: str) -> None:
    print(f"pd.read_sql from table {table_name} ...")
    engine = create_engine(os.environ["BUILD_ENGINE"])
    with engine.begin() as conn:
        df = pd.read_sql(text("select * from %(name)s" % {"name": table_name}), conn)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output_file_path = OUTPUT_DIR / f"{table_name}.csv"
    print(f"sql_to_csv from {table_name} to {output_file_path} ...")
    df.to_csv(output_file_path, index=False)


if __name__ == "__main__":
    # create all csv files
    all_geographies = generate_all_admin_geographies()
    for geography in all_geographies:
        create_table(
            geography.cpdb_geography_type,
            geography.table_name,
            geography.geography_id,
            geography.geography_name,
        )
        export_table(geography.table_name)

    # zip folder with all csv files
    print(f"Zipping folder\n\t{OUTPUT_DIR}")
    shutil.make_archive(
        base_name=OUTPUT_DIR,
        format="zip",
        root_dir=OUTPUT_DIR,
    )
