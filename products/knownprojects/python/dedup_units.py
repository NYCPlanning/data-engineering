import pandas as pd
from sqlalchemy import text

from .utils import engine, psql_insert_copy


def subtract_units(row, group):
    higher_priority = group[group["source_id"] < row["source_id"]]
    higher_priority_units = higher_priority["units_net"].sum()
    row["units_net"] = row["units_net"] - higher_priority_units
    if row["units_net"] < 0:
        row["units_net"] = 0
    return row


def resolve_project(group):
    if group.shape[0] > 1:
        group = group.sort_values("source_id")
        group = group.reset_index()
        for index, row in group.iterrows():
            group.iloc[index] = subtract_units(row, group)
    return group


def resolve_all_projects(df):
    # Hierarchy for unit subtraction
    hierarchy = {
        "DOB": 1,
        "HPD RFPs": 2,
        "EDC Projected Projects": 3,
        "DCP Application": 4,
        "Empire State Development Projected Projects": 5,
        "Neighborhood Study Rezoning Commitments": 6,
        "Neighborhood Study Projected Development Sites": 7,
        "DCP Planner-Added Projects": 8,
    }

    df["source_id"] = df["source"].map(hierarchy)

    # Subtract units within cluster based on hierarchy
    print("Subtracting units within projcts based on source hierarchy...")
    resolved = df.groupby(["project_id"], as_index=False).apply(resolve_project)
    resolved = resolved[
        ["project_id", "source", "units_gross", "units_net", "record_id"]
    ]
    print(resolved.head())

    return resolved


def import_table() -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(
            text(
                """
        SELECT
            a.source,
            a.record_id,
            a.units_gross::double precision as units_gross,
            a.units_gross::double precision as units_net,
            b.project_id
        FROM combined a LEFT JOIN (
            SELECT unnest(project_record_ids) as record_id, 
            ROW_NUMBER() OVER(ORDER BY project_record_ids) as project_id
            FROM project_record_ids
        ) b 
        ON a.record_id = b.record_id
        """
            ),
            con=conn,
        )


def export_table() -> bool:
    with engine.begin() as conn:
        resolved.to_sql(
            "deduped_units",
            con=conn,
            if_exists="replace",
            index=False,
            method=psql_insert_copy,
        )
    return True


if __name__ == "__main__":
    df = import_table()
    resolved = resolve_all_projects(df)
    export_table()
