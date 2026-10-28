"""
Export the field-level diffs (qa__diffs_all) for the QA app's Field-Level Diffs view.

Run from the products/cscl directory, after the qa__diffs_* models (the QA stage's
qa_models step):
    python poc_validation/export_field_diffs.py

Writes <QA_DIR>/validation_output/field_diffs.parquet: one row per (output_file_id,
comparison_id, changed field), with the prod and dev values. Rows that exist on only one
side (only_in_build / only_in_legacy) have no per-field changes and get one row with a
null field. Each row carries the known difference its diff_group cites, if any: the
seeds/known_differences.csv ID at the start of the label ('CSCL-LION-04: ...'), or its
older alias ('Bug 001: ...'), and whether it's approved: accounted for, and agreed by every
reviewer its output needs (see the signoff_matrix seed).
"""

import os
from pathlib import Path

import duckdb
from paths import validation_dir

KNOWN_DIFFERENCES_SEED_PATH = Path("seeds/known_differences.csv")
OUTPUT_FILENAME = "field_diffs.parquet"

QUERY = """
WITH diffs AS (
    SELECT
        *,
        CASE
            WHEN changes IS NULL OR len(json_keys(changes)) = 0 THEN [NULL::VARCHAR]
            ELSE json_keys(changes)
        END AS fields,
        regexp_extract(diff_group, '^(CSCL-[A-Z0-9]+-[0-9]+)', 1) AS cited_id,
        regexp_extract(diff_group, '^(Bug [0-9]+)', 1) AS cited_alias
    FROM qa__diffs_all
),

by_field AS (
    SELECT
        * EXCLUDE (fields),
        unnest(fields) AS field
    FROM diffs
),

known AS (
    SELECT id, alias, description
    FROM read_csv(?, header = true, all_varchar = true)
)

SELECT
    d.output_file_id,
    d.comparison_column,
    d.comparison_id,
    d.status,
    d.field,
    (d.changes -> d.field) ->> 'old' AS prod_value,
    (d.changes -> d.field) ->> 'new' AS dev_value,
    d.diff_group,
    d.accounted_for,
    d.approved,
    k.id AS known_difference_id,
    k.description AS known_difference_description
FROM by_field AS d
LEFT JOIN known AS k
    ON (d.cited_id <> '' AND k.id = d.cited_id)
    OR (d.cited_id = '' AND d.cited_alias <> '' AND k.alias = d.cited_alias)
ORDER BY d.output_file_id, d.field, d.comparison_id
"""


def main() -> None:
    out_path = validation_dir() / OUTPUT_FILENAME
    out_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)
    conn.execute(f"SET schema = '{os.environ['BUILD_ENGINE_SCHEMA']}'")
    rel = conn.sql(QUERY, params=[str(KNOWN_DIFFERENCES_SEED_PATH)])
    rel.write_parquet(str(out_path))
    n_rows, n_outputs = conn.sql(
        "SELECT count(*), count(DISTINCT output_file_id) FROM read_parquet(?)",
        params=[str(out_path)],
    ).fetchone()
    print(f"Wrote {n_rows} field diff rows for {n_outputs} outputs to {out_path}")


if __name__ == "__main__":
    main()
