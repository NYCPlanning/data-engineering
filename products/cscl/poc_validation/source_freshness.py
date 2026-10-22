"""How fresh our source extract is, and whether prod's release was built from a newer one.

A diff against prod can mean our build is wrong, or just that prod was built from a newer
(or older) CSCL extract than ours: prod 26c, for one, mixed vintages. This records the
evidence next to the diffs. It's a report: it warns, it never fails the QA stage.

- Source layers: row count and max created/modified dates of the main CSCL source layers.
- Dated prod layers: the few prod FGDB layers that carry CSCL's created/modified dates
  (passed through from the source), against the same source layer.
- Street codes: prod's SND street codes (B5SC) that ours doesn't have. Codes are handed
  out in sequence within blocks of 100 (e.g. 101900-101999), so a prod-only code above our
  highest code in its block is a feature created after our extract: prod's FEATURENAME is
  newer than ours.

Run as a QA command (needs DUCKDB_PATH, BUILD_ENGINE_SCHEMA and QA_DIR). Writes
<QA_DIR>/validation_output/source_freshness.csv, and <QA_DIR>/source_freshness.md for the
CI step summary.
"""

import csv
import os
from dataclasses import asdict, dataclass, fields
from pathlib import Path

import duckdb
from paths import validation_dir

from dcpy.utils.logging import logger

PROD_SCHEMA = "production_outputs"

SOURCE_LAYERS = [
    "dcp_cscl_centerline",
    "dcp_cscl_atomicpolygons",
    "dcp_cscl_featurename",
    "dcp_cscl_streetname",
    "dcp_cscl_commonplace_gdb",
    "dcp_cscl_altsegmentdata",
    "dcp_cscl_addresspoints",
    "dcp_cscl_segment_lgc",
    "dcp_cscl_nodes",
]

# prod FGDB layers that keep the source's created/modified dates: (source, prod)
DATED_PROD_LAYERS = [
    ("dcp_cscl_historicdistrict", "fgdb_nyhd"),
    ("dcp_cscl_zipcode", "fgdb_nyzip"),
    ("dcp_cscl_urbanrenewalarea", "fgdb_nyura"),
]

# our SND rows (before formatting) and prod's
BUILD_SND = "int__snd"
PROD_SND = f"{PROD_SCHEMA}.snd"


@dataclass
class Row:
    layer: str
    measure: str
    build: str = ""
    prod: str = ""
    warning: str = ""


def _date(value) -> str:
    return str(value)[:19] if value is not None else ""


def _max_dates(conn: duckdb.DuckDBPyConnection, table: str) -> tuple[int, str, str]:
    n, created, modified = conn.execute(
        f"select count(*), max(created_date), max(modified_date) from {table}"
    ).fetchone()  # type: ignore[misc]
    return n, _date(created), _date(modified)


def _exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    schema, _, name = table.rpartition(".")
    return bool(
        conn.execute(
            "select count(*) from information_schema.tables "
            "where table_name = ? and table_schema = coalesce(nullif(?, ''), current_schema())",
            [name, schema],
        ).fetchone()[0]  # type: ignore[index]
    )


def source_layer_rows(conn: duckdb.DuckDBPyConnection) -> list[Row]:
    rows = []
    for table in SOURCE_LAYERS:
        if not _exists(conn, table):
            rows.append(Row(table, "max created / modified", warning="not loaded"))
            continue
        n, created, modified = _max_dates(conn, table)
        rows.append(Row(table, "rows", str(n)))
        rows.append(Row(table, "max created_date", created))
        rows.append(Row(table, "max modified_date", modified))
    return rows


def dated_prod_layer_rows(conn: duckdb.DuckDBPyConnection) -> list[Row]:
    rows = []
    for source, prod in DATED_PROD_LAYERS:
        prod_table = f"{PROD_SCHEMA}.{prod}"
        if not (_exists(conn, source) and _exists(conn, prod_table)):
            rows.append(Row(f"{source} / {prod}", "max dates", warning="not loaded"))
            continue
        _, b_created, b_modified = _max_dates(conn, source)
        _, p_created, p_modified = _max_dates(conn, prod_table)
        for measure, ours, theirs in [
            ("max created_date", b_created, p_created),
            ("max modified_date", b_modified, p_modified),
        ]:
            newer = theirs > ours
            rows.append(
                Row(
                    f"{source} / {prod}",
                    measure,
                    ours,
                    theirs,
                    "prod's layer is newer than our source" if newer else "",
                )
            )
    return rows


def street_code_rows(conn: duckdb.DuckDBPyConnection) -> list[Row]:
    if not (_exists(conn, BUILD_SND) and _exists(conn, PROD_SND)):
        return [Row("snd", "street codes (B5SC)", warning="not built or not loaded")]
    only_prod, only_build, above_max, examples = conn.execute(
        f"""
        with
        build_codes as (select distinct left(b10sc, 6) as code from {BUILD_SND}),
        prod_codes as (select distinct left(b10sc, 6) as code from {PROD_SND}),
        block_max as (
            select left(code, 4) as block, max(code) as max_code from build_codes group by 1
        ),
        prod_only as (select code from prod_codes except select code from build_codes),
        build_only as (select code from build_codes except select code from prod_codes),
        prod_above as (
            select p.code from prod_only as p
            inner join block_max as m on left(p.code, 4) = m.block
            where p.code > m.max_code
        )
        select
            (select count(*) from prod_only),
            (select count(*) from build_only),
            (select count(*) from prod_above),
            (select string_agg(code, ' ' order by code) from (
                select code from prod_above order by code limit 10))
        """
    ).fetchone()  # type: ignore[misc]
    return [
        Row("snd", "street codes (B5SC) only in prod", prod=str(only_prod)),
        Row("snd", "street codes (B5SC) only in ours", build=str(only_build)),
        Row(
            "snd",
            "prod-only street codes above our highest code in their block of 100",
            prod=str(above_max) + (f" (e.g. {examples})" if examples else ""),
            warning=(
                "prod's FEATURENAME is newer than our source extract"
                if above_max
                else ""
            ),
        ),
    ]


def report(conn: duckdb.DuckDBPyConnection) -> list[Row]:
    return (
        source_layer_rows(conn) + dated_prod_layer_rows(conn) + street_code_rows(conn)
    )


def to_markdown(rows: list[Row]) -> str:
    lines = [
        "### Source freshness",
        "",
        "| layer | measure | ours | prod | warning |",
        "|-|-|-|-|-|",
    ]
    lines += [
        f"| {r.layer} | {r.measure} | {r.build} | {r.prod} | {r.warning} |"
        for r in rows
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    conn = duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)
    conn.execute(f"set schema '{os.environ['BUILD_ENGINE_SCHEMA']}'")
    rows = report(conn)

    out = validation_dir()
    out.mkdir(parents=True, exist_ok=True)
    with (out / "source_freshness.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[x.name for x in fields(Row)])
        writer.writeheader()
        writer.writerows(asdict(r) for r in rows)
    markdown = to_markdown(rows)
    (out.parent / "source_freshness.md").write_text(markdown)
    print(markdown)
    for r in rows:
        if r.warning:
            logger.warning(f"source freshness: {r.layer}, {r.measure}: {r.warning}")
    logger.info(f"Wrote {Path(out / 'source_freshness.csv')}")


if __name__ == "__main__":
    main()
