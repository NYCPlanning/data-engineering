#!/usr/bin/env python3
"""
DuckDB-based Melissa geocoding pipeline.

Ports db-melissa's Postgres pipeline (python/dataloading.py, sql/preprocessing.sql,
python/geocoding.py, sql/create.sql, sql/fill.sql, sql/output.sql) onto DuckDB, so
the whole thing runs in-process without a Postgres server. See ~/dev/db-melissa for
the original this was ported from.

Note: db-melissa's sql/clean.sql (drops rows with GRC 71, i.e. outside NYC) is
intentionally NOT applied here -- it was never wired into db-melissa's own build
step (melissa.sh only runs create.sql, fill.sql, output.sql), and the reference
output this pipeline was matched against still contains GRC=71 rows.

Geocoding is resumable without any separate position/checkpoint file (unlike the
old row-by-row experimental/melissa/geocode.py this replaced): results are
committed to the persistent --db-path DuckDB file in chunks (--chunk-size,
default 5000) as they're produced. If a run is interrupted, just rerun the same
command against the same output file (which reuses the same .duckdb path by
default) -- already-geocoded addresses are detected via an anti-join against
what's already on disk and skipped, so at most one chunk's worth of work is
ever lost. See _geocode_remaining.

melissa_input can be loaded two ways (see run_e2e.sh for the second, wired up
end to end):
  1. (default) Pass --input-file; pipeline.py reads it with pandas, same as
     db-melissa's dataloading.py did against Postgres.
  2. Run `dcp_load_recipe` against recipe.yml in this directory first (same
     convention as products/lift) to pull melissa_input from edm-private into
     a DuckDB file, then pass --skip-load --db-path <that file> --schema
     <recipe's target schema> so this script picks up the already-loaded
     table instead of re-reading a local file.
melissa_corrections/melissa_outsideofnyc always load from local files
(--corrections/--outside-of-nyc, defaulting to data/*.csv) regardless of
--skip-load -- they aren't in edm-private yet, only melissa_input is.

Usage:
    python3 pipeline.py <output.csv> --input-file <melissa_input.txt>
    python3 pipeline.py <output.csv> --skip-load --db-path melissa_v1.duckdb --schema <schema>
"""

import argparse
import multiprocessing
from pathlib import Path

import duckdb
import pandas as pd
from geocoding import geocode

HERE = Path(__file__).parent

# Columns produced by geocoding.py's geocode(), shared by melissa_input_geocode
# and melissa_corrections_geocode (both are the output of running the same
# geocode() over a different set of (id, address, zip_code, boro) records).
GEOCODE_COLUMNS = [
    "id",
    "hnum",
    "sname",
    "e_wa1_street1_boroughcode",
    "e_wa1_housenumberdisplay",
    "e_wa1_street1_streetname",
    "e_wa1_message",
    "e_wa2_xcoordinate",
    "e_wa2_ycoordinate",
    "e_wa2_communitydistrict",
    "e_wa2_nta",
    "e_wa2_physicalid",
    "e_wa2_ntaname",
    "e_wa2_nta2020",
    "e_wa2_latitude",
    "e_wa2_longitude",
    "e_wa2_blockfaceid",
    "e_wa2_reasoncode",
    "e_wa2_grc",
    "a_wa1_housenumberdisplay",
    "a_wa1_street1_streetname",
    "a_wa1_message",
    "a_wa2_bbl",
    "a_wa2_binofinputaddress",
    "a_wa2_tpadnewbin",
    "a_wa2_reasoncode",
    "a_wa2_grc",
    "ap_wa1_housenumberdisplay",
    "ap_wa1_street1_streetname",
    "ap_wa2_grc",
    "ap_wa2_reasoncode",
    "ap_wa1_message",
    "ap_wa2_latitude",
    "ap_wa2_longitude",
    "ap_wa2_xcoordinate",
    "ap_wa2_ycoordinate",
    "ap_wa2_ap_id",
]


def _load_csv_table(
    conn: duckdb.DuckDBPyConnection,
    path: Path,
    table_name: str,
    delimiter: str = ",",
) -> None:
    """dataloading.py equivalent for a single table: read a local file into a
    DuckDB table, normalizing column names the same way the original did
    (lowercase, spaces to underscores)."""
    df = pd.read_csv(path, dtype=str, delimiter=delimiter, index_col=False)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    view_name = f"{table_name}_df"
    conn.register(view_name, df)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")
    conn.unregister(view_name)


def load_input(conn: duckdb.DuckDBPyConnection, input_path: Path) -> None:
    """Load the (large, pipe-delimited) main Melissa address file."""
    _load_csv_table(conn, input_path, "melissa_input", delimiter="|")


def load_reference_data(
    conn: duckdb.DuckDBPyConnection,
    corrections_path: Path,
    outside_of_nyc_path: Path,
) -> None:
    """Load the small, static reference files (corrections, outside-of-NYC
    exclusions). Unlike melissa_input, these currently always load from local
    files -- they're checked into products/melissa/data/ rather than sourced
    from edm-private via recipe.yml, since only melissa_input has been pushed
    there so far."""
    _load_csv_table(conn, corrections_path, "melissa_corrections")
    _load_csv_table(conn, outside_of_nyc_path, "melissa_outsideofnyc")


def normalize_column_case(conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
    """Force every column of table_name to a lowercase name.

    DuckDB resolves unquoted identifiers case-insensitively, but a query like
    `SELECT address FROM t` against a column actually named "Address" returns
    a result column still named "Address" -- which breaks anything relying on
    exact-cased dict keys downstream (e.g. .df().to_dict("records") feeding
    geocoding.py's geocode(), which does record.get("address")). This matters
    for melissa_input specifically because dcp_load_recipe's DuckDB loader
    (unlike this script's own _load_csv_table) doesn't lowercase columns --
    it preserves the source CSV's header casing as-is.
    """
    columns = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
    select_list = ", ".join(f'"{c}" AS {c.lower()}' for c in columns)
    conn.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT {select_list} FROM {table_name}"
    )


def preprocess(conn: duckdb.DuckDBPyConnection) -> None:
    """preprocessing.sql equivalent: add a synthetic id to each table, and drop
    melissa_input rows that are known to be outside NYC."""
    for table in ("melissa_input", "melissa_corrections", "melissa_outsideofnyc"):
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT *, address || city || zip AS id FROM {table}
        """)

    conn.execute("""
        DELETE FROM melissa_input
        WHERE id IN (SELECT DISTINCT id FROM melissa_outsideofnyc)
    """)


DEFAULT_CHUNK_SIZE = 5000


def _geocode_records(records: list[dict]) -> pd.DataFrame:
    cpu_count = multiprocessing.cpu_count()
    pool_chunksize = max(1, len(records) // (cpu_count * 4))
    with multiprocessing.Pool(processes=cpu_count) as pool:
        results = pool.map(geocode, records, pool_chunksize)
    df = pd.DataFrame(results)
    # Ensure every expected column is present even if some geocode results
    # were missing a key (e.g. a function call failed with no result at all).
    for col in GEOCODE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[GEOCODE_COLUMNS]


# Geocode result columns are always text coming out of Python, but two things
# need normalizing before build()'s COALESCE/IS NULL logic can trust them:
#
# 1. A batch where some column is None for every row would otherwise let
#    DuckDB's CREATE TABLE AS SELECT infer a non-VARCHAR type for it (e.g.
#    INTEGER), which then breaks string functions like LEFT/RIGHT applied to
#    that column later in build(). Force VARCHAR explicitly.
# 2. python-geosupport returns "" (empty string), not None, for fields with
#    no value (e.g. TPAD New BIN when there's no new bin) -- geocoding.py's
#    parse_1e/1a/ap default to "" verbatim from db-melissa. In the original
#    Postgres pipeline this didn't matter: csv.writer serializes both None
#    and "" as an empty, unquoted CSV field, and Postgres's COPY ... CSV
#    treats an empty unquoted field as NULL by default -- so both collapsed
#    to real SQL NULL on import. DuckDB has no such implicit conversion, so
#    without NULLIF here every COALESCE(corrections, original) fallback in
#    build() would wrongly treat "no value" as "a real, empty value" and
#    never fall through to the next source.
_GEOCODE_SELECT_LIST = ", ".join(
    "id" if col == "id" else f"NULLIF(CAST({col} AS VARCHAR), '') AS {col}"
    for col in GEOCODE_COLUMNS
)


def _geocode_remaining(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    source_sql: str,
    chunk_size: int,
) -> None:
    """Geocode every record from source_sql not yet present in table_name,
    appending results chunk by chunk (each chunk is its own DuckDB INSERT, so
    it's durable on disk as soon as it completes).

    table_name -- backed by the persistent, file-based --db-path -- is itself
    the checkpoint: rerunning this against the same db-path re-evaluates the
    anti-join and only geocodes whatever's still missing, so a crash loses at
    most one chunk's worth of work instead of the whole run. No separate
    position file needed.
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(f"{c} VARCHAR" for c in GEOCODE_COLUMNS)}
        )
    """)

    already_done = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    remaining = (
        conn.execute(f"""
            WITH source AS ({source_sql})
            SELECT * FROM source WHERE id NOT IN (SELECT id FROM {table_name})
        """)
        .df()
        .to_dict("records")
    )

    total_remaining = len(remaining)
    if total_remaining == 0:
        print(f"  {table_name}: {already_done} already geocoded, nothing left to do")
        return
    print(
        f"  {table_name}: {already_done} already geocoded (resuming), "
        f"{total_remaining} left"
    )

    for start in range(0, total_remaining, chunk_size):
        chunk = remaining[start : start + chunk_size]
        chunk_df = _geocode_records(chunk)
        conn.register("_chunk", chunk_df)
        conn.execute(
            f"INSERT INTO {table_name} SELECT {_GEOCODE_SELECT_LIST} FROM _chunk"
        )
        conn.unregister("_chunk")
        done = min(start + chunk_size, total_remaining)
        print(f"  {table_name}: {done}/{total_remaining} geocoded")


def geocode_stage(
    conn: duckdb.DuckDBPyConnection, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> None:
    """geocoding.py's __main__ equivalent: geocode every distinct input address,
    and separately every distinct corrected address. Resumable -- see
    _geocode_remaining."""
    print("Geocoding input addresses...")
    _geocode_remaining(
        conn,
        "melissa_input_geocode",
        "SELECT DISTINCT id, address, zip AS zip_code FROM melissa_input",
        chunk_size,
    )

    print("Geocoding corrected addresses...")
    _geocode_remaining(
        conn,
        "melissa_corrections_geocode",
        """
        SELECT DISTINCT
            id,
            corrected_hn || '|' || corrected_street AS address,
            '' AS zip_code,
            corrected_borough AS boro
        FROM melissa_corrections
        """,
        chunk_size,
    )


def build(conn: duckdb.DuckDBPyConnection) -> None:
    """create.sql + fill.sql equivalent: one row per distinct input id, with
    corrections preferred over the original geocode result wherever both exist
    (COALESCE(corrections, original) per field -- same semantics as fill.sql's
    two-pass UPDATE ... CASE WHEN x IS NULL THEN y ELSE x END, collapsed into
    one query). BIN uses the 4-tier priority from fill.sql lines 92-139:
    corrections TPAD bin -> corrections input bin -> original TPAD bin ->
    original input bin.
    """
    conn.execute("""
        CREATE OR REPLACE TABLE melissa AS
        WITH base AS (SELECT DISTINCT id FROM melissa_input)
        SELECT
            b.id,
            ig.hnum,
            ig.sname,
            c.corrected_borough,
            c.corrected_hn AS corrected_house_number,
            c.corrected_street AS corrected_street_name,
            COALESCE(cg.e_wa1_street1_boroughcode, ig.e_wa1_street1_boroughcode)
                AS borough_code,
            COALESCE(cg.e_wa1_housenumberdisplay, ig.e_wa1_housenumberdisplay)
                AS f1_normalized_hn,
            COALESCE(cg.e_wa1_street1_streetname, ig.e_wa1_street1_streetname)
                AS f1_normalized_sn,
            COALESCE(cg.e_wa2_xcoordinate, ig.e_wa2_xcoordinate)
                AS centerline_xcoordinate,
            COALESCE(cg.e_wa2_ycoordinate, ig.e_wa2_ycoordinate)
                AS centerline_ycoordinate,
            COALESCE(cg.e_wa2_latitude, ig.e_wa2_latitude) AS centerline_latitude,
            COALESCE(cg.e_wa2_longitude, ig.e_wa2_longitude) AS centerline_longitude,
            COALESCE(cg.e_wa2_physicalid, ig.e_wa2_physicalid) AS physicalid,
            COALESCE(cg.e_wa2_blockfaceid, ig.e_wa2_blockfaceid) AS blockfaceid,
            COALESCE(cg.e_wa2_communitydistrict, ig.e_wa2_communitydistrict) AS cd,
            -- NTA is the 2020-vintage code (not the 2010-vintage code the
            -- original db-melissa pipeline's WA2_NTA field mapping used) so
            -- it's consistent with nta_name, which can only be sourced
            -- 2020-vintage -- see fill_nta_names(). Geosupport pairs a name
            -- with the 2010 code but not the 2020 one, so there's no way to
            -- get a self-consistent 2010 code+name pair here.
            COALESCE(cg.e_wa2_nta2020, ig.e_wa2_nta2020) AS nta,
            COALESCE(cg.e_wa2_ntaname, ig.e_wa2_ntaname) AS nta_name,
            COALESCE(cg.e_wa2_grc, ig.e_wa2_grc) AS f1_grc,
            COALESCE(cg.e_wa2_reasoncode, ig.e_wa2_reasoncode) AS f1_reasoncode,
            COALESCE(cg.e_wa1_message, ig.e_wa1_message) AS f1_message,
            COALESCE(cg.a_wa1_housenumberdisplay, ig.a_wa1_housenumberdisplay)
                AS f1a_normalized_hn,
            COALESCE(cg.a_wa1_street1_streetname, ig.a_wa1_street1_streetname)
                AS f1a_normalized_sn,
            COALESCE(
                cg.a_wa2_tpadnewbin, cg.a_wa2_binofinputaddress,
                ig.a_wa2_tpadnewbin, ig.a_wa2_binofinputaddress
            ) AS bin,
            CASE
                WHEN cg.a_wa2_tpadnewbin IS NOT NULL OR ig.a_wa2_tpadnewbin IS NOT NULL
                THEN 'Y' ELSE NULL
            END AS is_tpad_bin,
            COALESCE(cg.a_wa2_bbl, ig.a_wa2_bbl) AS bbl,
            COALESCE(cg.a_wa2_grc, ig.a_wa2_grc) AS f1a_grc,
            COALESCE(cg.a_wa2_reasoncode, ig.a_wa2_reasoncode) AS f1a_reasoncode,
            COALESCE(cg.a_wa1_message, ig.a_wa1_message) AS f1a_message,
            COALESCE(cg.ap_wa1_housenumberdisplay, ig.ap_wa1_housenumberdisplay)
                AS fap_normalized_hn,
            COALESCE(cg.ap_wa1_street1_streetname, ig.ap_wa1_street1_streetname)
                AS fap_normalized_sn,
            COALESCE(cg.ap_wa2_ap_id, ig.ap_wa2_ap_id) AS addresspointid,
            LEFT(COALESCE(cg.ap_wa2_xcoordinate, ig.ap_wa2_xcoordinate), 7)
                AS addresspointid_xcoordinate,
            RIGHT(COALESCE(cg.ap_wa2_ycoordinate, ig.ap_wa2_ycoordinate), 7)
                AS addresspointid_ycoordinate,
            COALESCE(cg.ap_wa2_latitude, ig.ap_wa2_latitude) AS addresspointid_latitude,
            COALESCE(cg.ap_wa2_longitude, ig.ap_wa2_longitude) AS addresspointid_longitude,
            COALESCE(cg.ap_wa2_grc, ig.ap_wa2_grc) AS fap_grc,
            COALESCE(cg.ap_wa2_reasoncode, ig.ap_wa2_reasoncode) AS fap_reasoncode,
            COALESCE(cg.ap_wa1_message, ig.ap_wa1_message) AS fap_message
        FROM base b
        LEFT JOIN melissa_corrections c ON b.id = c.id
        LEFT JOIN melissa_corrections_geocode cg ON b.id = cg.id
        LEFT JOIN melissa_input_geocode ig ON b.id = ig.id
    """)


def fill_nta_names(conn: duckdb.DuckDBPyConnection) -> None:
    """Fill nta_name from DCP's own dcp_nta2020 reference dataset, keyed by the
    2020 NTA code in melissa.nta -- Geosupport doesn't pair a name with that
    code itself (see geocoding.py's parse_1e / build()'s comment on nta).

    A no-op if dcp_nta2020 isn't loaded (e.g. in local/test runs, where it
    isn't declared in recipe.yml the way melissa_input is) -- nta_name is then
    just left NULL, same as before this function existed.
    """
    has_nta2020 = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = 'dcp_nta2020' AND table_schema = current_schema()
    """).fetchone()[0]
    if not has_nta2020:
        print("  dcp_nta2020 not loaded, skipping nta_name lookup")
        return

    conn.execute("""
        UPDATE melissa
        SET nta_name = n.ntaname
        FROM dcp_nta2020 n
        WHERE melissa.nta = n.nta2020
    """)


def output(conn: duckdb.DuckDBPyConnection, output_path: Path) -> None:
    """output.sql equivalent: re-attach every original melissa_input row (there
    can be several per id, e.g. one per suite) to its melissa geocode row, drop
    the internal id column, and write the final CSV."""
    conn.execute("""
        CREATE OR REPLACE TABLE melissa_output AS
        SELECT * FROM melissa_input a
        LEFT JOIN (
            SELECT
                id, hnum, sname, corrected_borough, corrected_house_number,
                corrected_street_name, borough_code, f1_normalized_hn, f1_normalized_sn,
                centerline_xcoordinate, centerline_ycoordinate, centerline_latitude,
                centerline_longitude, physicalid, blockfaceid, cd, nta, nta_name,
                f1_grc, f1_reasoncode, f1_message, f1a_normalized_hn, f1a_normalized_sn,
                bin, is_tpad_bin, bbl, f1a_grc, f1a_reasoncode, f1a_message,
                fap_normalized_hn, fap_normalized_sn, addresspointid,
                addresspointid_xcoordinate, addresspointid_ycoordinate,
                addresspointid_latitude, addresspointid_longitude,
                fap_grc, fap_reasoncode, fap_message
            FROM melissa
        ) b USING (id)
    """)
    conn.execute("ALTER TABLE melissa_output DROP COLUMN IF EXISTS id")
    # dcp_load_recipe's DuckDB loader (and db-melissa's original ogr2ogr-based
    # Postgres loader) both add a row-number primary key column; drop it if present.
    conn.execute("ALTER TABLE melissa_output DROP COLUMN IF EXISTS ogc_fid")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(f"""
        COPY melissa_output TO '{output_path}' (HEADER, DELIMITER ',')
    """)


def run(
    output_path: Path,
    db_path: Path,
    *,
    input_path: Path | None = None,
    corrections_path: Path | None = None,
    outside_of_nyc_path: Path | None = None,
    skip_load: bool = False,
    schema: str | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    limit: int | None = None,
) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        if schema:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            conn.execute(f"USE {schema}")

        if skip_load:
            print(
                "Skipping melissa_input load (expecting it's already loaded, "
                "e.g. via dcp_load_recipe)..."
            )
        else:
            assert input_path
            print("Loading input file...")
            load_input(conn, input_path)

        # Always normalize -- covers both load paths (recipe-loaded columns
        # keep the source CSV's original casing; local loads are already
        # lowercase, so this is a no-op there).
        normalize_column_case(conn, "melissa_input")

        if limit is not None:
            print(f"--limit set: truncating melissa_input to {limit} rows")
            conn.execute(
                f"CREATE OR REPLACE TABLE melissa_input AS SELECT * FROM melissa_input LIMIT {limit}"
            )

        # Corrections/outside-of-nyc always load from local files: unlike
        # melissa_input, they aren't sourced from edm-private via recipe.yml
        # yet, so --skip-load doesn't apply to them.
        assert corrections_path and outside_of_nyc_path
        print("Loading reference data (corrections, outside-of-nyc)...")
        load_reference_data(conn, corrections_path, outside_of_nyc_path)

        print("Preprocessing (id assignment, outside-NYC exclusion)...")
        preprocess(conn)
        geocode_stage(conn, chunk_size)
        print("Building merged geocode table...")
        build(conn)
        print("Filling NTA names...")
        fill_nta_names(conn)
        print(f"Writing output to {output_path}...")
        output(conn, output_path)
    finally:
        conn.close()
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DuckDB-based Melissa geocoding pipeline"
    )
    parser.add_argument("output_file", type=Path, help="Output CSV path")
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="Pipe-delimited Melissa input file (required unless --skip-load)",
    )
    parser.add_argument(
        "--corrections",
        type=Path,
        default=HERE / "data" / "melissa_corrections.csv",
        help="Corrections CSV (default: data/melissa_corrections.csv)",
    )
    parser.add_argument(
        "--outside-of-nyc",
        type=Path,
        default=HERE / "data" / "melissa_outsideofnyc.csv",
        help="Outside-of-NYC CSV (default: data/melissa_outsideofnyc.csv)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="DuckDB file to use (default: <output_file>.duckdb, next to the output "
        "file). With --skip-load, point this at the file dcp_load_recipe created "
        "(<recipe product>_<version>.duckdb, see recipe.yml).",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip reading --input-file; assume melissa_input already exists in "
        "--db-path (e.g. loaded via `dcp_load_recipe` against recipe.yml). "
        "melissa_corrections/melissa_outsideofnyc always load from --corrections/"
        "--outside-of-nyc regardless of this flag. Requires --db-path.",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="DuckDB schema all tables live in / should be created in -- match "
        "whatever dcp_load_recipe used (BUILD_ENGINE_SCHEMA env var, if set, "
        "otherwise the recipe build name) when using --skip-load.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Addresses geocoded per DuckDB commit (default: {DEFAULT_CHUNK_SIZE}). "
        "Rerunning the same command against the same --db-path resumes automatically "
        "-- already-geocoded addresses are skipped via an anti-join, so a crash only "
        "loses the in-flight chunk.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N rows of melissa_input (applied right after "
        "load/--skip-load, before preprocessing). For sanity-checking throughput "
        "and output quality against a slice before committing to a full run.",
    )
    args = parser.parse_args()

    if not args.skip_load and args.input_file is None:
        parser.error("--input-file is required unless --skip-load is set")
    if args.skip_load and args.db_path is None:
        parser.error(
            "--skip-load requires --db-path pointing at the recipe-loaded DuckDB file"
        )

    db_path = args.db_path or args.output_file.with_suffix(".duckdb")
    run(
        args.output_file,
        db_path,
        input_path=args.input_file,
        corrections_path=args.corrections,
        outside_of_nyc_path=args.outside_of_nyc,
        skip_load=args.skip_load,
        schema=args.schema,
        chunk_size=args.chunk_size,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
