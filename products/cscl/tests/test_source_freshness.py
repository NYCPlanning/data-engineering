"""source_freshness: source layer dates, and prod street codes newer than our source."""

import os
import sys
from pathlib import Path

import duckdb

os.environ.setdefault("VALIDATION_DIR", "/tmp")
sys.path.insert(0, str(Path(__file__).parent.parent / "poc_validation"))

import source_freshness as sf  # noqa: E402


def _conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("create schema production_outputs")
    conn.execute(
        "create table dcp_cscl_featurename as select * from (values "
        "(timestamp '2026-07-20 11:31:55', timestamp '2026-07-21 08:41:49'), "
        "(timestamp '2025-01-01', null)) t(created_date, modified_date)"
    )
    conn.execute(
        "create table dcp_cscl_historicdistrict as "
        "select timestamp '2025-08-13' as created_date, null::timestamp as modified_date"
    )
    conn.execute(
        "create table production_outputs.fgdb_nyhd as "
        "select timestamp '2025-09-01' as created_date, null::timestamp as modified_date"
    )
    # ours: 101948 is the highest code in block 1019; prod adds 101949 (newer) and 101867 (a gap)
    conn.execute(
        "create table int__snd as select * from (values ('1019480101'), ('1018680101')) t(b10sc)"
    )
    conn.execute(
        "create table production_outputs.snd as select * from (values "
        "('1019480101'), ('1018680101'), ('1019490101'), ('1018670101')) t(b10sc)"
    )
    return conn


def _row(rows, layer, measure) -> sf.Row:
    return next(r for r in rows if r.layer == layer and r.measure == measure)


def test_source_layer_dates_and_missing_layers():
    rows = sf.source_layer_rows(_conn())
    assert _row(rows, "dcp_cscl_featurename", "rows").build == "2"
    created = _row(rows, "dcp_cscl_featurename", "max created_date")
    assert created.build == "2026-07-20 11:31:55"
    assert _row(rows, "dcp_cscl_centerline", "max created / modified").warning


def test_dated_prod_layer_newer_than_source_warns():
    rows = sf.dated_prod_layer_rows(_conn())
    row = _row(rows, "dcp_cscl_historicdistrict / fgdb_nyhd", "max created_date")
    assert (row.build, row.prod) == ("2025-08-13 00:00:00", "2025-09-01 00:00:00")
    assert row.warning


def test_street_codes_above_our_block_max_warn():
    rows = {r.measure: r for r in sf.street_code_rows(_conn())}
    assert rows["street codes (B5SC) only in prod"].prod == "2"
    assert rows["street codes (B5SC) only in ours"].build == "0"
    above = rows["prod-only street codes above our highest code in their block of 100"]
    assert above.prod == "1 (e.g. 101949)"
    assert above.warning


def test_markdown_has_a_row_per_measure():
    rows = sf.report(_conn())
    assert sf.to_markdown(rows).count("\n| ") == len(rows) + 1
