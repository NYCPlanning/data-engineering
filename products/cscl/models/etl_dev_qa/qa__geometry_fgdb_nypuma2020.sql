{{ config(materialized='table', tags=['qa']) }}

-- Full-layer application of geometry_diff_qa (macros/geometry_diff_qa.sql) to
-- nypuma2020 - same motivation/history as qa__geometry_fgdb_nypuma2010 (inv-tv1.2,
-- chat log 2026-10-05).

{{ geometry_diff_qa(
    build_relation=ref('gdb_nypuma2020'),
    prod_relation=adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_nypuma2020"),
    key_column='"PUMA"'
) }}
