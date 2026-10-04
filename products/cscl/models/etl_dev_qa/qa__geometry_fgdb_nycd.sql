{{ config(materialized='table', tags=['qa']) }}

-- Full-layer application of geometry_diff_qa (macros/geometry_diff_qa.sql) to nycd
-- (Community Districts) - the layer that originally motivated this whole geometry QA
-- effort (SI01's CDTA bug, chat log 2026-10-04, was Community District 1's 2020
-- tabulation-area equivalent). An unfrozen, still-open layer (seeds/lion_outputs.csv:
-- "Shoreline-clip geometry noise (CSCL-DISTRICTS-03), not yet borough-bound").

{{ geometry_diff_qa(
    build_relation=ref('gdb_nycd'),
    prod_relation=adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_nycd"),
    key_column='"BoroCD"'
) }}
