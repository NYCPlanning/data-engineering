{{ config(materialized='table', tags=['qa']) }}

-- Full-layer application of geometry_diff_qa (macros/geometry_diff_qa.sql) to nyfb
-- (Fire Battalions) - an unfrozen, still-open layer (seeds/lion_outputs.csv:
-- "Shoreline-clip geometry noise (CSCL-DISTRICTS-03), not yet borough-bound"),
-- picked to see how the new multi-metric check performs against a real layer beyond
-- the curated test_cases.examples fixtures.

{{ geometry_diff_qa(
    build_relation=ref('gdb_nyfb'),
    prod_relation=adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_nyfb"),
    key_column='"FireBN"'
) }}
