{{ config(materialized='table', tags=['qa']) }}

-- Full-layer application of geometry_diff_qa (macros/geometry_diff_qa.sql) to
-- nypuma2010. Motivated this layer's own needle-sliver and clip_to_shoreline_gridsize
-- investigation (inv-tv1.2, chat log 2026-10-05) - was previously only checked by
-- poc_validation/compare_gdb.py's SHAPE_Length/Area tolerance, with no in-db QA at all.

{{ geometry_diff_qa(
    build_relation=ref('gdb_nypuma2010'),
    prod_relation=adapter.get_relation(database="db-cscl", schema="production_outputs", identifier="fgdb_nypuma2010"),
    key_column='"PUMA"'
) }}
