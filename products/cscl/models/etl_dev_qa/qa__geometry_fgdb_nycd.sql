{{ config(materialized='table', tags=['qa']) }}

-- Full-layer application of geometry_diff_qa (macros/geometry_diff_qa.sql) to nycd
-- (Community Districts) - the layer that originally motivated this whole geometry QA
-- effort (SI01's CDTA bug: Community District 1's 2020
-- tabulation-area equivalent).

{{ geometry_diff_qa(
    build_relation=ref('gdb_nycd'),
    prod_relation=prod_fgdb_linear("fgdb_nycd"),
    key_column='"BoroCD"'
) }}
