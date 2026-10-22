{{ config(materialized='table', tags=['qa']) }}

-- Full-layer application of geometry_diff_qa (macros/geometry_diff_qa.sql) to
-- nypuma2020 - same motivation/history as qa__geometry_fgdb_nypuma2010 (inv-tv1.2).

-- Plus passes_without_prod_hairlines (see geometry_diff_qa_district_layer).
{{ geometry_diff_qa_district_layer({'name': 'nypuma2020', 'key': '"PUMA"'}) }}
