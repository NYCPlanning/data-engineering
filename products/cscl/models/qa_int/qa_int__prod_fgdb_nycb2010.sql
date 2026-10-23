{{
  config(
    materialized='table',
    tags=['qa', 'qa_prod']
  )
}}

-- Production nycb2010 (2010 Census Blocks clipped to shoreline), narrowed to the
-- columns worth comparing - keyed on BCTCB2010 (lion_outputs.csv's declared key for
-- this layer). objectid (an ESRI load-time sequence, not stable source data) and the
-- geometry itself are dropped: raw geometry essentially never matches byte-for-byte
-- between our build and prod (different overlay engines), so a
-- direct geometry diff would just report ~100% of rows "modified" with no signal in
-- it. shape_length/shape_area stand in for geometry instead, same as
-- poc_validation/compare_gdb.py's own approach for file-based comparison.
--
-- shape_length/shape_area are compared with a relative tolerance in
-- qa__diffs_fgdb_nycb2010, not exactly: GDAL recomputes both on every read, so small
-- disagreement is read-to-read noise, not a real difference.

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "fgdb_nycb2010"
) -%}

SELECT
    bctcb2010,
    cb2010,
    borocode,
    boroname,
    ct2010,
    shape_length,
    shape_area
FROM {{ prod_relation }}
