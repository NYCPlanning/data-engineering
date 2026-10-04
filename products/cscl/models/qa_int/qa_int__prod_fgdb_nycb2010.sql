{{
  config(
    materialized='table',
    tags=['qa', 'qa_prod'],
    indexes=[
      {'columns': ['bctcb2010']}
    ]
  )
}}

-- Production nycb2010 (2010 Census Blocks clipped to shoreline), narrowed to the
-- columns worth comparing - keyed on BCTCB2010 (lion_outputs.csv's declared key for
-- this layer). objectid (an ESRI load-time sequence, not stable source data) and the
-- geometry itself are dropped: raw geometry essentially never matches byte-for-byte
-- between our build and prod (different overlay engines, see CSCL-DISTRICTS-03), so a
-- direct geometry diff would just report ~100% of rows "modified" with no signal in
-- it. shape_length/shape_area stand in for geometry instead, same as
-- poc_validation/compare_gdb.py's own approach for file-based comparison.
--
-- shape_length is rounded to the nearest whole ft, shape_area to the nearest 10 sq ft,
-- before comparing - GDAL recomputes both from scratch on every read, so small
-- disagreement is expected read-to-read noise, not a real difference (see
-- dcpy.geospatial.gdb.compare.DEFAULT_GEOMETRY_DERIVED_FLOAT_COLUMNS, which uses a
-- relative 5e-4 tolerance for the same reason). These are fixed, not relative, so
-- confirmed empirically rather than derived: on a random sample, every nycb2010
-- shape_area "diff" at nearest-whole-sqft rounding was exactly +-1 sqft (a rounding-
-- boundary coincidence, areas here run tens of thousands to low hundreds of
-- thousands sqft) - nearest-10 clears that noise. Not a general-purpose relative
-- tolerance; a layer with much larger or smaller areas may need a different value.

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "fgdb_nycb2010"
) -%}

SELECT
    bctcb2010,
    cb2010,
    borocode,
    boroname,
    ct2010,
    round(shape_length::numeric) AS shape_length,
    round(shape_area::numeric, -1) AS shape_area
FROM {{ prod_relation }}
