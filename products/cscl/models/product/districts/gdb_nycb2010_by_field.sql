{{ config(materialized='table') }}

-- Our own gdb_nycb2010, narrowed/renamed to match qa_int__prod_fgdb_nycb2010's column
-- set exactly (lowercase) so generate_diff_summary compares like for like. See that
-- model for why geometry itself is dropped in favor of shape_length/shape_area.

SELECT
    "BCTCB2010" AS bctcb2010,
    "CB2010" AS cb2010,
    "BoroCode" AS borocode,
    "BoroName" AS boroname,
    "CT2010" AS ct2010,
    "SHAPE_Length" AS shape_length,
    "SHAPE_Area" AS shape_area
FROM {{ ref('gdb_nycb2010') }}
