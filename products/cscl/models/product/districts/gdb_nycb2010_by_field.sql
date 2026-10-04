{{ config(materialized='table') }}

-- Our own gdb_nycb2010, narrowed/renamed to match qa_int__prod_fgdb_nycb2010's column
-- set exactly (lowercase, same rounding) so generate_diff_summary compares like for
-- like. See that model for why geometry itself is dropped in favor of rounded
-- shape_length/shape_area, and why they're rounded at all.

SELECT
    "BCTCB2010" AS bctcb2010,
    "CB2010" AS cb2010,
    "BoroCode" AS borocode,
    "BoroName" AS boroname,
    "CT2010" AS ct2010,
    round("SHAPE_Length"::numeric) AS shape_length,
    round("SHAPE_Area"::numeric, -1) AS shape_area
FROM {{ ref('gdb_nycb2010') }}
