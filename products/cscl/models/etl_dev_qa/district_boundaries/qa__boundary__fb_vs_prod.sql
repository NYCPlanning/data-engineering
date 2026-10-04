{{ config(materialized='table') }}

-- int__boundary__fb vs production_outputs.fgdb_nyfb.

{{ district_boundary_vs_prod(
    'int__boundary__fb',
    'qa__boundary__fb_validity',
    'fgdb_nyfb',
    "firebn::text",
    'fire_battalion'
) }}
