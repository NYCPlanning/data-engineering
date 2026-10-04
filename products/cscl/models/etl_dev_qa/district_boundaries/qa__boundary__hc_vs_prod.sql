{{ config(materialized='table') }}

-- int__boundary__hc vs production_outputs.fgdb_nyhc.

{{ district_boundary_vs_prod(
    'int__boundary__hc',
    'qa__boundary__hc_validity',
    'fgdb_nyhc',
    "hcentdist::text",
    'health_center'
) }}
