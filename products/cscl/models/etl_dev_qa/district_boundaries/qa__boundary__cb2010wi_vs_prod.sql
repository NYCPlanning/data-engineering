{{ config(materialized='table') }}

-- int__boundary__cb2010wi vs production_outputs.fgdb_nycb2010wi.

{{ district_boundary_vs_prod(
    'int__boundary__cb2010wi',
    'qa__boundary__cb2010wi_validity',
    'fgdb_nycb2010wi',
    'bctcb2010',
    'bctcb2010'
) }}
