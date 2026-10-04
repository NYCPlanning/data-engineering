{{ config(materialized='table') }}

-- int__boundary__cb2020 vs production_outputs.fgdb_nycb2020.

{{ district_boundary_vs_prod(
    'int__boundary__cb2020',
    'qa__boundary__cb2020_validity',
    'fgdb_nycb2020',
    'bctcb2020',
    'cb2020'
) }}
