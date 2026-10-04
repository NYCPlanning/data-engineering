{{ config(materialized='table') }}

-- int__boundary__cb2020wi vs production_outputs.fgdb_nycb2020wi.

{{ district_boundary_vs_prod(
    'int__boundary__cb2020wi',
    'qa__boundary__cb2020wi_validity',
    'fgdb_nycb2020wi',
    'bctcb2020',
    'cb2020'
) }}
