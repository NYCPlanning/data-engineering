{{ config(materialized='table') }}

-- int__boundary__cb2010 vs production_outputs.fgdb_nycb2010.

{{ district_boundary_vs_prod(
    'int__boundary__cb2010',
    'qa__boundary__cb2010_validity',
    'fgdb_nycb2010',
    'bctcb2010',
    'bctcb2010'
) }}
