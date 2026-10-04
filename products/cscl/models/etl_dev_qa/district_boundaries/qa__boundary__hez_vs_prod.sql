{{ config(materialized='table') }}

-- int__boundary__hez vs production_outputs.fgdb_nyhez.

{{ district_boundary_vs_prod(
    'int__boundary__hez',
    'qa__boundary__hez_validity',
    'fgdb_nyhez',
    'hurricane_evacuation_zone',
    'hez'
) }}
