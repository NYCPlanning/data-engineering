{{ config(materialized='table') }}

-- int__boundary__nta2020 vs production_outputs.fgdb_nynta2020.

{{ district_boundary_vs_prod(
    'int__boundary__nta2020',
    'qa__boundary__nta2020_validity',
    'fgdb_nynta2020',
    'nta2020',
    'nta2020'
) }}
