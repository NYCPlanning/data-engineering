{{ config(materialized='table') }}

-- int__boundary__cdta2020 vs production_outputs.fgdb_nycdta2020.

{{ district_boundary_vs_prod(
    'int__boundary__cdta2020',
    'qa__boundary__cdta2020_validity',
    'fgdb_nycdta2020',
    'cdta2020',
    'cdta2020'
) }}
