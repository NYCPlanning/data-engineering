{{ config(materialized='table') }}

-- int__boundary__puma2020 vs production_outputs.fgdb_nypuma2020.

{{ district_boundary_vs_prod(
    'int__boundary__puma2020',
    'qa__boundary__puma2020_validity',
    'fgdb_nypuma2020',
    "puma",
    'puma2020'
) }}
