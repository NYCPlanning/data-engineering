{{ config(materialized='table') }}

-- int__boundary__ct2020 vs production_outputs.fgdb_nyct2020.

{{ district_boundary_vs_prod(
    'int__boundary__ct2020',
    'qa__boundary__ct2020_validity',
    'fgdb_nyct2020',
    'boroct2020',
    'boroct2020'
) }}
