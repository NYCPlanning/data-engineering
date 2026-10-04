{{ config(materialized='table') }}

-- int__boundary__ct2010 vs production_outputs.fgdb_nyct2010.

{{ district_boundary_vs_prod(
    'int__boundary__ct2010',
    'qa__boundary__ct2010_validity',
    'fgdb_nyct2010',
    'boroct2010',
    'boroct'
) }}
