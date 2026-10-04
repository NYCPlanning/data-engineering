{{ config(materialized='table') }}

-- int__boundary__ct2010wi vs production_outputs.fgdb_nyct2010wi.

{{ district_boundary_vs_prod(
    'int__boundary__ct2010wi',
    'qa__boundary__ct2010wi_validity',
    'fgdb_nyct2010wi',
    'boroct2010',
    'boroct'
) }}
