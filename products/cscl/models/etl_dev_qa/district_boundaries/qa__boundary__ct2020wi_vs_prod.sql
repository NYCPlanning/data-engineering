{{ config(materialized='table') }}

-- int__boundary__ct2020wi vs production_outputs.fgdb_nyct2020wi.

{{ district_boundary_vs_prod(
    'int__boundary__ct2020wi',
    'qa__boundary__ct2020wi_validity',
    'fgdb_nyct2020wi',
    'boroct2020',
    'boroct2020'
) }}
