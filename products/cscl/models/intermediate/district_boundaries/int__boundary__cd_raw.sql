{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per community district, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cd_edges', 'cd') }}
