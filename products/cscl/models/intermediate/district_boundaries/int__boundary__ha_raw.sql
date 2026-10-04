{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per health area, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__ha_edges', 'health_area') }}
