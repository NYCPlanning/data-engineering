{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per fire division, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__fd_edges', 'fire_division', include_assigned_water=true) }}
