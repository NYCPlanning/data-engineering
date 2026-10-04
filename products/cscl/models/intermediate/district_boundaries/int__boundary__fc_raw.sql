{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per fire company, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__fc_edges', 'fire_company', include_assigned_water=true) }}
