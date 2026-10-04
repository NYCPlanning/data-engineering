{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per fire battalion, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__fb_edges', 'fire_battalion', include_assigned_water=true) }}
