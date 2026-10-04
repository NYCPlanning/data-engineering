{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per school district, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__sd_edges', 'sd', include_assigned_water=true) }}
