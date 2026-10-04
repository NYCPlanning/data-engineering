{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per hurricane evacuation zone, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__hez_edges', 'hez', include_assigned_water=true) }}
