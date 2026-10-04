{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per community district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cdwi_edges', 'cd', include_assigned_water=true) }}
