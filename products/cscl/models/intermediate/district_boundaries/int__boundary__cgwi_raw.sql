{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per congressional district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cgwi_edges', 'cg', include_assigned_water=true) }}
