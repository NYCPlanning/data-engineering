{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per city council district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__ccwi_edges', 'cc', include_assigned_water=true) }}
