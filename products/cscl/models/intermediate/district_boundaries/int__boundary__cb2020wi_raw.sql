{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2020 census block (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cb2020wi_edges', 'cb2020', include_assigned_water=true) }}
