{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per municipal court district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__mcwi_edges', 'mc', include_assigned_water=true) }}
