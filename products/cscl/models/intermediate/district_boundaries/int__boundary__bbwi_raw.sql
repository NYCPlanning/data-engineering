{{ config(
    materialized='table'
) }}

-- One row per borough (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__bbwi_edges', 'bb', include_assigned_water=true) }}
