{{ config(
    materialized='table'
) }}

-- One row per assembly district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__adwi_edges', 'ad', include_assigned_water=true) }}
