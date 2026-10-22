{{ config(
    materialized='table'
) }}

-- One row per municipal court district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__mcwi_edges', 'mc', include_assigned_water=true) }}
