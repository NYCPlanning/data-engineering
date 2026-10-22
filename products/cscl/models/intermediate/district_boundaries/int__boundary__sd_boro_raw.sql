{{ config(
    materialized='table'
) }}

-- One row per school district within a borough, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__sd_boro_edges', 'sd_boro', include_assigned_water=true) }}
