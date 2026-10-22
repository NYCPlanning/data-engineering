{{ config(
    materialized='table'
) }}

-- One row per health center district, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__hc_edges', 'health_center') }}
