{{ config(
    materialized='table'
) }}

-- One row per 2020 census tract (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__ct2020wi_edges', 'boroct2020', include_assigned_water=true) }}
