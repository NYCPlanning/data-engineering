{{ config(
    materialized='table'
) }}

-- One row per 2010 census tract (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__ct2010wi_edges', 'boroct', include_assigned_water=true) }}
