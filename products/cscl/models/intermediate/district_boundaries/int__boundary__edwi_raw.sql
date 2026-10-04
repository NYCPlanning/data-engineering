{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per election district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__edwi_edges', 'ed', include_assigned_water=true) }}
