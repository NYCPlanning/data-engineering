{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per state senate district (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__sswi_edges', 'ss', include_assigned_water=true) }}
