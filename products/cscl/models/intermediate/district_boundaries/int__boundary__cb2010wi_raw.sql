{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2010 census block (water included), before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cb2010wi_edges', 'bctcb2010', include_assigned_water=true) }}
