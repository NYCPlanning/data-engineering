{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2010 census tract, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__ct2010_edges', 'boroct') }}
