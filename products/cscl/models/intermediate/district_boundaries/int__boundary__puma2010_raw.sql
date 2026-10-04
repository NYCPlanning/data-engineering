{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2010 PUMA, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__puma2010_edges', 'puma') }}
