{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2010 census block, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cb2010_edges', 'bctcb2010') }}
