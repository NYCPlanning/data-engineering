{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2010 NTA, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__nta2010_edges', 'nta2010') }}
