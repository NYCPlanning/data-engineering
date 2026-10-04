{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- One row per 2020 CDTA, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cdta2020_edges', 'cdta2020') }}
