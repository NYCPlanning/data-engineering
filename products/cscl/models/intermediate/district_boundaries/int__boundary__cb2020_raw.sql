{{ config(
    materialized='table'
) }}

-- One row per 2020 census block, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cb2020_edges', 'cb2020') }}
