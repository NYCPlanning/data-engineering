{{ config(
    materialized='table'
) }}

-- One row per 2020 PUMA, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__puma2020_edges', 'puma2020') }}
