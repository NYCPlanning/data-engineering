{{ config(
    materialized='table'
) }}

-- One row per 2020 NTA, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__nta2020_edges', 'nta2020') }}
