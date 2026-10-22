{{ config(
    materialized='table'
) }}

-- One row per election district, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__ed_edges', 'ed') }}
