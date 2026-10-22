{{ config(
    materialized='table'
) }}

-- One row per borough, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__bb_edges', 'bb') }}
