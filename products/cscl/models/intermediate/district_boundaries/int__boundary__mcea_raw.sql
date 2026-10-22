{{ config(
    materialized='table'
) }}

-- One row per MCEA, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__mcea_edges', 'mcea') }}
