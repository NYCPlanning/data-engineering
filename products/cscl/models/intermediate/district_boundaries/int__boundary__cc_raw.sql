{{ config(
    materialized='table'
) }}

-- One row per city council district, from its exterior edges, before noise-ring removal.

{{ district_boundary_build_raw('int__boundary__cc_edges', 'cc') }}
