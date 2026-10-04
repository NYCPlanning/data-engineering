{{ config(materialized='table') }}

-- Topology checks per 2020 PUMA (is_valid must be true for every row).

{{ district_boundary_validity(
    'int__boundary__puma2020_edges',
    'int__boundary__puma2020_raw',
    'int__boundary__puma2020'
) }}
