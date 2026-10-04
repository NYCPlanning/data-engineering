{{ config(materialized='table') }}

-- Topology checks per 2020 census block (water included).

{{ district_boundary_validity(
    'int__boundary__cb2020wi_edges',
    'int__boundary__cb2020wi_raw',
    'int__boundary__cb2020wi'
) }}
