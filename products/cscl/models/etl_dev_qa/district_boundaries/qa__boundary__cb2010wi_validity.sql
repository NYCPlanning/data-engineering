{{ config(materialized='table') }}

-- Topology checks per 2010 census block (water included).

{{ district_boundary_validity(
    'int__boundary__cb2010wi_edges',
    'int__boundary__cb2010wi_raw',
    'int__boundary__cb2010wi'
) }}
