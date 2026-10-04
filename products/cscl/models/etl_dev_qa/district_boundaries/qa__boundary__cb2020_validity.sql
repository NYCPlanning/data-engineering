{{ config(materialized='table') }}

-- Topology checks per 2020 census block (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__cb2020_edges', 'int__boundary__cb2020_raw', 'int__boundary__cb2020') }}
