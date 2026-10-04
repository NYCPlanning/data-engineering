{{ config(materialized='table') }}

-- Topology checks per 2010 census block (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__cb2010_edges', 'int__boundary__cb2010_raw', 'int__boundary__cb2010') }}
