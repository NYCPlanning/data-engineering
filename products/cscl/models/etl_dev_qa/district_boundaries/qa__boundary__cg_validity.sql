{{ config(materialized='table') }}

-- Topology checks per congressional district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__cg_edges', 'int__boundary__cg_raw', 'int__boundary__cg') }}
