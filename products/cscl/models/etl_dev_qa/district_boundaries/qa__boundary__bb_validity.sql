{{ config(materialized='table') }}

-- Topology checks per borough (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__bb_edges', 'int__boundary__bb_raw', 'int__boundary__bb') }}
