{{ config(materialized='table') }}

-- Topology checks per fire company (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__fc_edges', 'int__boundary__fc_raw', 'int__boundary__fc') }}
