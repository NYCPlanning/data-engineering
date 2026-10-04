{{ config(materialized='table') }}

-- Topology checks per fire division (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__fd_edges', 'int__boundary__fd_raw', 'int__boundary__fd') }}
