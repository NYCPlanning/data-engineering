{{ config(materialized='table') }}

-- Topology checks per community district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__cd_edges', 'int__boundary__cd_raw', 'int__boundary__cd') }}
