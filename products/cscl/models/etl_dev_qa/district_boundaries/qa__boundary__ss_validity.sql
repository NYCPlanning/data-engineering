{{ config(materialized='table') }}

-- Topology checks per state senate district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__ss_edges', 'int__boundary__ss_raw', 'int__boundary__ss') }}
