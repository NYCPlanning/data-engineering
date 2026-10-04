{{ config(materialized='table') }}

-- Topology checks per city council district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__cc_edges', 'int__boundary__cc_raw', 'int__boundary__cc') }}
