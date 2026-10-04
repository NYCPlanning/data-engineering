{{ config(materialized='table') }}

-- Topology checks per assembly district (water included).

{{ district_boundary_validity('int__boundary__adwi_edges', 'int__boundary__adwi_raw', 'int__boundary__adwi') }}
