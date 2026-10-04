{{ config(materialized='table') }}

-- Topology checks per community district (water included).

{{ district_boundary_validity('int__boundary__cdwi_edges', 'int__boundary__cdwi_raw', 'int__boundary__cdwi') }}
