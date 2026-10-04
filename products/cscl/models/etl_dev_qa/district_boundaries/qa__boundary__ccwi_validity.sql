{{ config(materialized='table') }}

-- Topology checks per city council district (water included).

{{ district_boundary_validity('int__boundary__ccwi_edges', 'int__boundary__ccwi_raw', 'int__boundary__ccwi') }}
