{{ config(materialized='table') }}

-- Topology checks per congressional district (water included).

{{ district_boundary_validity('int__boundary__cgwi_edges', 'int__boundary__cgwi_raw', 'int__boundary__cgwi') }}
