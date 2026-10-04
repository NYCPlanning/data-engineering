{{ config(materialized='table') }}

-- Topology checks per election district (water included).

{{ district_boundary_validity('int__boundary__edwi_edges', 'int__boundary__edwi_raw', 'int__boundary__edwi') }}
