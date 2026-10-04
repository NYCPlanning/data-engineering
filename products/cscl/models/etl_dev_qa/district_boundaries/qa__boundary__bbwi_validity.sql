{{ config(materialized='table') }}

-- Topology checks per borough (water included).

{{ district_boundary_validity('int__boundary__bbwi_edges', 'int__boundary__bbwi_raw', 'int__boundary__bbwi') }}
