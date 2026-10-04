{{ config(materialized='table') }}

-- Topology checks per hurricane evacuation zone (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__hez_edges', 'int__boundary__hez_raw', 'int__boundary__hez') }}
