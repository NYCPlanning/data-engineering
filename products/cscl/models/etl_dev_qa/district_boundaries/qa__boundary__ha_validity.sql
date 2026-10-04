{{ config(materialized='table') }}

-- Topology checks per health area (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__ha_edges', 'int__boundary__ha_raw', 'int__boundary__ha') }}
