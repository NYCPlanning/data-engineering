{{ config(materialized='table') }}

-- Topology checks per MCEA (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__mcea_edges', 'int__boundary__mcea_raw', 'int__boundary__mcea') }}
