{{ config(materialized='table') }}

-- Topology checks per municipal court district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__mc_edges', 'int__boundary__mc_raw', 'int__boundary__mc') }}
