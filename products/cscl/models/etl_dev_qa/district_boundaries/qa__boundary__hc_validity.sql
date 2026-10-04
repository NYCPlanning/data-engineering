{{ config(materialized='table') }}

-- Topology checks per health center district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__hc_edges', 'int__boundary__hc_raw', 'int__boundary__hc') }}
