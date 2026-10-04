{{ config(materialized='table') }}

-- Topology checks per school district (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__sd_edges', 'int__boundary__sd_raw', 'int__boundary__sd') }}
