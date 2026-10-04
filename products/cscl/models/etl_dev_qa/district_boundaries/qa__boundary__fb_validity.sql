{{ config(materialized='table') }}

-- Topology checks per fire battalion (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__fb_edges', 'int__boundary__fb_raw', 'int__boundary__fb') }}
