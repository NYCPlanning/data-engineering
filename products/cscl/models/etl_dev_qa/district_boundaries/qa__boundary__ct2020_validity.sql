{{ config(materialized='table') }}

-- Topology checks per 2020 census tract (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__ct2020_edges', 'int__boundary__ct2020_raw', 'int__boundary__ct2020') }}
