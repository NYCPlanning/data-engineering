{{ config(materialized='table') }}

-- Topology checks per 2010 NTA (is_valid must be true for every row).

{{ district_boundary_validity('int__boundary__nta2010_edges', 'int__boundary__nta2010_raw', 'int__boundary__nta2010') }}
