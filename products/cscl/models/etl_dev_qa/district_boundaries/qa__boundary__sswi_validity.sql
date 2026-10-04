{{ config(materialized='table') }}

-- Topology checks per state senate district (water included).

{{ district_boundary_validity('int__boundary__sswi_edges', 'int__boundary__sswi_raw', 'int__boundary__sswi') }}
