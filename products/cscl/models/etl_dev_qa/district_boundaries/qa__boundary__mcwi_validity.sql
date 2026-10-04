{{ config(materialized='table') }}

-- Topology checks per municipal court district (water included).

{{ district_boundary_validity('int__boundary__mcwi_edges', 'int__boundary__mcwi_raw', 'int__boundary__mcwi') }}
