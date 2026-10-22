{{ config(
    materialized='table'
) }}

-- 2010 census tract boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__ct2010wi_raw', 'int__boundary__ct2010wi_edges') }}
