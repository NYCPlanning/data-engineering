{{ config(
    materialized='table'
) }}

-- 2020 census tract boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__ct2020wi_raw', 'int__boundary__ct2020wi_edges') }}
