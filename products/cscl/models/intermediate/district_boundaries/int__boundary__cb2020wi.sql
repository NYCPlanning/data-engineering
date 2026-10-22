{{ config(
    materialized='table'
) }}

-- 2020 census block boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__cb2020wi_raw', 'int__boundary__cb2020wi_edges') }}
