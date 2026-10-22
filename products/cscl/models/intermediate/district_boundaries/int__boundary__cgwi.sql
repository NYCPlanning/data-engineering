{{ config(
    materialized='table'
) }}

-- Congressional district boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__cgwi_raw', 'int__boundary__cgwi_edges') }}
