{{ config(
    materialized='table'
) }}

-- Assembly district boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__adwi_raw', 'int__boundary__adwi_edges') }}
