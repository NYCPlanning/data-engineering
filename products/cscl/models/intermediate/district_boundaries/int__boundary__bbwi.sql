{{ config(
    materialized='table'
) }}

-- Borough boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__bbwi_raw', 'int__boundary__bbwi_edges') }}
