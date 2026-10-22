{{ config(
    materialized='table'
) }}

-- Community district boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__cdwi_raw', 'int__boundary__cdwi_edges') }}
