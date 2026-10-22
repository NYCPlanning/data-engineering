{{ config(
    materialized='table'
) }}

-- State senate district boundaries, water included, built from the AtomicPolygon topology.

{{ district_boundary_strip_noise_rings('int__boundary__sswi_raw', 'int__boundary__sswi_edges') }}
