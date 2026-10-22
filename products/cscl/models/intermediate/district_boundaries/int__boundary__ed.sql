{{ config(
    materialized='table'
) }}

-- election district boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__ed_raw', 'int__boundary__ed_edges') }}
