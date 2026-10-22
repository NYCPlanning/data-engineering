{{ config(
    materialized='table'
) }}

-- municipal court district boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__mc_raw', 'int__boundary__mc_edges') }}
