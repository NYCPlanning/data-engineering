{{ config(
    materialized='table'
) }}

-- 2010 PUMA boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__puma2010_raw', 'int__boundary__puma2010_edges') }}
