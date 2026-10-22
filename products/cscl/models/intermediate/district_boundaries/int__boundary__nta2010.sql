{{ config(
    materialized='table'
) }}

-- 2010 NTA boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__nta2010_raw', 'int__boundary__nta2010_edges') }}
