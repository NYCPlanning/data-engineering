{{ config(
    materialized='table'
) }}

-- 2020 CDTA boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__cdta2020_raw', 'int__boundary__cdta2020_edges') }}
