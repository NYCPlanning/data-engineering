{{ config(
    materialized='table'
) }}

-- Fire battalion boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__fb_raw', 'int__boundary__fb_edges') }}
