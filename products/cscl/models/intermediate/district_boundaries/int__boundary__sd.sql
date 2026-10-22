{{ config(
    materialized='table'
) }}

-- school district boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__sd_raw', 'int__boundary__sd_edges') }}
