{{ config(
    materialized='table'
) }}

-- Health center district boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__hc_raw', 'int__boundary__hc_edges') }}
