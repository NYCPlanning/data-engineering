{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2020 PUMA boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__puma2020_raw', 'int__boundary__puma2020_edges') }}
