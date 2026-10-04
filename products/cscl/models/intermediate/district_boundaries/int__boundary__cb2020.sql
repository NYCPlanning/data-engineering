{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2020 census block boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__cb2020_raw', 'int__boundary__cb2020_edges') }}
