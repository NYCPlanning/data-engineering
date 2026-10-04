{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]
) }}

-- Fire battalion boundaries built from the AtomicPolygon topology (macros/district_boundary.sql).

{{ district_boundary_strip_noise_rings('int__boundary__fb_raw', 'int__boundary__fb_edges') }}
