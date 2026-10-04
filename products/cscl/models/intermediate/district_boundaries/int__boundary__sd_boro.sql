{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]
) }}

-- School district boundaries split by borough, built from the AtomicPolygon topology
-- (gdb_nysd's shape: District 10 is published once per borough).

{{ district_boundary_strip_noise_rings('int__boundary__sd_boro_raw', 'int__boundary__sd_boro_edges') }}
