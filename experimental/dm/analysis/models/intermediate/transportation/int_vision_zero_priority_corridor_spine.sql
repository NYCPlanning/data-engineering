-- the source has no IDs, so a corridor's ID is a hash of its geometry
SELECT
    md5(ST_AsHEXWKB(geom)) AS priority_corridor_id,
    ST_Length(ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true)) / 5280 AS miles,
    geom
FROM {{ ref('stg_dot__vzv_priority_corridors') }}
