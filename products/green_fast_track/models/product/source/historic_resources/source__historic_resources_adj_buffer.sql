SELECT ST_UNION(buffer_geom) AS buffer_geom
FROM {{ ref("int_spatial__historic_resources_adj") }}
