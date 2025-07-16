SELECT ST_Union(buffer_geom) AS buffer_geom
FROM {{ ref("int_spatial__exposed_railway") }}
