SELECT ST_UNION_AGG(buffer_geom) AS buffer_geom
FROM {{ ref("int_spatial__arterial_highway") }}
