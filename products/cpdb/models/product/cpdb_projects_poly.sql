SELECT *
FROM {{ ref('cpdb_projects_shp') }}
WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiPolygon'
