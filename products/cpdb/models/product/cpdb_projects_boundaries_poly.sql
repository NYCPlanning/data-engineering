SELECT *
FROM {{ ref('cpdb_projects_boundaries') }}
WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiPolygon'
