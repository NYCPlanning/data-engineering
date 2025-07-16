DROP TABLE IF EXISTS cpdb_dcpattributes_pts;
SELECT * INTO cpdb_dcpattributes_pts
FROM cpdb_dcpattributes
WHERE ST_GeometryType(geom) = 'ST_MultiPoint';

DROP TABLE IF EXISTS cpdb_dcpattributes_poly;
SELECT * INTO cpdb_dcpattributes_poly
FROM cpdb_dcpattributes
WHERE ST_GeometryType(geom) = 'ST_MultiPolygon';

DROP TABLE IF EXISTS cpdb_projects_pts;
SELECT * INTO cpdb_projects_pts
FROM cpdb_projects_shp
WHERE ST_GeometryType(geom) = 'ST_MultiPoint';

DROP TABLE IF EXISTS cpdb_projects_poly;
SELECT * INTO cpdb_projects_poly
FROM cpdb_projects_shp
WHERE ST_GeometryType(geom) = 'ST_MultiPolygon';
