DROP TABLE IF EXISTS cpdb_dcpattributes_pts;
SELECT * INTO cpdb_dcpattributes_pts
FROM cpdb_dcpattributes
WHERE st_geometrytype(geom) = 'ST_MultiPoint';

DROP TABLE IF EXISTS cpdb_dcpattributes_poly;
SELECT * INTO cpdb_dcpattributes_poly
FROM cpdb_dcpattributes
WHERE st_geometrytype(geom) = 'ST_MultiPolygon';

DROP TABLE IF EXISTS cpdb_projects_pts;
SELECT * INTO cpdb_projects_pts
FROM cpdb_projects_shp
WHERE st_geometrytype(geom) = 'ST_MultiPoint';

DROP TABLE IF EXISTS cpdb_projects_poly;
SELECT * INTO cpdb_projects_poly
FROM cpdb_projects_shp
WHERE st_geometrytype(geom) = 'ST_MultiPolygon';
