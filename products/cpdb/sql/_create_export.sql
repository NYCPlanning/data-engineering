DROP TABLE IF EXISTS cpdb_projects_pts;
SELECT * INTO cpdb_projects_pts
FROM cpdb_projects_shp
WHERE
    ccpversion = :'ccp_v'
    AND ST_GEOMETRYTYPE(geom) = 'ST_MultiPoint';

DROP TABLE IF EXISTS cpdb_projects_poly;
SELECT * INTO cpdb_projects_poly
FROM cpdb_projects_shp
WHERE
    ccpversion = :'ccp_v'
    AND ST_GEOMETRYTYPE(geom) = 'ST_MultiPolygon';
