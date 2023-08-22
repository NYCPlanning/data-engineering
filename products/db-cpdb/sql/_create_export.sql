DROP TABLE IF EXISTS cpdb_dcpattributes_pts;
SELECT * INTO cpdb_dcpattributes_pts
FROM cpdb_dcpattributes
WHERE
    ccpversion =: 'ccp_v'
    AND ST_GEOMETRYTYPE(geom) = 'ST_MultiPoint';

DROP TABLE IF EXISTS cpdb_dcpattributes_poly;
SELECT * INTO cpdb_dcpattributes_poly
FROM cpdb_dcpattributes
WHERE
    ccpversion =: 'ccp_v'
    AND ST_GEOMETRYTYPE(geom) = 'ST_MultiPolygon';
