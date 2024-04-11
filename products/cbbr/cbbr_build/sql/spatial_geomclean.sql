--Update geom to be multi
UPDATE _cbbr_submissions
SET geom = ST_MULTI(geom)
WHERE ST_GEOMETRYTYPE(geom) IN ('ST_Polygon', 'ST_Point', 'ST_LineString');

--Update lines to be polygons
UPDATE _cbbr_submissions
SET geom = ST_BUFFER(geom::geography, 15)::geometry
WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE _cbbr_submissions
SET geom = ST_MULTI(geom)
WHERE ST_GEOMETRYTYPE(geom) IN ('ST_Polygon', 'ST_Point', 'ST_LineString');
