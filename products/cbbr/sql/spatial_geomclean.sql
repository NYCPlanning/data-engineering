--Update geom to be multi
UPDATE _cbbr_submissions
SET geom = st_multi(geom)
WHERE st_geometrytype(geom) IN ('ST_Polygon', 'ST_Point', 'ST_LineString');

--Update lines to be polygons
UPDATE _cbbr_submissions
SET geom = st_buffer(geom::geography, 15)::geometry
WHERE st_geometrytype(geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE _cbbr_submissions
SET geom = st_multi(geom)
WHERE st_geometrytype(geom) IN ('ST_Polygon', 'ST_Point', 'ST_LineString');
