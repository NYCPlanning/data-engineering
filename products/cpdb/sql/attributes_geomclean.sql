--Update lines to be polygons
UPDATE cpdb_dcpattributes SET geom = ST_SNAPTOGRID(geom, .00001)
WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiLineString';
UPDATE cpdb_dcpattributes
SET geom = ST_BUFFER(geom::geography, 15)::geometry
WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE cpdb_dcpattributes
SET geom = ST_MULTI(geom)
WHERE ST_GEOMETRYTYPE(geom) IN ('ST_Polygon', 'ST_Point');
