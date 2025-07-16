--Update lines to be polygons
UPDATE cpdb_dcpattributes SET geom = st_snaptogrid(geom, .00001)
WHERE st_geometrytype(geom) = 'ST_MultiLineString';
UPDATE cpdb_dcpattributes
SET geom = st_buffer(geom::geography, 15)::geometry
WHERE st_geometrytype(geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE cpdb_dcpattributes
SET geom = st_multi(geom)
WHERE st_geometrytype(geom) IN ('ST_Polygon', 'ST_Point');
