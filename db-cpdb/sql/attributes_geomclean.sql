--Update lines to be polygons
UPDATE cpdb_dcpattributes SET geom = ST_SnapToGrid(geom, .00001) WHERE ST_GeometryType(geom) ='ST_MultiLineString';
UPDATE cpdb_dcpattributes
SET geom=ST_Buffer(geom::geography, 15)::geometry
WHERE ST_GeometryType(geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE cpdb_dcpattributes
SET geom=ST_Multi(geom)
WHERE ST_GeometryType(geom) in ('ST_Polygon', 'ST_Point');