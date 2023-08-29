-- add geometry information onto pluto
-- add a geometry column
SELECT AddGeometryColumn('public', 'pluto', 'geom', 4326, 'Geometry', 2);

-- join on geometry data
UPDATE pluto a
SET geom = St_Makevalid(St_Multi(b.geom))
FROM pluto_dtm AS b
WHERE a.bbl = b.bbl;
