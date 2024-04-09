-- add geometry information onto pluto
-- add a geometry column
SELECT addgeometrycolumn(:'build_schema', 'pluto', 'geom', 4326, 'Geometry', 2);

-- join on geometry data
UPDATE pluto a
SET geom = st_makevalid(st_multi(b.geom))
FROM pluto_dtm AS b
WHERE a.bbl = b.bbl;
