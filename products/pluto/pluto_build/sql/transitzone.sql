-- assign transit zone designation based on spatial intersection
-- with the greater transit zone area
-- output is 'Y' if lot intersects with transit zone, NULL otherwise

UPDATE pluto a
SET trnstzone = 'Y'
FROM dcp_greater_transit_zone AS b
WHERE ST_INTERSECTS(a.geom, b.wkb_geometry);
