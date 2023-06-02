-- # Update lot area with lot area value from 18v2.1
UPDATE pluto a
SET lotarea = b.lotarea
FROM dcp_mappluto b
WHERE a.bbl=b.bbl::bigint::text
AND a.lotarea = '0'
AND b.lotarea <> 0
AND (((ST_Area(ST_Transform(a.geom,2263))::numeric - ST_Area(ST_Transform(b.geom,2263))::numeric) / ST_Area(ST_Transform(b.geom,2263))::numeric) < 5 
	AND  ((ST_Area(ST_Transform(a.geom,2263))::numeric - ST_Area(ST_Transform(b.geom,2263))::numeric) / ST_Area(ST_Transform(b.geom,2263))::numeric) > -5);

-- # Update lotfront and lotdepth from 18v2.1
UPDATE pluto a
SET lotfront = b.lotfront,
	lotdepth = b.lotdepth
FROM dcp_mappluto b
WHERE a.bbl=b.bbl::bigint::text
AND a.lotfront::numeric = 0
AND a.lotdepth::numeric = 0
AND (b.lotfront::numeric > 0 OR b.lotdepth > 0)
AND a.lotarea::numeric = b.lotarea::numeric;

-- # Update bldgfront and bldgdepth from 18v2.1
UPDATE pluto a
SET bldgfront = b.bldgfront,
	bldgdepth = b.bldgdepth
FROM dcp_mappluto b
WHERE a.bbl=b.bbl::bigint::text
AND a.bldgfront::numeric = 0
AND a.bldgdepth::numeric = 0
AND (b.bldgfront::numeric > 0 OR b.bldgdepth::numeric > 0)
AND a.yearbuilt::numeric = b.yearbuilt::numeric
AND a.lotfront::numeric = b.lotfront::numeric
AND a.lotdepth::numeric = b.lotdepth::numeric;

-- # Recalculate lot area
UPDATE pluto
SET builtfar = round(bldgarea::numeric / lotarea::numeric, 2)
WHERE lotarea <> '0' AND lotarea IS NOT NULL;

-- # Update irrlotcode from 18v2.1
UPDATE pluto a
SET irrlotcode = b.irrlotcode
FROM dcp_mappluto b
WHERE a.bbl=b.bbl::bigint::text
AND a.lotfront::numeric = b.lotfront::numeric
AND a.lotdepth::numeric = b.lotdepth::numeric
AND b.irrlotcode = 'Y';

-- # Set year fileds to 0 where year < 1600
UPDATE pluto a
SET yearbuilt = 0
WHERE yearbuilt::numeric < 1600;

UPDATE pluto a
SET yearalter1 = 0
WHERE yearalter1::numeric < 1600;

UPDATE pluto a
SET yearalter2 = 0
WHERE yearalter2::numeric < 1600;

-- # Take zipcode from 18v2.1
UPDATE pluto a
SET zipcode = b.zipcode
FROM dcp_mappluto b
WHERE a.bbl=b.bbl::bigint::text
AND length(b.zipcode::text) = 5
AND a.zipcode IS NULL;

UPDATE pluto a
SET zipcode = b.zipcode
FROM dcp_mappluto b
WHERE a.bbl=b.bbl::bigint::text
AND (a.zipcode::numeric<>b.zipcode::numeric)
AND length(b.zipcode::text) = 5;

-- # Drop column
ALTER TABLE pluto
DROP COLUMN IF EXISTS mappluto_f;

-- -- create replica
-- DROP TABLE IF EXISTS pluto_original;
-- select * into pluto_original from pluto;