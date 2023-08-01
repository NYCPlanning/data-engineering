-- only allow numeric values in the lot depth field
UPDATE pluto a
SET lotdepth = NULL 
WHERE a.lotdepth ~ '[^0-9]'
AND lotdepth NOT LIKE '%.%';
-- only allow numeric values in the numfloors field
UPDATE pluto a
SET numfloors = NULL 
WHERE a.numfloors ~ '[^0-9]'
AND numfloors NOT LIKE '%.%';
-- only allow numfloors values >= 1
UPDATE pluto a
SET numfloors = NULL
WHERE a.numfloors IS NOT NULL 
AND a.numfloors::numeric < 1;
-- remove commas from lot area
UPDATE pluto a
SET lotarea = REPLACE(lotarea,',','')
WHERE lotarea LIKE '%,%';

-- repetitive with numericfields_geomfields
--where sanborn is just spaces set to NULL
UPDATE pluto a
SET sanborn = NULL
WHERE a.sanborn !~ '[0-9]';

--where x/y cood is just spaces set to NULL
UPDATE pluto a
SET xcoord = NULL
WHERE a.xcoord !~ '[0-9]';
UPDATE pluto a
SET ycoord = NULL
WHERE a.ycoord !~ '[0-9]';

-- make appbbl a single 0 where it's zero
UPDATE pluto
SET appbbl = '0'
WHERE appbbl::numeric = 0;

-- make sanitdistrict numeric
UPDATE pluto
SET sanitdistrict = sanitdistrict::integer;