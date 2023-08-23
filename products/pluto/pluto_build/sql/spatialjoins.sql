-- if a lot did not get assigned service areas through Geosupport assign service areas spatially

-- DROP INDEX dcp_censusblocks_gix;
-- CREATE INDEX dcp_censusblocks_gix ON dcp_censusblocks USING GIST (geom);

UPDATE pluto a
SET xcoord = NULL
WHERE a.xcoord !~ '[0-9]';
UPDATE pluto a
SET ycoord = NULL
WHERE a.ycoord !~ '[0-9]';

UPDATE pluto a
SET cd = b.borocd
FROM dcp_cdboundaries_wi b
WHERE ST_Within(a.centroid, b.geom)
AND a.cd IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET ct2010 = LEFT(b.ct2010,4)||'.'||RIGHT(b.ct2010,2),
    tract2010 = LEFT(b.ct2010,4)||'.'||RIGHT(b.ct2010,2)
FROM dcp_ct2010_wi b
WHERE a.geom&&b.geom AND ST_Within(a.centroid, b.geom)
AND (a.ct2010 IS NULL OR a.ct2010::numeric = 0);

UPDATE pluto a SET 
    cb2010 = coalesce(a.cb2010, b.cb2010)
FROM dcp_cb2010_wi b
WHERE a.geom&&b.geom AND ST_Within(a.centroid, b.geom);

UPDATE pluto a SET 
    bct2020 = coalesce(a.bct2020, b.boroct2020)
FROM dcp_ct2020_wi b
WHERE a.geom&&b.geom AND ST_Within(a.centroid, b.geom);

UPDATE pluto a SET 
    bctcb2020 = coalesce(a.bctcb2020, b.bctcb2020)
FROM dcp_cb2020_wi b
WHERE a.geom&&b.geom AND ST_Within(a.centroid, b.geom);

UPDATE pluto a
SET schooldist = b.schooldist
FROM dcp_school_districts b
WHERE ST_Within(a.centroid, b.geom)
AND a.schooldist IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET council = ltrim(b.coundist::text, '0')
FROM dcp_councildistricts_wi b
WHERE ST_Within(a.centroid, b.geom)
AND a.council IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET firecomp = b.firecotype||lpad(b.fireconum::text,3,'0')
FROM dcp_firecompanies b
WHERE ST_Within(a.centroid, b.geom)
AND a.firecomp IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET policeprct = b.precinct
FROM dcp_policeprecincts b
WHERE ST_Within(a.centroid, b.geom)
AND a.policeprct IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET healthcenterdistrict = b.hcentdist
FROM dcp_healthcenters b
WHERE ST_Within(a.centroid, b.geom)
AND a.healthcenterdistrict IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET healtharea = b.healtharea
FROM dcp_healthareas b
WHERE ST_Within(a.centroid, b.geom)
AND a.healtharea IS NULL
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET sanitdistrict = LEFT(schedulecode,3),
sanitsub = RIGHT(schedulecode,2)
FROM dsny_frequencies b
WHERE ST_Within(a.centroid, b.geom)
AND (a.sanitsub IS NULL OR a.sanitsub = ' ')
AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET zipcode = b.zipcode
FROM doitt_zipcodeboundaries b
WHERE ST_Within(a.centroid, b.geom)
AND a.zipcode IS NULL
AND a.xcoord IS NOT NULL;

ALTER TABLE pluto 
DROP COLUMN IF EXISTS centroid;
