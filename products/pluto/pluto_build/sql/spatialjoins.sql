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
FROM stg__dcp_cdboundaries_wi AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.cd IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET
    ct2010 = LEFT(b.ct2010, 4) || '.' || RIGHT(b.ct2010, 2),
    tract2010 = LEFT(b.ct2010, 4) || '.' || RIGHT(b.ct2010, 2)
FROM stg__dcp_ct2010_wi AS b
WHERE
    a.geom && b.geom AND ST_WITHIN(a.centroid, b.geom)
    AND (a.ct2010 IS NULL OR a.ct2010::numeric = 0);

UPDATE pluto a SET
    cb2010 = COALESCE(a.cb2010, b.cb2010)
FROM stg__dcp_cb2010_wi AS b
WHERE a.geom && b.geom AND ST_WITHIN(a.centroid, b.geom);

UPDATE pluto a SET
    bct2020 = COALESCE(a.bct2020, b.boroct2020)
FROM stg__dcp_ct2020_wi AS b
WHERE a.geom && b.geom AND ST_WITHIN(a.centroid, b.geom);

UPDATE pluto a SET
    bctcb2020 = COALESCE(a.bctcb2020, b.bctcb2020)
FROM stg__dcp_cb2020_wi AS b
WHERE a.geom && b.geom AND ST_WITHIN(a.centroid, b.geom);

UPDATE pluto a
SET schooldist = b.schooldist
FROM stg__dcp_school_districts AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.schooldist IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET council = LTRIM(b.coundist::text, '0')
FROM stg__dcp_councildistricts_wi AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.council IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET firecomp = b.firecotype || LPAD(b.fireconum::text, 3, '0')
FROM stg__dcp_firecompanies AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.firecomp IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET policeprct = b.precinct
FROM stg__dcp_policeprecincts AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.policeprct IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET healthcenterdistrict = b.hcentdist
FROM stg__dcp_healthcenters AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.healthcenterdistrict IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET healtharea = b.healtharea
FROM stg__dcp_healthareas AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.healtharea IS NULL
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET
    sanitdistrict = LEFT(schedulecode, 3),
    sanitsub = RIGHT(schedulecode, 2)
FROM stg__dsny_frequencies AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND (a.sanitsub IS NULL OR a.sanitsub = ' ')
    AND a.xcoord IS NOT NULL;

UPDATE pluto a
SET zipcode = b.zipcode
FROM stg__doitt_zipcodeboundaries AS b
WHERE
    ST_WITHIN(a.centroid, b.geom)
    AND a.zipcode IS NULL
    AND a.xcoord IS NOT NULL;

ALTER TABLE pluto
DROP COLUMN IF EXISTS centroid;
