-- add geom field to agency verified data
ALTER TABLE dcp_cpdb_agencyverified
ADD geom geometry;

-- add geom for DOT projects based on bridge ID
UPDATE dcp_cpdb_agencyverified a
SET geom = b.wkb_geometry
FROM dot_projects_bridges_byfms b
WHERE b.bin::text = a.bin::text
AND agency = 'DOT'
AND a.geom IS NULL;

-- add geom for DPR projects based on park ID
UPDATE dcp_cpdb_agencyverified a
SET geom = b.wkb_geometry
FROM dpr_parksproperties b
WHERE b.gispropnum::text = a.bin::text
AND agency = 'DPR'
AND a.geom IS NULL;

-- add geom for projects based on bin
UPDATE dcp_cpdb_agencyverified a
SET geom = ST_Centroid(b.wkb_geometry)
FROM doitt_buildingfootprints b 
WHERE a.bin::bigint::text = b.bin::bigint::text
AND a.bin IS NOT NULL
AND a.geom IS NULL
AND a.bin::text <> '#REF!';

-- add geom for projects based on bbl
UPDATE dcp_cpdb_agencyverified a
SET geom = ST_Centroid(b.wkb_geometry)
FROM dcp_mappluto_wi b 
WHERE a.bbl::text = b.bbl::text
AND a.bbl IS NOT NULL
AND a.geom IS NULL;
