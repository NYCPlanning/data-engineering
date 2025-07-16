UPDATE dcp_cpdb_agencyverified a
SET geom = ST_Centroid(b.wkb_geometry)
FROM doitt_buildingfootprints AS b, dcp_cpdb_agencyverified_geo AS c
WHERE
    b.bin::bigint::text = c.bin::bigint::text AND a.maprojid = c.maprojid AND c.bin IS NOT NULL
    AND b.wkb_geometry IS NOT NULL;

UPDATE dcp_cpdb_agencyverified a
SET geom = ST_SetSRID(ST_MakePoint(c.lon::double precision, c.lat::double precision), 4326)
FROM dcp_cpdb_agencyverified_geo AS c
WHERE a.maprojid = c.maprojid AND c.lon IS NOT NULL AND c.lat IS NOT NULL;

-- UPDATE dcp_cpdb_agencyverified a 
-- SET geom = NULL
-- FROM dcp_cpdb_agencyverified_geo c
-- WHERE a.maprojid = c.maprojid and c.lon is null and c.lat is null and c.bin is null and a.geom is null;

-- Add agency verified geometries to attributes table
WITH proj AS (
    SELECT
        ST_Multi(ST_Union(geom)) AS geom,
        maprojid
    FROM dcp_cpdb_agencyverified
    GROUP BY maprojid
)

UPDATE cpdb_dcpattributes
SET
    geom = proj.geom,
    dataname = 'dcp_cpdb_agencyverified',
    datasource = 'agency',
    geomsource = 'agency'
FROM proj
WHERE
    cpdb_dcpattributes.maprojid = proj.maprojid
    AND proj.geom IS NOT NULL;
