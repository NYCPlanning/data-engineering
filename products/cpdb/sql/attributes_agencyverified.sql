UPDATE dcp_cpdb_agencyverified a
SET geom = ST_CENTROID(b.wkb_geometry)
FROM doitt_buildingfootprints AS b, dcp_cpdb_agencyverified_geo AS c
WHERE
    b.bin::bigint::text = c.bin::bigint::text AND a.maprojid = c.maprojid AND c.bin IS NOT null
    AND b.wkb_geometry IS NOT null;

UPDATE dcp_cpdb_agencyverified a
SET geom = ST_SETSRID(ST_MAKEPOINT(c.lon::double precision, c.lat::double precision), 4326)
FROM dcp_cpdb_agencyverified_geo AS c
WHERE a.maprojid = c.maprojid AND c.lon IS NOT null AND c.lat IS NOT null;

-- UPDATE dcp_cpdb_agencyverified a 
-- SET geom = NULL
-- FROM dcp_cpdb_agencyverified_geo c
-- WHERE a.maprojid = c.maprojid and c.lon is null and c.lat is null and c.bin is null and a.geom is null;

-- Add agency verified geometries to attributes table
WITH proj AS (
    SELECT
        ST_MULTI(ST_UNION(geom)) AS geom,
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
    AND proj.geom IS NOT null;
