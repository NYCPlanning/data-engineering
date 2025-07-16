-- remove faulty geometries from attributes table

-- Remove
UPDATE cpdb_dcpattributes
SET geom = NULL
WHERE
    maprojid IN (SELECT DISTINCT maprojid FROM cpdb_badgeoms)
    AND (geomsource != 'dpr' OR geomsource != 'dot' OR geomsource != 'ddc');

-- Remove from agency data
UPDATE cpdb_dcpattributes
SET geom = NULL
WHERE
    (
        (
            maprojid IN (
                SELECT DISTINCT maprojid
                FROM dcp_cpdb_agencyverified
                WHERE mappable = 'No - Can never be mapped'
            )
        )
        OR
        (
            maprojid IN (
                SELECT DISTINCT maprojid
                FROM dcp_cpdb_agencyverified
                WHERE mappable = 'No - Can be in future'
            )
            AND NOT geomsource IN ('dpr', 'dot', 'ddc', 'edc')
        )
    )
    AND geom IS NOT NULL;

-- Fix projects outside of NYC
-- For now, only fixes point projects with inverted longtiude values
-- All points in NYC are around latitude -73.9
DROP TABLE IF EXISTS cpdb_dcpattributes_not_in_nyc_fixed;

WITH point_projects_not_entirely_in_nyc AS (
    SELECT
        maprojid,
        geom
    FROM cpdb_dcpattributes
    WHERE
        ST_GeometryType(geom) = 'ST_MultiPoint'
        AND ST_XMax(geom) > 0
),

project_points AS (
    SELECT
        maprojid,
        (ST_Dump(geom)).geom AS single_point
    FROM point_projects_not_entirely_in_nyc
),

fixed_points AS (
    SELECT
        maprojid,
        CASE
            WHEN ST_X(single_point) > 0
                THEN ST_SetSRID(ST_MakePoint(-ST_X(single_point), ST_Y(single_point)), 4326)
            ELSE single_point
        END AS single_point_fixed
    FROM project_points
),

projects AS (
    SELECT
        maprojid,
        ST_Multi(ST_Union(single_point_fixed)) AS geom
    FROM fixed_points
    GROUP BY maprojid
)

SELECT *
INTO cpdb_dcpattributes_not_in_nyc_fixed
FROM projects;

UPDATE cpdb_dcpattributes
SET geom = cpdb_dcpattributes_not_in_nyc_fixed.geom
FROM cpdb_dcpattributes_not_in_nyc_fixed
WHERE
    cpdb_dcpattributes.maprojid = cpdb_dcpattributes_not_in_nyc_fixed.maprojid;
