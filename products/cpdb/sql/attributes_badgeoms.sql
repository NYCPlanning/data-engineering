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
        st_geometrytype(geom) = 'ST_MultiPoint'
        AND st_xmax(geom) > 0
),

project_points AS (
    SELECT
        maprojid,
        (st_dump(geom)).geom AS single_point
    FROM point_projects_not_entirely_in_nyc
),

fixed_points AS (
    SELECT
        maprojid,
        CASE
            WHEN st_x(single_point) > 0
                THEN st_setsrid(st_makepoint(-st_x(single_point), st_y(single_point)), 4326)
            ELSE single_point
        END AS single_point_fixed
    FROM project_points
),

projects AS (
    SELECT
        maprojid,
        st_multi(st_union(single_point_fixed)) AS geom
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
