-- Add DPR geometries to attributes table
UPDATE dpr_capitalprojects
SET
    wkb_geometry = NULL,
    lat = NULL,
    lon = NULL
WHERE lat = 0 OR lon = 0;

WITH proj AS (
    SELECT
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom,
        REPLACE(fmsid, ' ', '') AS fmsid
    FROM dpr_capitalprojects
    GROUP BY fmsid
)

UPDATE cpdb_dcpattributes
SET
    geom = proj.geom,
    maprojid = proj.fmsid,
    dataname = 'dpr_capitalprojects',
    datasource = 'dpr',
    geomsource = 'dpr'
FROM proj
WHERE
    cpdb_dcpattributes.maprojid = proj.fmsid
    AND proj.geom IS NOT NULL;
