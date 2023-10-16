-- Add DPR to attributes table

WITH proj AS (
    SELECT
        regexp_replace(
            regexp_matches(a.description, '[BMQRX][0-9][0-9][0-9]')::text, '[^0-9a-zA-Z]+', '', 'g'
        )::text AS dprparkid,
        *
    FROM cpdb_dcpattributes AS a
    WHERE a.magencyacro = 'DPR'
)

UPDATE cpdb_dcpattributes
SET
    geom = b.wkb_geometry,
    dataname = 'dpr_parksproperties',
    datasource = 'dpr',
    geomsource = 'dpr'
FROM proj AS a,
    dpr_parksproperties AS b
WHERE
    a.dprparkid = b.gispropnum
    AND cpdb_dcpattributes.maprojid = a.maprojid
    AND cpdb_dcpattributes.geom IS NULL;
