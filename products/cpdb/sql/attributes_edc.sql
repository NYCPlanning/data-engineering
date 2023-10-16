-- Add EDC geometries to attributes table

WITH proj AS (
    SELECT
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom,
        pjid
    FROM edc_capitalprojects
    GROUP BY pjid
)

UPDATE cpdb_dcpattributes SET
    geom = proj.geom,
    dataname = 'edc_capitalprojects',
    datasource = 'edc',
    geomsource = 'edc'
FROM proj
WHERE cpdb_dcpattributes.maprojid = REPLACE(proj.pjid, ' ', '');


-- - ferry
UPDATE cpdb_dcpattributes SET
    geom = edc_capitalprojects_ferry.wkb_geometry,
    dataname = 'edc_capitalprojects_ferry',
    datasource = 'edc',
    geomsource = 'edc'
FROM edc_capitalprojects_ferry
WHERE cpdb_dcpattributes.maprojid = edc_capitalprojects_ferry.fmsid;
