-- Add DDC geometries to attributes table

WITH proj AS (
    SELECT
        ST_MULTI(ST_UNION(wkb_geometry)) AS geom,
        '850' || projectid AS fmsid
    FROM ddc_capitalprojects_infrastructure
    GROUP BY projectid
)

UPDATE cpdb_dcpattributes SET
    geom = proj.geom,
    dataname = 'ddc_capitalprojects_infrastructure',
    datasource = 'ddc',
    geomsource = 'ddc'
FROM proj
WHERE cpdb_dcpattributes.maprojid = proj.fmsid;


-- - buildings
WITH proj AS (
    SELECT
        ST_MULTI(ST_UNION(ST_CENTROID(wkb_geometry))) AS geom,
        '850' || projectid AS fmsid
    FROM ddc_capitalprojects_publicbuildings
    GROUP BY projectid
)

UPDATE cpdb_dcpattributes SET
    geom = proj.geom,
    dataname = 'ddc_capitalprojects_publicbuildings',
    datasource = 'ddc',
    geomsource = 'ddc'
FROM proj
WHERE cpdb_dcpattributes.maprojid = proj.fmsid;
