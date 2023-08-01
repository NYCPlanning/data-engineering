-- Add DDC geometries to attributes table

WITH proj AS(
SELECT  ST_Multi(ST_Union(wkb_geometry)) as geom,
       '850'||projectid as fmsid
FROM ddc_capitalprojects_infrastructure
GROUP BY projectid
)
UPDATE cpdb_dcpattributes SET geom = proj.geom,
       dataname = 'ddc_capitalprojects_infrastructure',
       datasource = 'ddc',
       geomsource = 'ddc'
FROM proj
WHERE cpdb_dcpattributes.maprojid = proj.fmsid;


-- - buildings
WITH proj AS(
SELECT  ST_Multi(ST_Union(ST_Centroid(wkb_geometry))) as geom,
       '850'||projectid as fmsid
FROM ddc_capitalprojects_publicbuildings
GROUP BY projectid
)
UPDATE cpdb_dcpattributes SET geom = proj.geom,
       dataname = 'ddc_capitalprojects_publicbuildings',
       datasource = 'ddc',
       geomsource = 'ddc'
FROM proj
WHERE cpdb_dcpattributes.maprojid = proj.fmsid;
 
