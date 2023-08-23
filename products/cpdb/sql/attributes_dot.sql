-- Add geometries to the attributes table from DOT datasets

-- bridges
UPDATE cpdb_dcpattributes
SET geom = a.wkb_geometry,
    dataname = 'dot_projects_bridges',
    datasource = 'dot',
    geomsource = 'dot'
FROM dot_projects_bridges_byfms as a
WHERE cpdb_dcpattributes.maprojid = regexp_replace(a.fms_id,' ','');

-- intersections: needs to be aggregated to project level
WITH proj AS (
SELECT ST_Multi(ST_Union(wkb_geometry)) as geom,
       fmsagencyid||fmsid as fmsid
FROM dot_projects_intersections
GROUP BY fmsagencyid, fmsid
)
UPDATE cpdb_dcpattributes SET geom = proj.geom,
       dataname = 'dot_projects_intersections',
       datasource = 'dot',
       geomsource = 'dot'
FROM proj
WHERE cpdb_dcpattributes.maprojid = proj.fmsid;

-- streets
WITH proj AS (
SELECT ST_Multi(ST_Union(wkb_geometry)) as geom,
       fmsagencyid||fmsid as fmsid
FROM dot_projects_streets
GROUP BY fmsagencyid, fmsid
)
UPDATE cpdb_dcpattributes SET geom = proj.geom,
       dataname = 'dot_projects_streets',
       datasource = 'dot',
       geomsource = 'dot'
FROM proj
WHERE cpdb_dcpattributes.maprojid = proj.fmsid;

