-- Add geometries to attributes based on fuzzy string matching

-- Join Park geoms to records via park name
-- round 1: like statements with compliations like 'bridge park' removed

WITH master AS(
SELECT a.magency, a.projectid, a.description, b.signname, b.wkb_geometry as geom
FROM cpdb_dcpattributes a,
     dpr_parksproperties b
WHERE a.geom IS NULL AND
      a.magency = '846' AND
      upper(b.signname) <> 'PARK' AND
      upper(b.signname) <> 'LOT' AND
      upper(b.signname) <> 'GARDEN' AND
      upper(b.signname) <> 'TRIANGLE' AND
      upper(b.signname) <> 'SITTING AREA' AND
      upper(b.signname) <> 'BRIDGE PARK' AND
      upper(a.description) LIKE upper('%' || b.signname || '%')    
) 
UPDATE cpdb_dcpattributes
SET geomsource = 'Algorithm',
    dataname='dpr_parksproperties',
    datasource='DPR',
    geom=master.geom
FROM master
WHERE cpdb_dcpattributes.magency=master.magency AND
      cpdb_dcpattributes.projectid=master.projectid AND
      cpdb_dcpattributes.geom IS NULL;

-- round 2: now that some geoms have been filled, add back Bridge Park
WITH master AS(
SELECT a.magency, a.projectid, a.description, b.signname, b.wkb_geometry as geom
FROM cpdb_dcpattributes a,
     dpr_parksproperties b
WHERE a.geom IS NULL AND
      a.magency = '846' AND
      upper(b.signname) <> 'PARK' AND
      upper(b.signname) <> 'LOT' AND
      upper(b.signname) <> 'GARDEN' AND
      upper(b.signname) <> 'TRIANGLE' AND
      upper(b.signname) <> 'SITTING AREA' AND
      upper(a.description) LIKE upper('%' ||b.signname || '%')
)
UPDATE cpdb_dcpattributes
SET geomsource = 'Algorithm',
    dataname='dpr_parksproperties',
    datasource='DPR',
    geom=master.geom
FROM master
WHERE cpdb_dcpattributes.magency=master.magency AND
      cpdb_dcpattributes.projectid=master.projectid AND
      cpdb_dcpattributes.geom IS NULL;

--Join Park geoms to records via fuzzy park name  - fuzzy like statements 
WITH master AS(
SELECT a.magency, a.projectid, a.description, b.signname, b.wkb_geometry as geom
FROM cpdb_dcpattributes a,
     dpr_parksproperties b
WHERE a.geom IS NULL AND
      a.magency = '846' AND
      upper(b.signname) <> 'PARK' AND
      upper(b.signname) <> 'LOT' AND
      upper(b.signname) <> 'GARDEN' AND
      upper(b.signname) <> 'TRIANGLE' AND
      upper(b.signname) <> 'SITTING AREA' AND
      upper(b.signname) <> 'BRIDGE PARK' AND
      levenshtein(upper(a.description), upper('%' ||b.signname || '%')) <=3
)
UPDATE cpdb_dcpattributes
SET geomsource = 'Algorithm',
    dataname='dpr_parksproperties',
    datasource='DPR',
    geom=master.geom
FROM master
WHERE cpdb_dcpattributes.magency=master.magency AND
      cpdb_dcpattributes.projectid=master.projectid AND
      cpdb_dcpattributes.geom IS NULL;

