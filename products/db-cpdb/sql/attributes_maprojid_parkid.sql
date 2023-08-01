-- create maprojid --> parkid relational table
DROP TABLE IF EXISTS attributes_maprojid_parkid;
-- Add parkids from dpr_capitalprojects
-- then string matching on id
-- then spatial join
UPDATE dpr_parksproperties
SET wkb_geometry = ST_MakeValid(wkb_geometry);

UPDATE cpdb_dcpattributes
SET geom = ST_MakeValid(geom); 

CREATE TABLE attributes_maprojid_parkid AS (
  SELECT *
  FROM (
    SELECT replace(fmsid, ' ', '') as maprojid,
           park_id as parkid
    FROM dpr_capitalprojects_fms_parkid
    UNION
    SELECT maprojid,
           regexp_replace(regexp_matches(description, '[BMQRX][0-9][0-9][0-9]')::text,'[^0-9a-zA-Z]+','','g')::text AS parkid
    FROM cpdb_dcpattributes
    WHERE magencyacro = 'DPR'
    UNION
    SELECT a.maprojid,
       b.gispropnum as parkid
    FROM cpdb_dcpattributes a,
       dpr_parksproperties b
    WHERE ST_Within(a.geom, b.wkb_geometry) and ST_IsValid(a.geom)
  ) a
  ORDER BY maprojid
);
