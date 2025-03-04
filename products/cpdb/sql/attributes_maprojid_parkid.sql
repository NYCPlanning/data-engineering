-- create maprojid --> parkid relational table
DROP TABLE IF EXISTS attributes_maprojid_parkid;
-- Add parkids from dpr_capitalprojects
-- then string matching on id
-- then spatial join
UPDATE dpr_parksproperties
SET wkb_geometry = ST_MAKEVALID(wkb_geometry);

UPDATE cpdb_dcpattributes
SET geom = ST_MAKEVALID(geom);

CREATE TABLE attributes_maprojid_parkid AS
SELECT
    REPLACE(fmsid, ' ', '') AS maprojid,
    park_id AS parkid
FROM dpr_capitalprojects_fms_parkid
UNION
SELECT
    maprojid,
    REGEXP_REPLACE(
        REGEXP_MATCHES(description, '[BMQRX][0-9][0-9][0-9]')::text, '[^0-9a-zA-Z]+', '', 'g'
    )::text AS parkid
FROM cpdb_dcpattributes
WHERE magencyacro = 'DPR'
UNION
SELECT
    a.maprojid,
    b.gispropnum AS parkid
FROM cpdb_dcpattributes AS a,
    dpr_parksproperties AS b
WHERE ST_WITHIN(a.geom, b.wkb_geometry) AND ST_ISVALID(a.geom)
ORDER BY maprojid;
