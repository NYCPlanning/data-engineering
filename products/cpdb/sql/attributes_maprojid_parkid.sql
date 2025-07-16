-- create maprojid --> parkid relational table
DROP TABLE IF EXISTS attributes_maprojid_parkid;
-- Add parkids from dpr_capitalprojects
-- then string matching on id
-- then spatial join
UPDATE dpr_parksproperties
SET wkb_geometry = st_makevalid(wkb_geometry);

UPDATE cpdb_dcpattributes
SET geom = st_makevalid(geom);

CREATE TABLE attributes_maprojid_parkid AS (
    SELECT *
    FROM (
        SELECT
            replace(fmsid, ' ', '') AS maprojid,
            park_id AS parkid
        FROM dpr_capitalprojects_fms_parkid
        UNION
        SELECT
            maprojid,
            regexp_replace(
                regexp_matches(description, '[BMQRX][0-9][0-9][0-9]')::text, '[^0-9a-zA-Z]+', '', 'g'
            )::text AS parkid
        FROM cpdb_dcpattributes
        WHERE magencyacro = 'DPR'
        UNION
        SELECT
            a.maprojid,
            b.gispropnum AS parkid
        FROM cpdb_dcpattributes AS a,
            dpr_parksproperties AS b
        WHERE st_within(a.geom, b.wkb_geometry) AND st_isvalid(a.geom)
    ) AS a
    ORDER BY maprojid
);
