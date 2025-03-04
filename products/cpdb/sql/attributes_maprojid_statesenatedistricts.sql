-- create maprojid --> state senate districts relational table
DROP TABLE IF EXISTS attributes_maprojid_statesenatedistricts;
-- spatial join
CREATE TABLE attributes_maprojid_statesenatedistricts AS
SELECT
    a.maprojid AS feature_id,
    'statesenate'::text AS admin_boundary_type,
    b.stsendist::text AS admin_boundary_id
FROM cpdb_dcpattributes AS a,
    dcp_statesenatedistricts AS b
WHERE ST_INTERSECTS(a.geom, b.wkb_geometry);
