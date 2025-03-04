-- create maprojid --> traffic analysis zones relational table
DROP TABLE IF EXISTS attributes_maprojid_trafficanalysiszones;
-- spatial join
CREATE TABLE attributes_maprojid_trafficanalysiszones AS
SELECT
    a.maprojid AS feature_id,
    'taz'::text AS admin_boundary_type,
    b.geoid10::text AS admin_boundary_id
FROM cpdb_dcpattributes AS a,
    dcp_trafficanalysiszones AS b
WHERE ST_INTERSECTS(a.geom, ST_SETSRID(b.wkb_geometry, 4326));
