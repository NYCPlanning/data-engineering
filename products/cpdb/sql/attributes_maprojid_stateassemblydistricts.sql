-- create maprojid --> state assembly districts relational table
DROP TABLE IF EXISTS attributes_maprojid_stateassemblydistricts;
-- spatial join
CREATE TABLE attributes_maprojid_stateassemblydistricts AS
SELECT
    a.maprojid AS feature_id,
    'stateassembly'::text AS admin_boundary_type,
    b.assemdist::text AS admin_boundary_id
FROM cpdb_dcpattributes AS a,
    dcp_stateassemblydistricts AS b
WHERE
    ST_INTERSECTS(a.geom, b.wkb_geometry)
    AND ST_GEOMETRYTYPE(b.wkb_geometry) = 'ST_MultiPolygon';
