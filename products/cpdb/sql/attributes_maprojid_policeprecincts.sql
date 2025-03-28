-- create maprojid --> police precincts relational table
DROP TABLE IF EXISTS attributes_maprojid_policeprecincts;
-- spatial join
CREATE TABLE attributes_maprojid_policeprecincts AS (
    SELECT a.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'policeprecinct'::text AS admin_boundary_type,
            b.precinct::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_policeprecincts AS b
        WHERE
            ST_INTERSECTS(a.geom, b.wkb_geometry)
            AND ST_GEOMETRYTYPE(b.wkb_geometry) = 'ST_MultiPolygon'
    ) AS a
);
