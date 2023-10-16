-- create maprojid --> community district relational table
DROP TABLE IF EXISTS attributes_maprojid_cd;
-- spatial join
CREATE TABLE attributes_maprojid_cd AS (
    SELECT a.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'commboard'::text AS admin_boundary_type,
            b.borocd::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_cdboundaries AS b
        WHERE
            ST_INTERSECTS(a.geom, b.wkb_geometry)
            AND ST_GEOMETRYTYPE(b.wkb_geometry) = 'ST_MultiPolygon'
    ) AS a
);
