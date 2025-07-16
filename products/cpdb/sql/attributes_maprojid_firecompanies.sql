-- create maprojid --> fire companies relational table
DROP TABLE IF EXISTS attributes_maprojid_firecompanies;
-- spatial join
CREATE TABLE attributes_maprojid_firecompanies AS (
    SELECT a.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'fireconum'::text AS admin_boundary_type,
            b.fireconum::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_firecompanies AS b
        WHERE ST_Intersects(a.geom, b.wkb_geometry)
    ) AS a
    UNION ALL
    SELECT b.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'firecotype'::text AS admin_boundary_type,
            b.firecotype::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_firecompanies AS b
        WHERE ST_Intersects(a.geom, b.wkb_geometry)
    ) AS b
    UNION ALL
    SELECT c.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'firebn'::text AS admin_boundary_type,
            b.firebn::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_firecompanies AS b
        WHERE ST_Intersects(a.geom, b.wkb_geometry)
    ) AS c
    UNION ALL
    SELECT d.*
    FROM (
        SELECT
            a.maprojid AS feature_id,
            'firediv'::text AS admin_boundary_type,
            b.firediv::text AS admin_boundary_id
        FROM cpdb_dcpattributes AS a,
            dcp_firecompanies AS b
        WHERE ST_Intersects(a.geom, b.wkb_geometry)
    ) AS d
);
