-- create maprojid --> fire companies relational table
DROP TABLE IF EXISTS attributes_maprojid_firecompanies;
-- spatial join
CREATE TABLE attributes_maprojid_firecompanies AS
SELECT
    maprojid AS feature_id,
    unnest(ARRAY['fireconum'::text, 'firecotype'::text, 'firebn'::text, 'firediv'::text]) AS admin_boundary_type,
    unnest(ARRAY[fireconum::text, firecotype::text, firebn::text, firediv::text]) AS admin_boundary_id
FROM cpdb_dcpattributes AS a,
    dcp_firecompanies AS b
WHERE st_intersects(a.geom, b.wkb_geometry);
