SELECT
    maprojid AS feature_id,
    unnest(ARRAY['fireconum'::text, 'firecotype'::text, 'firebn'::text, 'firediv'::text]) AS admin_boundary_type,
    unnest(ARRAY[fireconum::text, firecotype::text, firebn::text, firediv::text]) AS admin_boundary_id
FROM {{ ref('cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_firecompanies') }} AS b
WHERE st_intersects(a.geom, b.wkb_geometry)
