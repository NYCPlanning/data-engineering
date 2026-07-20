SELECT
    maprojid AS feature_id,
    unnest(ARRAY['borocode'::text, 'nta'::text, 'censtract'::text]) AS admin_boundary_type,
    unnest(ARRAY[borocode::text, nta2020::text, boroct2020::text]) AS admin_boundary_id
FROM {{ ref('cpdb_dcpattributes') }} AS a
INNER JOIN {{ ref('stg__dcp_ct2020') }} AS ct ON st_intersects(a.geom, ct.wkb_geometry)
