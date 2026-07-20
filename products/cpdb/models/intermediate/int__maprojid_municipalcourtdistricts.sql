SELECT
    a.maprojid AS feature_id,
    'municourt'::text AS admin_boundary_type,
    b.municourt::text AS admin_boundary_id
FROM {{ ref('cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_municipalcourtdistricts') }} AS b
WHERE ST_INTERSECTS(a.geom, b.wkb_geometry)
