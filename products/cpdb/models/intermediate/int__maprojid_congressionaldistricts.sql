SELECT
    a.maprojid AS feature_id,
    'congdist'::text AS admin_boundary_type,
    b.congdist::text AS admin_boundary_id
FROM {{ source('cpdb_legacy_pipeline', 'cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_congressionaldistricts') }} AS b
WHERE a.geom && b.wkb_geometry AND ST_INTERSECTS(a.geom, b.wkb_geometry)
