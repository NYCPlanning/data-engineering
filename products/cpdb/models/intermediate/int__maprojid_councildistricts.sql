SELECT
    a.maprojid AS feature_id,
    'council'::text AS admin_boundary_type,
    b.coundist::text AS admin_boundary_id
FROM {{ source('cpdb_legacy_pipeline', 'cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_councildistricts') }} AS b
WHERE ST_INTERSECTS(a.geom, b.wkb_geometry)
