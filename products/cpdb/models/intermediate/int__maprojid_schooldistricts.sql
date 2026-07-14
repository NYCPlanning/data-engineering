SELECT
    a.maprojid AS feature_id,
    'schooldistrict'::text AS admin_boundary_type,
    b.schooldist::text AS admin_boundary_id
FROM {{ source('cpdb_legacy_pipeline', 'cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_school_districts') }} AS b
WHERE ST_INTERSECTS(a.geom, b.wkb_geometry)
