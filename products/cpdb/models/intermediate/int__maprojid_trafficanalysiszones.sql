SELECT
    a.maprojid AS feature_id,
    'taz'::text AS admin_boundary_type,
    b.geoid10::text AS admin_boundary_id
FROM {{ source('cpdb_legacy_pipeline', 'cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_trafficanalysiszones') }} AS b
WHERE ST_INTERSECTS(a.geom, ST_SETSRID(b.wkb_geometry, 4326))
