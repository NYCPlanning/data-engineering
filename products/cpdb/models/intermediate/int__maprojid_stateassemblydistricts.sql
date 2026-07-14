SELECT
    a.maprojid AS feature_id,
    'stateassembly'::text AS admin_boundary_type,
    b.assemdist::text AS admin_boundary_id
FROM {{ source('cpdb_legacy_pipeline', 'cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_stateassemblydistricts') }} AS b
WHERE
    ST_INTERSECTS(a.geom, b.wkb_geometry)
    AND ST_GEOMETRYTYPE(b.wkb_geometry) = 'ST_MultiPolygon'
