SELECT
    a.maprojid AS feature_id,
    'policeprecinct'::text AS admin_boundary_type,
    b.precinct::text AS admin_boundary_id
FROM {{ source('cpdb_legacy_pipeline', 'cpdb_dcpattributes') }} AS a,
    {{ ref('stg__dcp_policeprecincts') }} AS b
WHERE
    ST_INTERSECTS(a.geom, b.wkb_geometry)
    AND ST_GEOMETRYTYPE(b.wkb_geometry) = 'ST_MultiPolygon'
