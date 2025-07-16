SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__natural_resources") }}
WHERE st_geometrytype(raw_geom) = 'ST_MultiPolygon'
-- a little unfortunate, but this natural resource is joined by bbl rather than spatially
-- maybe it should be exported in a "_lots" file instead?
UNION ALL
SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_flags__dob_natural_resources") }}
