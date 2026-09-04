SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__natural_resources") }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'MULTIPOLYGON'
-- a little unfortunate, but this natural resource is joined by bbl rather than spatially
-- maybe it should be exported in a "_lots" file instead?
UNION ALL
SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_flags__dob_natural_resources") }}
