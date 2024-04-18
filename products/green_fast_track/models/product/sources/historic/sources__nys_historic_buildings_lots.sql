SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('int_buffers__nysshpo_historic_buildings') }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'ST_MultiPolygon'
