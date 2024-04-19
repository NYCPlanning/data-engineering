SELECT
    variable_type,
    variable_id,
    ST_MULTI(raw_geom) AS raw_geom
FROM {{ ref('int_buffers__dpr_schoolyard_to_playgrounds') }}
