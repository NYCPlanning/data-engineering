SELECT
    source_relation,
    'hra_' || variable_type AS flag_variable_type,
    variable_type,
    variable_id,
    raw_geom,
    ST_BUFFER(raw_geom, 90) AS buffer_geom
FROM {{ ref('int_spatial__historic_resources') }}
