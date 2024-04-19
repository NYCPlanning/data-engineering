SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__dpr_schoolyard_to_playgrounds') }}
