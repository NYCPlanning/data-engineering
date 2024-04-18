SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__nysparks_historicplaces') }}
