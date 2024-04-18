SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__dcm_arterial_highways') }}
