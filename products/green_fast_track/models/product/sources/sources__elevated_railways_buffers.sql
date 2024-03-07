SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int__elevated_railways') }}
