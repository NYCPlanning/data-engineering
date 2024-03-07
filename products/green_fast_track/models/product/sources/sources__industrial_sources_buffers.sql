SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int__industrial_sources') }}
