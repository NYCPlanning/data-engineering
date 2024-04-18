SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__dep_cats_permits') }}
