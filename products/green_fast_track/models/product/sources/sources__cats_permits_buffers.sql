SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int__dep_cats_permits') }}
