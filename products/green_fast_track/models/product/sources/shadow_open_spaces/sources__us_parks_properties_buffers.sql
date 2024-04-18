SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__us_parks_properties') }}
