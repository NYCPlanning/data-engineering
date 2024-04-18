SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__nyc_parks_properties') }}
