SELECT ST_MakeValid(geom) AS geom
FROM {{ source('sources', 'dpr_walk_to_a_park') }}
