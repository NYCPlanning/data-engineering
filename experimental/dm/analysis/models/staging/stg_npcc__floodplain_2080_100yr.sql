-- One row per polygon. The source is one multipolygon that isn't valid as a whole, so it's split into its parts
-- (about 1,900) and each part is made valid.
SELECT ST_MakeValid(unnest(ST_Dump(geom)).geom) AS geom
FROM {{ source('sources', 'npcc_floodplain_2080_100yr') }}
