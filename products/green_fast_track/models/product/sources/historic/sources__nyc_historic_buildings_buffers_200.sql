-- GIS needs a similar output to sources__nyc_historic_buildings_buffers,
-- but with a slightly wider buffer for `sh_adjacent_hist_res`

SELECT ST_UNION(ST_BUFFER(raw_geom, 200)) AS geom
FROM {{ ref('int_buffers__lpc_landmarks') }}
