-- `hr_site_90_ft_hist_res`

SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__lpc_landmarks') }}
