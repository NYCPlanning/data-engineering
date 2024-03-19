SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__dcp_air_quality_vent_towers') }}
