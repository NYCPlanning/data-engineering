SELECT ST_UNION(buffer) AS geom
FROM {{ ref('stg__dcp_air_quality_vent_towers_buffered') }}
