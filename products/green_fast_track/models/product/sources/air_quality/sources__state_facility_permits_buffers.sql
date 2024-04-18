SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__nysdec_state_facility_permits') }}
