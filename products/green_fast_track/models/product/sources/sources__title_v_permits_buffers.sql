SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__nysdec_title_v_facility_permits') }}
