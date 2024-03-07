SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int__nysdec_state_facility_permits') }}
