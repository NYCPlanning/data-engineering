SELECT ST_UNION(buffer) AS geom
FROM {{ ref('int__nysdec_title_v_facility_permits') }}
