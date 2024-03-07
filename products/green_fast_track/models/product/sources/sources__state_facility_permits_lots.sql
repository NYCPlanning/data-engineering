SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('int__nysdec_state_facility_permits') }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'ST_MultiPolygon'
