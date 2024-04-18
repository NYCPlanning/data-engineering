SELECT
    variable_type,
    variable_id,
    permit_geom AS raw_geom
FROM {{ ref('stg__nysdec_title_v_facility_permits') }}
