SELECT
    variable_type,
    variable_id,
    buffer
FROM {{ ref('int__nysdec_title_v_facility_permits') }}
