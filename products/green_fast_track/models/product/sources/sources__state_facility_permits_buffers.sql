SELECT
    variable_type,
    variable_id,
    buffer
FROM {{ ref('int__nysdec_state_facility_permits') }}
