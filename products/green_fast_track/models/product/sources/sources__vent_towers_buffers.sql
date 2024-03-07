SELECT
    variable_id,
    variable_type,
    buffer
FROM {{ ref('stg__dcp_air_quality_vent_towers_buffered') }}
