SELECT
    variable_id,
    variable_type,
    buffer
FROM {{ ref('stg__dcm_arterial_highways') }}
