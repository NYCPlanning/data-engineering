SELECT
    nta.nta2020,
    types.nta_type_name
FROM {{ ref('stg_dcp__nta2020') }} AS nta
LEFT JOIN {{ ref('nta_types') }} AS types ON nta.nta_type = types.nta_type
