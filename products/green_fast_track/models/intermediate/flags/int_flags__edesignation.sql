WITH dcp_edesignation AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_edesignation_csv') }}
),

all_flags AS (
    SELECT
        bbl,
        'e_des_hazmat' AS flag_variable_type,
        'e_designations_hazmat' AS variable_type,
        enumber AS variable_id
    FROM dcp_edesignation
    WHERE hazmat_code = '1'
    UNION ALL
    SELECT
        bbl,
        'e_des_air_quality' AS flag_variable_type,
        'e_designations_air' AS variable_type,
        enumber AS variable_id
    FROM dcp_edesignation
    WHERE air_code = '1'
    UNION ALL
    SELECT
        bbl,
        'e_des_noise' AS flag_variable_type,
        'e_designations_noise' AS variable_type,
        enumber AS variable_id
    FROM dcp_edesignation
    WHERE noise_code = '1'
)

SELECT
    bbl,
    flag_variable_type,
    variable_type,
    variable_id
FROM all_flags
