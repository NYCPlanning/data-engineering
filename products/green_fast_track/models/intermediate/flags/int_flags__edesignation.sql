WITH dcp_edesignation AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_edesignation_csv') }}
)

SELECT
    bbl,
    'e_des_hazmat' AS flag_id_field_name,
    'e_designations_hazmat' AS variable_type,
    enumber AS variable_id
FROM dcp_edesignation
WHERE hazmat_code = '1'
UNION ALL
SELECT
    bbl,
    'e_des_air_quality' AS flag_id_field_name,
    'e_designations_air' AS variable_type,
    enumber AS variable_id
FROM dcp_edesignation
WHERE air_code = '1'
UNION ALL
SELECT
    bbl,
    'e_des_noise' AS flag_id_field_name,
    'e_designations_noise' AS variable_type,
    enumber AS variable_id
FROM dcp_edesignation
WHERE noise_code = '1'
