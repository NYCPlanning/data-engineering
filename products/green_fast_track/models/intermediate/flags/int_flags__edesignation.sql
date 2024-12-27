WITH dcp_edesignation AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_edesignation_csv') }}
),
ids AS (
    SELECT
        bbl,
        CASE
            WHEN ceqr_num IS NULL THEN enumber
            ELSE enumber || ' ' || ceqr_num
        END AS variable_id,
        hazmat_code,
        air_code,
        noise_code
    FROM dcp_edesignation
)
SELECT
    bbl,
    'e_des_hazmat' AS flag_id_field_name,
    'e_designations_hazmat' AS variable_type,
    variable_id
FROM ids
WHERE hazmat_code = 'true'
UNION ALL
SELECT
    bbl,
    'e_des_air_quality' AS flag_id_field_name,
    'e_designations_air' AS variable_type,
    variable_id
FROM ids
WHERE air_code = 'true'
UNION ALL
SELECT
    bbl,
    'e_des_noise' AS flag_id_field_name,
    'e_designations_noise' AS variable_type,
    variable_id
FROM ids
WHERE noise_code = 'true'
