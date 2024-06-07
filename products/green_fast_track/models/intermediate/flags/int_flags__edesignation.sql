WITH dcp_edesignation AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_edesignation_csv') }}
),
ids AS (
    SELECT
        bbl,
        enumber || ' ' || ceqr_num AS variable_id,
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
WHERE hazmat_code = '1'
UNION ALL
SELECT
    bbl,
    'e_des_air_quality' AS flag_id_field_name,
    'e_designations_air' AS variable_type,
    variable_id
FROM ids
WHERE air_code = '1'
UNION ALL
SELECT
    bbl,
    'e_des_noise' AS flag_id_field_name,
    'e_designations_noise' AS variable_type,
    variable_id
FROM ids
WHERE noise_code = '1'
