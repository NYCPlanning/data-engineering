WITH dcp_edesignation AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'dcp_edesignation_csv') }}
)

SELECT
    bbl,
    'edesignation_hazmat' AS variable_type,
    enumber AS variable_id
FROM dcp_edesignation
WHERE hazmat_code = '1'
UNION ALL
SELECT
    bbl,
    'edesignation_air' AS variable_type,
    enumber AS variable_id
FROM dcp_edesignation
WHERE air_code = '1'
UNION ALL
SELECT
    bbl,
    'edesignation_noise' AS variable_type,
    enumber AS variable_id
FROM dcp_edesignation
WHERE noise_code = '1'
