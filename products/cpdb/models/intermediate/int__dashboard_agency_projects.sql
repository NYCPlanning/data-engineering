-- the Capital Projects Dashboard's reporting period whose FMS data is the latest on or before
-- the FISA extract the build's commitment plan comes from
WITH plan_extract AS (
    SELECT TO_DATE(MAX(data_library_version), 'YYYYMMDD') AS extract_date
    FROM {{ source('recipe_sources', 'fisa_capitalcommitments') }}
),

period AS (
    SELECT MAX(reporting_period) AS reporting_period
    FROM {{ ref('stg__moo_capital_projects_budget_schedule') }}
    WHERE fms_data_date <= (SELECT extract_date FROM plan_extract)
)

SELECT d.*
FROM {{ ref('stg__moo_capital_projects_budget_schedule') }} AS d
INNER JOIN period AS p ON d.reporting_period = p.reporting_period
