SELECT *
FROM {{ ref('int_housing_survey_household_spine') }}
ORDER BY survey_year, control
