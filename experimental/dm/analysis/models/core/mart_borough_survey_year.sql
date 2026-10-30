SELECT
    metrics.boro_code,
    boroughs.boro_name,
    metrics.* EXCLUDE (boro_code)
FROM {{ ref('int_borough_survey_year_household_size_metrics') }} AS metrics
INNER JOIN (SELECT DISTINCT boro_code, boro_name FROM {{ ref('int_cd_spine') }}) AS boroughs
    ON metrics.boro_code = boroughs.boro_code
ORDER BY metrics.boro_code, metrics.survey_year
