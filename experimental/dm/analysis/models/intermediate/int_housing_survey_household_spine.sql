SELECT
    occupied.survey_year,
    occupied.control,
    units.boro_code,
    units.is_built_2010_or_later,
    occupied.* EXCLUDE (survey_year, control)
FROM {{ ref('stg_hvs__occupied') }} AS occupied
INNER JOIN {{ ref('stg_hvs__all_units') }} AS units
    ON occupied.survey_year = units.survey_year AND occupied.control = units.control
