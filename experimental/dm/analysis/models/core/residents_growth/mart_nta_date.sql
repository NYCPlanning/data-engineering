SELECT
    housing.nta2020,
    spine.nta_name,
    spine.nta_type_name,
    spine.cdta2020,
    housing.as_of_date,
    housing.housing_units_from_2020_census
FROM {{ ref('int_nta_date_housing_metrics') }} AS housing
INNER JOIN {{ ref('dim_nta') }} AS spine ON housing.nta2020 = spine.nta2020
ORDER BY housing.nta2020, housing.as_of_date
