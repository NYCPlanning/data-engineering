SELECT
    housing.borocd,
    spine.community_district_name,
    spine.cdta_name,
    housing.as_of_date,
    housing.housing_units_from_2020_census
FROM {{ ref('int_cd_date_housing_metrics') }} AS housing
INNER JOIN {{ ref('int_cd_spine') }} AS spine ON housing.borocd = spine.borocd
ORDER BY housing.borocd, housing.as_of_date
