-- The series has to start at the dim's 2010 backcast and end at the 2020 census.
SELECT
    mart.borocd,
    mart.as_of_date,
    mart.housing_units_from_2020_census
FROM {{ ref('mart_community_district_date') }} AS mart
INNER JOIN {{ ref('dim_community_district') }} AS dim ON mart.borocd = dim.borocd
WHERE
    (mart.as_of_date = DATE '2010-04-01' AND mart.housing_units_from_2020_census != dim.housing_units_2010_backcast)
    OR (mart.as_of_date = DATE '2020-04-01' AND mart.housing_units_from_2020_census != dim.housing_units_2020_census)
