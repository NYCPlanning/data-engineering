SELECT *
FROM {{ ref('dim_census_tract') }}
WHERE population_age_under_18_2020_census + population_age_18_24_2020_census + population_age_25_44_2020_census
    + population_age_45_64_2020_census + population_age_65_plus_2020_census != population_2020_census
