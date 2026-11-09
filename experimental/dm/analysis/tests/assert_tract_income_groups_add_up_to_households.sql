SELECT *
FROM {{ ref('dim_census_tract') }}
WHERE abs(
    households_income_under_25k_acs + households_income_25k_50k_acs + households_income_50k_100k_acs
    + households_income_100k_200k_acs + households_income_200k_plus_acs - households_acs
) > 0.5
