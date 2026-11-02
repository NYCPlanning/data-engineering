-- NTAs and tracts come from separate PFF rows and boundary files, so check they agree.
{%- set counts = [
    'population_2010_census', 'population_2020_census', 'population_in_households_2020_census',
    'housing_units_2010_census', 'housing_units_2020_census',
    'population_age_under_18_2020_census', 'population_age_18_24_2020_census', 'population_age_25_44_2020_census',
    'population_age_45_64_2020_census', 'population_age_65_plus_2020_census',
    'households_acs', 'households_income_under_25k_acs', 'households_income_25k_50k_acs',
    'households_income_50k_100k_acs', 'households_income_100k_200k_acs', 'households_income_200k_plus_acs',
] %}
WITH tracts AS (
    SELECT
        nta2020,
        sum(land_acres) AS land_acres,
        {% for c in counts %}sum({{ c }}) AS {{ c }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('dim_census_tract') }}
    GROUP BY nta2020
)

SELECT ntas.nta2020
FROM {{ ref('dim_nta') }} AS ntas
FULL JOIN tracts ON ntas.nta2020 = tracts.nta2020
WHERE ntas.nta2020 IS NULL
    OR tracts.nta2020 IS NULL
    OR abs(ntas.land_acres - tracts.land_acres) > 0.01
    {% for c in counts %}OR abs(ntas.{{ c }} - tracts.{{ c }}) > 0.5
    {% endfor %}
