-- Tracts and community districts (joint interest areas included) both cover the whole city, so
-- their totals should match even though tracts don't nest in districts.
{%- set counts = ['population_2010_census', 'population_2020_census', 'housing_units_2010_census', 'housing_units_2020_census'] %}
WITH tracts AS (
    SELECT {% for c in counts %}sum({{ c }}) AS {{ c }}{{ ", " if not loop.last }}{% endfor %}
    FROM {{ ref('dim_census_tract') }}
),

districts AS (
    SELECT {% for c in counts %}sum({{ c }}) AS {{ c }}{{ ", " if not loop.last }}{% endfor %}
    FROM {{ ref('dim_community_district') }}
)

SELECT *
FROM tracts, districts
WHERE {% for c in counts %}tracts.{{ c }} != districts.{{ c }}{{ " OR " if not loop.last }}{% endfor %}
