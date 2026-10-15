-- Each borough row must equal the sum of its community districts, column by
-- column. A citywide check can't see a unit counted in the wrong borough, since
-- the two errors cancel.
{% set latest = var('cpp_latest_complete_year') | int %}
{% set columns = [
    'units_2020_census',
    'units_2020',
    'completed_units_' ~ (latest - 9) ~ '_' ~ latest,
    'completed_units_2021_' ~ latest,
    'units_' ~ latest,
    'projected_completed_units_' ~ (latest + 1) ~ '_' ~ (latest + 10),
    'projected_units_' ~ (latest + 10),
] %}

WITH cd AS (
    SELECT
        left(geography_id, 1) AS geography_id,
        {% for c in columns -%}
        sum({{ c }}) AS {{ c }}{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('cpp_housing_growth_cd') }}
    GROUP BY 1
)

SELECT
    coalesce(boro.geography_id, cd.geography_id) AS geography_id,
    {% for c in columns -%}
    boro.{{ c }} - cd.{{ c }} AS {{ c }}_difference{{ "," if not loop.last }}
    {% endfor %}
FROM {{ ref('cpp_housing_growth_boro') }} AS boro
FULL JOIN cd ON boro.geography_id = cd.geography_id
WHERE
    {% for c in columns -%}
    boro.{{ c }} IS DISTINCT FROM cd.{{ c }}{{ " OR" if not loop.last }}
    {% endfor %}
