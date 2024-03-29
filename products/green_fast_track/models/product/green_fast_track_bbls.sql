{% set sql_statement %}
    SELECT * FROM {{ ref('variables') }}
{% endset %}

{% if execute %}
    {% set variables = run_query(sql_statement) %}
{% endif %}

WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

flags_long AS (
    SELECT * FROM {{ ref('int_flags__all') }}
),

flags_ranked AS (
    SELECT
        bbl,
        variable_type,
        variable_id,
        distance,
        ROW_NUMBER()
            OVER (PARTITION BY bbl, variable_type ORDER BY distance)
        AS row_number
    FROM flags_long
),

flags_wide AS (
    SELECT
        {% for row in variables -%}
            /* construct a comma-separated list of values ordered by distance and name */
            ARRAY_TO_STRING(
                ARRAY_AGG(variable_id ORDER BY distance ASC, variable_id ASC) FILTER (
                    WHERE variable_type = '{{ row["variable_type"] }}'
                ),
                ', '
            ) AS "{{ row['label'] }}",
        {% endfor %}
        bbl
    FROM flags_ranked
    GROUP BY bbl
)

SELECT
    pluto.bbl,
    {% for row in variables -%}
        CASE
            WHEN f."{{ row['label'] }}" IS NULL THEN 'No'
            ELSE 'Yes'
        END AS "{{ row['label'] }} Flag",
        f."{{ row['label'] }}",
    {% endfor %}
    pluto.geom
FROM
    pluto
LEFT JOIN flags_wide AS f ON pluto.bbl = f.bbl
