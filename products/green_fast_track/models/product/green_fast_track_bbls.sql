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
    SELECT * FROM {{ ref('int__flags') }}
),

flags_ranked AS (
    SELECT
        bbl,
        variable_type,
        variable_id,
        ROW_NUMBER()
            OVER (PARTITION BY bbl, variable_type ORDER BY distance)
        AS row_number
    FROM flags_long
),

flags_wide AS (
    SELECT
        {% for row in variables -%}
            MAX(
                CASE
                    WHEN
                        variable_type = '{{ row["variable_type"] }}'
                        THEN variable_id
                END
            ) AS "{{ row['label'] }}",
        {% endfor %}
        bbl
    FROM flags_ranked
    WHERE row_number = 1
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
