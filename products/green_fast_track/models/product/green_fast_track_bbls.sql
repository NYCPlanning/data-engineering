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
        row_number()
            OVER (PARTITION BY bbl, variable_type ORDER BY distance)
        AS row_number
    FROM flags_long
),

flags_wide AS (
    SELECT
        {% for row in variables -%}
            /* construct a comma-separated list of values ordered by distance and name */
            array_to_string(
                array_agg(variable_id ORDER BY distance ASC, variable_id ASC) FILTER (
                    WHERE variable_type = '{{ row["variable_type"] }}'
                ),
                ', '
            ) AS "{{ row['label'] }}",
        {% endfor %}
        bool_or(variable_id IS NOT NULL)
        FILTER (
            WHERE variable_type = any(ARRAY{{
                variables
                | selectattr("ceqr_category", "equalto", "Natural Resources")
                | map(attribute='variable_type')
                | list
            }})
            AND variable_type != 'wetlands_checkzones'
        ) AS natural_resource,
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
    CASE
        WHEN f.natural_resource THEN 'Yes'
        ELSE 'No'
    END AS "Contains Natural Resource",
    pluto.geom
FROM pluto
LEFT JOIN flags_wide AS f ON pluto.bbl = f.bbl
