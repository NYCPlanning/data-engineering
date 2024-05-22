{% set sql_statement_question_flags %}
    SELECT * FROM {{ ref('question_flags') }}
{% endset %}

{% if execute %}
    {% set question_flags = run_query(sql_statement_question_flags) %}
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
        flag_id_field_name,
        variable_type,
        variable_id,
        distance
    FROM flags_long
),

flags_wide AS (
    SELECT
        {% for row in question_flags -%}
            /* construct a comma-separated list of values ordered by distance and value */
            array_to_string(
                array_agg(variable_id ORDER BY flag_id_field_name ASC) FILTER (
                    WHERE flag_id_field_name = '{{ row["flag_id_field_name"] }}'
                ),
                ', '
            ) AS "{{ row['flag_id_field_name'] }}",
        {% endfor %}
        bbl
    FROM flags_ranked
    GROUP BY bbl
),

final AS (
    SELECT
        pluto.bbl,
        {% for row in question_flags -%}
            {% if row['flag_field_name'] == 'zoning_category' %}
                /* the flag zoning_category isn't a binary Yes/No */
                f."{{ row['flag_id_field_name'] }}" AS "{{ row['flag_field_name'] }}",
                /* the id column for the flag zoning_category must show source data */
                array_to_string(
                    ARRAY[pluto.zonedist1, pluto.zonedist2, pluto.zonedist3, pluto.zonedist4],
                    ', '
                ) AS "{{ row['flag_id_field_name'] }}",
            {% else %}
                /* determine the flag */
                CASE
                    WHEN f."{{ row['flag_id_field_name'] }}" IS NULL THEN 'No'
                    ELSE 'Yes'
                END AS "{{ row['flag_field_name'] }}",
                /* pass along the value of the flag id */
                f."{{ row['flag_id_field_name'] }}",
            {% endif %}
        {% endfor %}
        pluto.geom
    FROM pluto
    LEFT JOIN flags_wide AS f ON pluto.bbl = f.bbl
)

SELECT * FROM final
ORDER BY bbl ASC
