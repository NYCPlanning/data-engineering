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

/*
Each flag used to be its own array_agg(...) FILTER (WHERE ...) column computed together in one
GROUP BY. Duckdb doesn't share the underlying grouped scan across FILTER'd aggregates the way
postgres does -- memory scaled roughly linearly per concurrent array_agg column in testing (1
column ~1s, 15 ~8s, 22 OOM at 4GB) -- so each flag is aggregated in its own small GROUP BY here
instead, then joined together. Distributing the 22x cost across sequential small aggregations
instead of one giant concurrent one is what avoids the OOM.
*/
{% for row in question_flags %}
flag_{{ loop.index0 }} AS (
    SELECT
        bbl,
        /* construct a comma-separated list of values ordered by distance and value */
        array_to_string(
            array_agg(variable_id ORDER BY flag_id_field_name ASC),
            ', '
        ) AS "{{ row['flag_id_field_name'] }}"
    FROM flags_ranked
    WHERE flag_id_field_name = '{{ row["flag_id_field_name"] }}'
    GROUP BY bbl
),
{% endfor %}

flagged_bbls AS (
    SELECT DISTINCT bbl FROM flags_ranked
),

flags_wide AS (
    SELECT
        b.bbl,
        {% for row in question_flags -%}
        flag_{{ loop.index0 }}."{{ row['flag_id_field_name'] }}",
        {% endfor %}
    FROM flagged_bbls AS b
    {% for row in question_flags -%}
    LEFT JOIN flag_{{ loop.index0 }} ON b.bbl = flag_{{ loop.index0 }}.bbl
    {% endfor %}
),

final AS (
    SELECT
        pluto.bbl,
        {% for row in question_flags -%}
            {% if row['flag_field_name'] == 'zoning_category' %}
                /* the flag zoning_category isn't a binary Yes/No */
                flags_wide."{{ row['flag_id_field_name'] }}" AS "{{ row['flag_field_name'] }}",
                /* the id column for the flag zoning_category must show source data */
                array_to_string(
                    ARRAY[pluto.zonedist1, pluto.zonedist2, pluto.zonedist3, pluto.zonedist4],
                    ', '
                ) AS "{{ row['flag_id_field_name'] }}",
            {% else %}
                /* determine the flag */
                CASE
                    WHEN flags_wide."{{ row['flag_id_field_name'] }}" IS NULL THEN 'No'
                    ELSE 'Yes'
                END AS "{{ row['flag_field_name'] }}",
                /* pass along the value of the flag id */
                flags_wide."{{ row['flag_id_field_name'] }}",
            {% endif %}
        {% endfor %}
        -- strip the embedded CRS type modifier (GEOMETRY('EPSG:2263')) so this matches the
        -- generic `geometry` contract type below -- duckdb tracks CRS in the column type itself,
        -- postgis tracks it as per-row metadata, so there's no modifier to match against there
        pluto.geom::geometry AS geom
    FROM pluto
    LEFT JOIN flags_wide ON pluto.bbl = flags_wide.bbl
)

SELECT * FROM final
ORDER BY bbl ASC
