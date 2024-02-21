{% set sql_statement %} -- noqa: disable=all
    SELECT * FROM {{ ref('ceqr_variables') }}
{% endset %}

{% if execute %}
    {% set ceqr_variables = run_query(sql_statement) %}
{% endif %}

WITH pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

ceqr_flags_long AS (
    SELECT * FROM {{ ref('ceqr_flags_long') }}
),

ceqr_flags_ranked AS (
    SELECT
        bbl,
        variable_type,
        variable_id,
        ROW_NUMBER()
            OVER (PARTITION BY bbl, variable_type ORDER BY distance_from)
        AS row_number
    FROM ceqr_flags_long
),

ceqr_flags_wide AS (
    SELECT
        {% for row in ceqr_variables -%}
            MAX(
                CASE
                    WHEN
                        variable_type = '{{ row["variable_type"] }}'
                        THEN variable_id
                END
            ) AS "{{ row['label'] }}",
        {% endfor %}
        bbl::text -- TODO remove when not pulling long flags from seed table
    FROM ceqr_flags_ranked
    WHERE row_number = 1
    GROUP BY bbl
)

SELECT
    pluto.bbl,
    {% for row in ceqr_variables -%}
        f."{{ row['label'] }}",
    {% endfor %}
    pluto.geom
FROM
    pluto
LEFT JOIN ceqr_flags_wide AS f ON pluto.bbl = f.bbl
