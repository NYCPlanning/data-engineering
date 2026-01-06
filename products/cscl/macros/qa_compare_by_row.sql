{% macro qa_compare_by_row(model) -%}
WITH dev AS (
    SELECT * FROM {{ ref(model) }}
),
prod AS (
    SELECT * FROM {{ source('production_outputs', model) }}
),
combined AS (
    SELECT
        'dev' AS source,
        *,
        md5(cast(dev AS TEXT)) AS row_hash
    FROM dev
    UNION ALL
    SELECT
        'prod' AS source,
        *,
        md5(cast(prod AS TEXT)) AS row_hash
    FROM prod
),
counts AS (
    SELECT
        *,
        count(*) OVER (PARTITION BY row_hash) AS match_count,
        count(CASE WHEN source = 'dev' THEN 1 END) OVER (PARTITION BY row_hash) AS dev_count,
        count(CASE WHEN source = 'prod' THEN 1 END) OVER (PARTITION BY row_hash) AS prod_count
    FROM combined
)
SELECT * FROM counts
WHERE dev_count <> prod_count
{%- endmacro %}
