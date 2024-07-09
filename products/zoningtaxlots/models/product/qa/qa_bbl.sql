{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'previous_ztl') }}
),

bbl_comp AS (
    SELECT
        a.bbl AS bblnew,
        b.bbl AS bblold
    FROM new_version AS a
    FULL OUTER JOIN prev_version AS b
        ON a.bbl = b.bbl
),

bbl AS (
    SELECT
        sum(CASE WHEN bblnew IS null THEN 1 ELSE 0 END) AS removed,
        sum(CASE WHEN bblold IS null THEN 1 ELSE 0 END) AS added,
        'VERSION' AS version,
        'VERSION_PREV' AS version_prev
    FROM bbl_comp
)

SELECT * FROM bbl
