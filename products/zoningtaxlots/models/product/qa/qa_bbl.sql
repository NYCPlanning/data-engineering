{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningtaxlots') }}
),

bbl AS (
    SELECT
        sum(CASE WHEN bblnew IS null THEN 1 ELSE 0 END) AS removed,
        sum(CASE WHEN bblold IS null THEN 1 ELSE 0 END) AS added,
        'new_version' AS version,
        'prev_version' AS version_prev
    FROM (
        SELECT
            a.bbl AS bblnew,
            b.bbl AS bblold
        FROM new_version AS a
        FULL OUTER JOIN prev_version AS b
            ON a.bbl = b.bbl
    ) AS c
)

SELECT * FROM bbl
