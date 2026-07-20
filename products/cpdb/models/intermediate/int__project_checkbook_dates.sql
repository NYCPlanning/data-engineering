WITH dates AS (
    SELECT
        TRIM(LEFT(capital_project, 12)) AS maprojid,
        issue_date::text AS date,
        check_amount::double precision AS spent
    FROM {{ ref('stg__nycoc_checkbook') }}

    UNION ALL

    SELECT
        maprojid,
        TO_DATE(plancommdate, 'MM/YY')::text AS date,
        0 AS spent
    FROM {{ ref('int__ccp_commitments') }}
)

SELECT
    maprojid,
    MIN(date) AS mindate,
    MAX(date) AS maxdate,
    SUM(spent) AS spent_total_checkbooknyc
FROM dates
GROUP BY maprojid
