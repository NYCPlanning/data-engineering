-- OMB's asset category for each project, from the latest climate budgeting release published
-- on or before the FISA extract the build's commitment plan comes from. OMB sets the category
-- per budget line, so a project's is the one with the most planned dollars.
WITH plan_extract AS (
    SELECT TO_DATE(MAX(data_library_version), 'YYYYMMDD') AS extract_date
    FROM {{ source('recipe_sources', 'fisa_capitalcommitments') }}
),

release AS (
    SELECT MAX(published_date) AS published_date
    FROM {{ ref('stg__omb_capital_climate_investments') }}
    WHERE published_date <= (SELECT extract_date FROM plan_extract)
),

lines AS (
    SELECT c.*
    FROM {{ ref('stg__omb_capital_climate_investments') }} AS c
    INNER JOIN release AS r ON c.published_date = r.published_date
),

category_totals AS (
    SELECT
        maprojid,
        asset_category,
        SUM(fiscal_year_amount) AS amount
    FROM lines
    WHERE asset_category IS NOT NULL
    GROUP BY maprojid, asset_category
),

categories AS (
    SELECT
        maprojid,
        asset_category,
        amount / NULLIF(SUM(amount) OVER (PARTITION BY maprojid), 0) AS asset_category_share,
        COUNT(*) OVER (PARTITION BY maprojid) AS asset_category_count,
        STRING_AGG(asset_category, ', ') OVER (
            PARTITION BY maprojid ORDER BY amount DESC, asset_category ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS asset_categories,
        -- ties go to the category that sorts first, so the pick is stable
        ROW_NUMBER() OVER (PARTITION BY maprojid ORDER BY amount DESC, asset_category ASC) AS rk
    FROM category_totals
),

projects AS (
    SELECT
        maprojid,
        MAX(financial_plan) AS financial_plan,
        BOOL_OR(heat_vulnerability_index) AS heat_vulnerability_index,
        BOOL_OR(flood_vulnerability_index) AS flood_vulnerability_index
    FROM lines
    GROUP BY maprojid
)

SELECT
    p.maprojid,
    p.financial_plan,
    c.asset_category,
    c.asset_category_share,
    c.asset_category_count,
    c.asset_categories,
    p.heat_vulnerability_index,
    p.flood_vulnerability_index
FROM projects AS p
LEFT JOIN categories AS c ON p.maprojid = c.maprojid AND c.rk = 1
