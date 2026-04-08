{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Pivot MIH options into binary columns
-- A single lot can have multiple MIH options that all apply

WITH bbls_with_all_options AS (
    SELECT
        bbl,
        STRING_AGG(cleaned_option, ',') AS all_options
    FROM {{ ref('int_mih__lot_overlap') }}
    GROUP BY bbl
),

pivoted AS (
    SELECT
        bbl,
        CASE
            WHEN all_options LIKE '%Option 1%' THEN '1'
        END AS mih_opt1,
        CASE
            WHEN all_options LIKE '%Option 2%' THEN '1'
        END AS mih_opt2,
        CASE
            WHEN
                all_options LIKE '%Option 3%'
                OR all_options LIKE '%Deep Affordability Option%'
                THEN '1'
        END AS mih_opt3,
        CASE
            WHEN all_options LIKE '%Workforce Option%' THEN '1'
        END AS mih_opt4
    FROM bbls_with_all_options
)

SELECT
    bbl,
    mih_opt1,
    mih_opt2,
    mih_opt3,
    mih_opt4
FROM pivoted
