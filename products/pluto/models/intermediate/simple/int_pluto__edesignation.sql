{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Set E-designation number for each tax lot
-- When multiple E-designations exist for one lot, take the one with lowest ceqr_num and ulurp_num

WITH edesignation AS (
    SELECT
        bbl,
        enumber
    FROM (
        SELECT
            bbl,
            enumber,
            ROW_NUMBER() OVER (
                PARTITION BY bbl
                ORDER BY ceqr_num, ulurp_num, enumber
            ) AS row_number
        FROM {{ ref('stg__dcp_edesignation') }}
    ) AS x
    WHERE x.row_number = 1
)

SELECT
    bbl,
    enumber AS edesignum
FROM edesignation
