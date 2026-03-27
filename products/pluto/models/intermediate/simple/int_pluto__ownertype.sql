{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Determine owner type based on COLP (City Owned and Leased Properties) data
-- Sets ownertype to 'X' for properties where total exemption equals total assessment (fully exempt)

WITH base_pluto AS (
    SELECT
        bbl,
        exempttot,
        assesstot
    FROM {{ target.schema }}.pluto
),

colp_lookup AS (
    SELECT
        p.bbl,
        c.ownership AS ownertype,
        p.exempttot,
        p.assesstot
    FROM base_pluto AS p
    LEFT JOIN (
        SELECT DISTINCT ON (bbl)
            bbl,
            ownership
        FROM {{ ref('stg__dcp_colp') }}
        ORDER BY bbl
    ) AS c
        ON p.bbl::numeric = c.bbl::numeric
),

ownertype_calculated AS (
    SELECT
        bbl,
        CASE
            WHEN
                ownertype IS NULL
                AND exempttot = assesstot
                THEN 'X'
            ELSE ownertype
        END AS ownertype
    FROM colp_lookup
)

SELECT
    bbl,
    ownertype
FROM ownertype_calculated
