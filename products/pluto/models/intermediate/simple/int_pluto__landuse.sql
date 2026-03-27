{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Determine land use based on building class and calculate area source for vacant lots
-- Uses pluto_input_landuse_bldgclass lookup table
-- Sets areasource to '4' for vacant lots (landuse=11, no buildings, no building area)

WITH base_pluto AS (
    SELECT
        bbl,
        bldgclass,
        areasource,
        numbldgs,
        bldgarea
    FROM {{ target.schema }}.pluto
),

landuse_lookup AS (
    SELECT
        p.bbl,
        lu.landuse,
        p.areasource,
        p.numbldgs,
        p.bldgarea
    FROM base_pluto AS p
    LEFT JOIN {{ ref('pluto_input_landuse_bldgclass') }} AS lu
        ON p.bldgclass = lu.bldgclass
),

areasource_calculated AS (
    SELECT
        bbl,
        landuse,
        CASE
            WHEN
                (areasource IS NULL OR areasource = '0')
                AND landuse = '11'
                AND numbldgs::numeric = 0
                AND (bldgarea::numeric = 0 OR bldgarea IS NULL)
                THEN '4'
            ELSE areasource
        END AS areasource
    FROM landuse_lookup
)

SELECT
    bbl,
    landuse,
    areasource
FROM areasource_calculated
