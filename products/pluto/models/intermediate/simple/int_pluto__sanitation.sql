{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Extract sanitation borough and district from sanitdistrict field
-- sanitboro: first character of sanitdistrict
-- sanitdistrict: last 2 characters of sanitdistrict

SELECT
    bbl,
    CASE
        WHEN sanitdistrict IS NOT NULL THEN LEFT(sanitdistrict, 1)
    END AS sanitboro,
    CASE
        WHEN sanitdistrict IS NOT NULL THEN RIGHT(sanitdistrict, 2)
    END AS sanitdistrict
FROM {{ target.schema }}.pluto
