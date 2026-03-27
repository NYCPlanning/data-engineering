{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Transform irregular lot code from RPAD format (I/R) to Y/N format
-- I (Irregular) -> Y
-- R or anything else -> N

SELECT
    bbl,
    CASE
        WHEN irrlotcode = 'I' THEN 'Y'
        ELSE 'N'
    END AS irrlotcode
FROM {{ target.schema }}.pluto
