{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Format condo number: remove borough code prefix and leading zeros
-- Takes rightmost 5 characters and converts to numeric then back to text to strip leading zeros

SELECT
    bbl,
    (RIGHT(condono, 5)::numeric)::text AS condono
FROM {{ target.schema }}.pluto
