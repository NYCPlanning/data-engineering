{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Clean numeric fields by removing invalid values
-- lotdepth, numfloors: must be numeric (except decimal point)
-- numfloors: must be >= 1
-- lotarea: remove commas
-- sanborn: must contain at least one digit

SELECT
    bbl,
    CASE
        WHEN lotdepth ~ '[^0-9]' AND lotdepth NOT LIKE '%.%' THEN NULL
        ELSE lotdepth
    END AS lotdepth,
    CASE
        WHEN numfloors ~ '[^0-9]' AND numfloors NOT LIKE '%.%' THEN NULL
        WHEN numfloors IS NOT NULL AND numfloors::numeric < 1 THEN NULL
        ELSE numfloors
    END AS numfloors,
    CASE
        WHEN lotarea LIKE '%,%' THEN REPLACE(lotarea, ',', '')
        ELSE lotarea
    END AS lotarea,
    CASE
        WHEN sanborn !~ '[0-9]' THEN NULL
        ELSE sanborn
    END AS sanborn
FROM {{ target.schema }}.pluto
