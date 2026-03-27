{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Calculate Floor Area Ratio (FAR) metrics for each tax lot
-- 1. Built FAR: ratio of building area to lot area
-- 2. Max FAR values by zoning district: residential, commercial, facility, affordable residential, manufacturing

WITH base_pluto AS (
    SELECT
        bbl,
        zonedist1,
        bldgarea,
        lotarea
    FROM {{ target.schema }}.pluto
),

far_calculated AS (
    SELECT
        bbl,
        zonedist1,
        -- Calculate built FAR (building area / lot area)
        -- Only calculate when lotarea is non-zero and non-null
        CASE
            WHEN lotarea IS NOT NULL AND lotarea != '0'
                THEN ROUND((bldgarea::numeric / lotarea::numeric), 2)
        END AS builtfar
    FROM base_pluto
),

max_far_lookup AS (
    SELECT
        f.bbl,
        f.builtfar,
        COALESCE(z.residfar::double precision, 0::double precision) AS residfar,
        COALESCE(z.commfar::double precision, 0::double precision) AS commfar,
        COALESCE(z.facilfar::double precision, 0::double precision) AS facilfar,
        COALESCE(z.affresfar::double precision, 0::double precision) AS affresfar,
        COALESCE(z.mnffar::double precision, 0::double precision) AS mnffar
    FROM far_calculated AS f
    LEFT JOIN {{ ref('dcp_zoning_maxfar') }} AS z
        ON f.zonedist1 = z.zonedist
)

SELECT
    bbl,
    builtfar,
    residfar,
    commfar,
    facilfar,
    affresfar,
    mnffar
FROM max_far_lookup
