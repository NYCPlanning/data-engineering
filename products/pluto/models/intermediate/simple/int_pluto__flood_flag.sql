{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Flag tax lots that fall within FEMA 1% annual chance floodplains
-- firm07_flag: 2007 floodplain
-- pfirm15_flag: 2015 preliminary floodplain
-- Only returns BBLs with at least one flag (not all BBLs)

WITH firm07_subdivided AS (
    SELECT
        ST_SUBDIVIDE(ST_MAKEVALID(geom)) AS geom
    FROM {{ ref('stg__fema_firms2007_100yr') }}
    WHERE
        fld_zone != 'X'
        AND fld_zone != '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
),

firm07_bbls AS (
    SELECT DISTINCT p.bbl
    FROM {{ target.schema }}.pluto AS p
    INNER JOIN firm07_subdivided AS f
        ON p.geom && f.geom AND ST_INTERSECTS(p.geom, f.geom)
),

pfirm15_subdivided AS (
    SELECT
        ST_SUBDIVIDE(ST_MAKEVALID(geom)) AS geom
    FROM {{ ref('stg__fema_pfirms2015_100yr') }}
    WHERE
        fld_zone != 'X'
        AND fld_zone != '0.2 PCT ANNUAL CHANCE FLOOD HAZARD'
),

pfirm15_bbls AS (
    SELECT DISTINCT p.bbl
    FROM {{ target.schema }}.pluto AS p
    INNER JOIN pfirm15_subdivided AS f
        ON p.geom && f.geom AND ST_INTERSECTS(p.geom, f.geom)
),

all_flagged_bbls AS (
    SELECT bbl FROM firm07_bbls
    UNION
    SELECT bbl FROM pfirm15_bbls
)

SELECT
    afb.bbl,
    CASE WHEN f07.bbl IS NOT NULL THEN '1' END AS firm07_flag,
    CASE WHEN f15.bbl IS NOT NULL THEN '1' END AS pfirm15_flag
FROM all_flagged_bbls AS afb
LEFT JOIN firm07_bbls AS f07 ON afb.bbl = f07.bbl
LEFT JOIN pfirm15_bbls AS f15 ON afb.bbl = f15.bbl

