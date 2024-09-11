{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
),

dcp_limitedheight AS (
    SELECT * FROM {{ ref('stg__dcp_limitedheight') }}
),

limitedheightper AS (
    SELECT
        p.dtm_id,
        p.bbl,
        n.lhlbl,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(p.geom, n.geom) THEN p.geom
                ELSE ST_MULTI(ST_INTERSECTION(p.geom, n.geom))
            END
        ) AS segbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, p.geom) THEN n.geom
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, p.geom))
            END
        ) AS segzonegeom,
        ST_AREA(p.geom) AS allbblgeom
    FROM validdtm AS p
    INNER JOIN dcp_limitedheight AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

limitedheightperorder AS (
    SELECT
        dtm_id,
        bbl,
        segbblgeom,
        segzonegeom,
        lhlbl AS limitedheightdistrict
    FROM limitedheightper
    WHERE (segbblgeom / allbblgeom) * 100 >= 10
)

SELECT * FROM limitedheightperorder
