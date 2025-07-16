{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_limitedheight AS (
    SELECT * FROM {{ ref('stg__dcp_limitedheight') }}
),

limitedheightper AS (
    SELECT
        p.dtm_id,
        p.bbl,
        n.lhlbl,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom
                ELSE ST_Multi(ST_Intersection(p.geom, n.geom))
            END
        ) AS segbblgeom,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom
                ELSE ST_Multi(ST_Intersection(n.geom, p.geom))
            END
        ) AS segzonegeom,
        ST_Area(p.geom) AS allbblgeom
    FROM dtm AS p
    INNER JOIN dcp_limitedheight AS n
        ON ST_Intersects(p.geom, n.geom)
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
