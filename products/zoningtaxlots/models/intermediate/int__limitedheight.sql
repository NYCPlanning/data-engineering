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
        st_area(
            CASE
                WHEN st_coveredby(p.geom, n.geom) THEN p.geom
                ELSE st_multi(st_intersection(p.geom, n.geom))
            END
        ) AS segbblgeom,
        st_area(
            CASE
                WHEN st_coveredby(n.geom, p.geom) THEN n.geom
                ELSE st_multi(st_intersection(n.geom, p.geom))
            END
        ) AS segzonegeom,
        st_area(p.geom) AS allbblgeom
    FROM dtm AS p
    INNER JOIN dcp_limitedheight AS n
        ON st_intersects(p.geom, n.geom)
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
