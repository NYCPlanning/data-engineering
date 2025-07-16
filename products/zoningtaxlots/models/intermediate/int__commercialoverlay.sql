{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_commercialoverlay AS (
    SELECT * FROM {{ ref('stg__dcp_commercialoverlay') }}
),

commoverlayper AS (
    SELECT
        p.dtm_id,
        p.bbl,
        n.overlay,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom
                ELSE ST_Multi(ST_Intersection(p.geom, n.geom))
            END
        ) AS segbblgeom,
        ST_Area(p.geom) AS allbblgeom,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom
                ELSE ST_Multi(ST_Intersection(n.geom, p.geom))
            END
        ) AS segzonegeom,
        ST_Area(n.geom) AS allzonegeom
    FROM dtm AS p
    INNER JOIN dcp_commercialoverlay AS n
        ON ST_Intersects(p.geom, n.geom)
),

grouped AS (
    SELECT
        dtm_id,
        bbl,
        overlay,
        sum(segbblgeom) AS segbblgeom,
        sum(segzonegeom) AS segzonegeom,
        sum(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        max(segzonegeom / allzonegeom) * 100 AS maxperzonegeom
    FROM commoverlayper
    GROUP BY dtm_id, bbl, overlay
),

filtered AS (
    SELECT
        dtm_id,
        bbl,
        overlay,
        segbblgeom,
        segzonegeom,
        perbblgeom,
        maxperzonegeom
    FROM grouped
    WHERE perbblgeom >= 10 OR maxperzonegeom >= 50
),

commoverlayperorder AS (
    SELECT
        dtm_id,
        bbl,
        segbblgeom,
        segzonegeom,
        perbblgeom,
        maxperzonegeom,
        overlay,
        row_number()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC, segzonegeom DESC
            )
        AS row_number
    FROM filtered
)

SELECT * FROM commoverlayperorder
