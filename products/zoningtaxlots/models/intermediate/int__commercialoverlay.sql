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
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(p.geom, n.geom) THEN p.geom
                ELSE ST_MULTI(ST_INTERSECTION(p.geom, n.geom))
            END
        ) AS segbblgeom,
        ST_AREA(p.geom) AS allbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, p.geom) THEN n.geom
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, p.geom))
            END
        ) AS segzonegeom,
        ST_AREA(n.geom) AS allzonegeom
    FROM dtm AS p
    INNER JOIN dcp_commercialoverlay AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

grouped AS (
    SELECT
        dtm_id,
        bbl,
        overlay,
        SUM(segbblgeom) AS segbblgeom,
        SUM(segzonegeom) AS segzonegeom,
        SUM(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        MAX(segzonegeom / allzonegeom) * 100 AS maxperzonegeom
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
        ROW_NUMBER() OVER (
            PARTITION BY dtm_id
            ORDER BY segbblgeom DESC, segzonegeom DESC
        ) AS row_number
    FROM filtered
)

SELECT * FROM commoverlayperorder
