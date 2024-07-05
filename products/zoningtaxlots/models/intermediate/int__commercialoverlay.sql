WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
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
    FROM validdtm AS p
    INNER JOIN dcp_commercialoverlay AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

filtered AS (
    SELECT
        dtm_id,
        bbl,
        overlay,
        segbblgeom,
        segzonegeom
    FROM commoverlayper
    WHERE (segbblgeom / allbblgeom) * 100 >= 10 OR (segzonegeom / allzonegeom) * 100 >= 50
),

grouped AS (
    SELECT
        dtm_id,
        bbl,
        overlay,
        SUM(segbblgeom) AS segbblgeom,
        SUM(segzonegeom) AS segzonegeom
    FROM filtered
    GROUP BY dtm_id, bbl, overlay
),

commoverlayperorder AS (
    SELECT
        dtm_id,
        bbl,
        segbblgeom,
        segzonegeom,
        overlay,
        ROW_NUMBER()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC, segzonegeom DESC
            )
        AS row_number
    FROM grouped
)


SELECT * FROM commoverlayperorder
