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

commoverlayperorder AS (
    SELECT
        dtm_id,
        bbl,
        overlay,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC, segzonegeom DESC
            )
        AS row_number
    FROM commoverlayper
),

filtered AS (
    SELECT * FROM commoverlayperorder
    WHERE perbblgeom >= 10 OR perzonegeom >= 50
),

pivot AS (
    SELECT
        a.dtm_id,
        b1.overlay AS commercialoverlay1,
        b2.overlay AS commercialoverlay2
    FROM (SELECT DISTINCT dtm_id FROM filtered) AS a
    LEFT JOIN filtered AS b1
        ON a.dtm_id = b1.dtm_id AND b1.row_number = 1
    LEFT JOIN filtered AS b2
        ON a.dtm_id = b2.dtm_id AND b2.row_number = 2
),

drop_dup AS (
    SELECT
        dtm_id,
        commercialoverlay1,
        (CASE
            WHEN commercialoverlay1 = commercialoverlay2 THEN NULL
            ELSE commercialoverlay2
        END) AS commercialoverlay2
    FROM pivot
),

corr_zoninggaps AS (
    SELECT
        dtm_id,
        (COALESCE(commercialoverlay1, commercialoverlay2)) AS commercialoverlay1,
        (CASE
            WHEN commercialoverlay1 IS NULL THEN NULL
            ELSE commercialoverlay2
        END) AS commercialoverlay2
    FROM drop_dup
)

SELECT * FROM corr_zoninggaps
