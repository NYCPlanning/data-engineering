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
        ST_AREA(p.geom) AS allbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, p.geom) THEN n.geom
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, p.geom))
            END
        ) AS segzonegeom,
        ST_AREA(n.geom) AS allzonegeom
    FROM validdtm AS p
    INNER JOIN dcp_limitedheight AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

limitedheightperorder AS (
    SELECT
        dtm_id,
        lhlbl,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom
    FROM limitedheightper
),

flag_limited AS (
    SELECT
        dtm_id,
        lhlbl,
        (CASE
            WHEN perbblgeom >= 10 THEN lhlbl
        END) AS limitedheightdistrict
    FROM limitedheightperorder
)

SELECT * FROM flag_limited
