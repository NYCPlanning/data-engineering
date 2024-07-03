WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
),

dcp_limitedheight AS (
    SELECT * FROM {{ ref('stg__dcp_limitedheight') }}
),

limitedheightper AS (
    SELECT
        p.dtm_id,
        n.lhlbl,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(p.geom, n.geom) THEN p.geom
                ELSE ST_MULTI(ST_INTERSECTION(p.geom, n.geom))
            END
        ) AS segbblgeom,
        ST_AREA(p.geom) AS allbblgeom
    FROM validdtm AS p
    INNER JOIN dcp_limitedheight AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

limitedheightperorder AS (
    SELECT
        dtm_id,
        lhlbl AS limitedheightdistrict
    FROM limitedheightper
    WHERE (segbblgeom / allbblgeom) * 100 >= 10
)

SELECT * FROM limitedheightperorder
