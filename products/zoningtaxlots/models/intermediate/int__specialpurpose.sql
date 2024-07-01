WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
),

dcp_specialpurpose AS (
    SELECT * FROM {{ ref('stg__dcp_specialpurpose') }}
),

specialdistrict_priority AS (
    SELECT * FROM {{ ref('specialdistrict_priority') }}
),

specialpurposeper AS (
    SELECT
        p.dtm_id,
        p.bbl,
        n.sdlbl,
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
    INNER JOIN dcp_specialpurpose AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

specialpurposeperorder_init AS (
    SELECT
        dtm_id,
        bbl,
        sdlbl,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        -- per sp district type, rank by 
        --   1) if lot meets 10% coverage by sp district threshold
        --   2) area of coverage
        -- This is to get cases where special sp district may only be assigned to one lot. 
        -- 1) is to cover edge case where largest section of specific large (but common) sp district that happens to have largest area on small fraction of huge lot
        -- See BBL 1013730001 in ZoLa - largest section of special district but should not be assigned spdist SRI
        ROW_NUMBER()
            OVER (PARTITION BY sdlbl ORDER BY (segbblgeom / allbblgeom) * 100 < 10, segzonegeom DESC)
        AS sd_row_number
    FROM specialpurposeper
),

specialpurposeperorder AS (
    SELECT
        dtm_id,
        bbl,
        sdlbl,
        segbblgeom,
        ROW_NUMBER() OVER (PARTITION BY dtm_id ORDER BY segbblgeom ASC, sdlbl DESC) AS row_number
    FROM specialpurposeperorder_init
    WHERE
        perbblgeom >= 10
        OR sd_row_number = 1
),

pivot AS (
    SELECT
        a.dtm_id,
        a.bbl,
        b1.sdlbl AS specialdistrict1,
        b2.sdlbl AS specialdistrict2,
        b3.sdlbl AS specialdistrict3
    FROM (SELECT DISTINCT
        dtm_id,
        bbl
    FROM specialpurposeperorder) AS a
    LEFT JOIN specialpurposeperorder AS b1
        ON a.dtm_id = b1.dtm_id AND b1.row_number = 1
    LEFT JOIN specialpurposeperorder AS b2
        ON a.dtm_id = b2.dtm_id AND b2.row_number = 2
    LEFT JOIN specialpurposeperorder AS b3
        ON a.dtm_id = b3.dtm_id AND b3.row_number = 3
),

set_sd_order AS (
    SELECT
        a.dtm_id,
        a.bbl,
        (COALESCE(b.sdlabel1, a.specialdistrict1)) AS specialdistrict1,
        (COALESCE(b.sdlabel2, a.specialdistrict2)) AS specialdistrict2,
        a.specialdistrict3
    FROM pivot AS a
    LEFT JOIN specialdistrict_priority AS b
        ON
            (a.specialdistrict1 = b.sdlabel1 AND a.specialdistrict2 = b.sdlabel2)
            OR (a.specialdistrict1 = b.sdlabel2 AND a.specialdistrict2 = b.sdlabel1)
)

SELECT * FROM set_sd_order
