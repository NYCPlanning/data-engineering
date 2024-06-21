WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
),

dcp_specialpurpose AS (
    SELECT * FROM {{ ref('stg__dcp_specialpurpose') }}
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
    )

SELECT * FROM specialpurposeperorder
