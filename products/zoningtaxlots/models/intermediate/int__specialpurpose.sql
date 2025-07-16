{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
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
        st_area(
            CASE
                WHEN st_coveredby(p.geom, n.geom) THEN p.geom
                ELSE st_multi(st_intersection(p.geom, n.geom))
            END
        ) AS segbblgeom,
        st_area(p.geom) AS allbblgeom,
        st_area(
            CASE
                WHEN st_coveredby(n.geom, p.geom) THEN n.geom
                ELSE st_multi(st_intersection(n.geom, p.geom))
            END
        ) AS segzonegeom,
        st_area(n.geom) AS allzonegeom
    FROM dtm AS p
    INNER JOIN dcp_specialpurpose AS n
        ON st_intersects(p.geom, n.geom)
),

specialpurposeperorder_init AS (
    SELECT
        dtm_id,
        bbl,
        sdlbl,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        segbblgeom,
        segzonegeom,
        -- per sp district type, rank by 
        --   1) if lot meets 10% coverage by sp district threshold
        --   2) area of coverage
        -- This is to get cases where special sp district may only be assigned to one lot. 
        -- 1) is to cover edge case where largest section of specific large (but common) sp district that happens to have largest area on small fraction of huge lot
        -- See BBL 1013730001 in ZoLa - largest section of special district but should not be assigned spdist SRI
        row_number() OVER (
            PARTITION BY sdlbl
            ORDER BY (segbblgeom / allbblgeom) * 100 < 10, segzonegeom DESC
        ) AS sd_row_number
    FROM specialpurposeper
),

specialpurposeperorder AS (
    SELECT
        a.dtm_id,
        a.bbl,
        a.sdlbl,
        a.segbblgeom,
        a.segzonegeom,
        b.priority,
        row_number() OVER (
            PARTITION BY a.dtm_id
            ORDER BY a.segbblgeom DESC, b.priority ASC, a.sdlbl ASC
        ) AS row_number
    FROM specialpurposeperorder_init AS a
    LEFT JOIN specialdistrict_priority AS b
        ON a.sdlbl = b.sdlbl
    WHERE
        a.perbblgeom >= 10
        OR a.sd_row_number = 1
)

SELECT * FROM specialpurposeperorder
