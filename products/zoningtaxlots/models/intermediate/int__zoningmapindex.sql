{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['dtm_id']},
    ]
) }}

WITH dtm AS (
    SELECT * FROM {{ ref('stg__dof_dtm') }}
),

dcp_zoningmapindex AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapindex') }}
),

zoningmapper AS (
    SELECT
        dtm_id,
        p.bbl,
        n.zoning_map,
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
    INNER JOIN dcp_zoningmapindex AS n
        ON st_intersects(p.geom, n.geom)
),

zoningmapperorder AS (
    SELECT
        dtm_id,
        bbl,
        zoning_map,
        segbblgeom,
        segzonegeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom
    FROM zoningmapper
    WHERE allbblgeom > 0
),

ordered AS (
    SELECT
        dtm_id,
        bbl,
        segbblgeom,
        segzonegeom,
        zoning_map,
        perbblgeom,
        row_number()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM zoningmapperorder
)

SELECT * FROM ordered
