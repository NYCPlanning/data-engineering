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
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(p.geom, n.geom) THEN p.geom
                ELSE ST_MULTI(ST_INTERSECTION(p.geom, n.geom))
            END
        ) AS segbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, p.geom) THEN n.geom
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, p.geom))
            END
        ) AS segzonegeom,
        ST_AREA(p.geom) AS allbblgeom
    FROM dtm AS p
    INNER JOIN dcp_zoningmapindex AS n
        ON ST_INTERSECTS(p.geom, n.geom)
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
        ROW_NUMBER() OVER (
            PARTITION BY dtm_id
            ORDER BY segbblgeom DESC
        ) AS row_number
    FROM zoningmapperorder
)

SELECT * FROM ordered
