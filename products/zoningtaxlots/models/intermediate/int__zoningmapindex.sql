{{ config(
    materialized = 'table'
) }}

WITH validdtm AS (
    SELECT * FROM {{ ref('int__validdtm') }}
),

dcp_zoningmapindex AS (
    SELECT * FROM {{ ref('stg__dcp_zoningmapindex') }}
),

validindex AS (
    SELECT
        a.zoning_map,
        ST_MAKEVALID(a.geom) AS geom
    FROM dcp_zoningmapindex AS a
),

zoningmapper AS (
    SELECT
        dtm_id,
        p.bbl,
        n.zoning_map,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(ST_MAKEVALID(p.geom), n.geom) THEN p.geom
                ELSE ST_MULTI(ST_INTERSECTION(ST_MAKEVALID(p.geom), n.geom))
            END
        ) AS segbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, ST_MAKEVALID(p.geom)) THEN n.geom
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, ST_MAKEVALID(p.geom)))
            END
        ) AS segzonegeom,
        ST_AREA(p.geom) AS allbblgeom
    FROM validdtm AS p
    INNER JOIN validindex AS n
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
        ROW_NUMBER()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM zoningmapperorder
)

SELECT * FROM ordered
