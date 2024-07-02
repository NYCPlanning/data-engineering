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
        ST_AREA(p.geom) AS allbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, ST_MAKEVALID(p.geom)) THEN n.geom
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, ST_MAKEVALID(p.geom)))
            END
        ) AS segzonegeom,
        ST_AREA(n.geom) AS allzonegeom
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
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM zoningmapper
    WHERE allbblgeom > 0
),

zoningmapperorder_distinct AS (
    SELECT DISTINCT
        dtm_id,
        zoning_map,
        perbblgeom,
        row_number
    FROM zoningmapperorder
),

pivot AS (
    SELECT
        a.dtm_id,
        (CASE
            WHEN b1.perbblgeom >= 10 THEN b1.zoning_map
        END) AS zoningmapnumber,
        (CASE
            WHEN b2.row_number = 2 THEN 'Y'
        END) AS zoningmapcode
    FROM (SELECT DISTINCT dtm_id FROM zoningmapperorder_distinct) AS a
    LEFT JOIN zoningmapperorder_distinct AS b1
        ON a.dtm_id = b1.dtm_id AND b1.row_number = 1
    LEFT JOIN zoningmapperorder_distinct AS b2
        ON a.dtm_id = b2.dtm_id AND b2.row_number = 2
)

SELECT * FROM pivot
