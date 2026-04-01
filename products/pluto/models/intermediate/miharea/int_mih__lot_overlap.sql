{{
    config(
        materialized='table',
        tags=['pluto_enrichment']
    )
}}

-- Calculate spatial overlap between tax lots and MIH areas
-- A lot is assigned to a MIH area if:
--   1. ≥10% of the lot area is covered by the MIH area, OR
--   2. ≥50% of the MIH area is covered by the lot

WITH mih_per_area AS (
    SELECT
        p.bbl,
        m.project_id,
        m.mih_id,
        m.wkb_geometry AS mih_geom,
        p.geom AS lot_geom,
        m.cleaned_option,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(p.geom, m.wkb_geometry) THEN p.geom
                ELSE ST_MULTI(ST_INTERSECTION(p.geom, m.wkb_geometry))
            END
        ) AS segbblgeom,
        ST_AREA(p.geom) AS allbblgeom,
        ST_AREA(
            CASE
                WHEN ST_COVEREDBY(m.wkb_geometry, p.geom) THEN m.wkb_geometry
                ELSE ST_MULTI(ST_INTERSECTION(m.wkb_geometry, p.geom))
            END
        ) AS segmihgeom,
        ST_AREA(m.wkb_geometry) AS allmihgeom
    FROM {{ target.schema }}.pluto AS p
    INNER JOIN {{ ref('int_mih__cleaned') }} AS m
        ON ST_INTERSECTS(p.geom, m.wkb_geometry)
),

mih_areas AS (
    SELECT
        bbl,
        cleaned_option,
        project_id,
        mih_id,
        SUM(segbblgeom) AS segbblgeom,
        SUM(segmihgeom) AS segmihgeom,
        SUM(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        MAX(segmihgeom / allmihgeom) * 100 AS maxpermihgeom
    FROM mih_per_area
    GROUP BY bbl, cleaned_option, project_id, mih_id
)

SELECT *
FROM mih_areas
WHERE perbblgeom >= 10 OR maxpermihgeom >= 50
