-- NTAs that touch, even at a corner, and NTAs across less than a quarter mile of water. NTAs are clipped to the
-- shoreline and cover all the land, parks and rail yards included, so two NTAs whose shortest connecting line
-- crosses no other NTA are across water. A quarter mile reaches over the Harlem River and most of the East River
-- without reaching across the harbor. EPSG:2263 is in feet. always_xy because the source is lon/lat.
{%- set water_feet = 1320 %}
WITH ntas AS (
    SELECT
        nta2020,
        geom,
        ST_Transform(geom, 'EPSG:4326', 'EPSG:2263', true) AS geom_feet
    FROM {{ ref('int_nta_spine') }}
),

-- bounding boxes, to skip exact checks on pairs that can't be close
boxes AS (
    SELECT
        *,
        ST_XMin(geom_feet) AS x0,
        ST_XMax(geom_feet) AS x1,
        ST_YMin(geom_feet) AS y0,
        ST_YMax(geom_feet) AS y1
    FROM ntas
),

touching AS (
    SELECT
        nta.nta2020,
        neighbor.nta2020 AS neighbor_nta2020,
        ST_Length(
            ST_Transform(ST_Intersection(nta.geom, neighbor.geom), 'EPSG:4326', 'EPSG:2263', true)
        ) AS shared_boundary_feet
    FROM ntas AS nta
    INNER JOIN ntas AS neighbor
        ON nta.nta2020 != neighbor.nta2020 AND ST_Intersects(nta.geom, neighbor.geom)
),

near AS (
    SELECT
        nta.nta2020,
        neighbor.nta2020 AS neighbor_nta2020,
        ST_ShortestLine(nta.geom_feet, neighbor.geom_feet) AS gap
    FROM boxes AS nta
    INNER JOIN boxes AS neighbor
        ON
            nta.nta2020 != neighbor.nta2020
            AND neighbor.x0 <= nta.x1 + {{ water_feet }}
            AND neighbor.x1 >= nta.x0 - {{ water_feet }}
            AND neighbor.y0 <= nta.y1 + {{ water_feet }}
            AND neighbor.y1 >= nta.y0 - {{ water_feet }}
    WHERE
        ST_Distance(nta.geom_feet, neighbor.geom_feet) <= {{ water_feet }}
        AND NOT ST_Intersects(nta.geom, neighbor.geom)
),

across_water AS (
    SELECT
        near.nta2020,
        near.neighbor_nta2020,
        ST_Length(near.gap) AS gap_feet
    FROM near
    -- more than a foot inside another NTA, so a line ending where a third NTA meets the shore still counts
    WHERE NOT EXISTS (
        SELECT 1
        FROM boxes AS other
        WHERE
            other.nta2020 NOT IN (near.nta2020, near.neighbor_nta2020)
            AND other.x0 <= ST_XMax(near.gap)
            AND other.x1 >= ST_XMin(near.gap)
            AND other.y0 <= ST_YMax(near.gap)
            AND other.y1 >= ST_YMin(near.gap)
            AND ST_Intersects(other.geom_feet, near.gap)
            AND ST_Length(ST_Intersection(other.geom_feet, near.gap)) > 1
    )
)

SELECT
    nta2020,
    neighbor_nta2020,
    'touching' AS neighbor_type,
    shared_boundary_feet,
    0.0 AS gap_feet
FROM touching
UNION ALL
SELECT
    nta2020,
    neighbor_nta2020,
    'across water' AS neighbor_type,
    0.0 AS shared_boundary_feet,
    gap_feet
FROM across_water
