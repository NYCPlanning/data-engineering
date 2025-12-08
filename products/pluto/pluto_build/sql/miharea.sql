-- calculate how much (total area and percentage) of each lot is covered by MIH areas
-- assign the MIH project ID to each tax lot based on which MIH area covers the
-- majority of the lot
-- a MIH area is only assigned if more than 10% of the lot is covered by the MIH area
-- OR more than 50% of the MIH area overlaps with the lot
DROP TABLE IF EXISTS mihperorder;
CREATE TABLE mihperorder AS
WITH mih_unioned AS (
    SELECT CONCAT(project_name, ' - ', mih_option) as mih_area_key, ST_UNION(wkb_geometry) AS wkb_geometry
    FROM dcp_mih
    group by CONCAT(project_name, ' - ', mih_option)
),
mihper AS (
    SELECT
        p.id,
        p.bbl,
        mih_area_key,
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
    FROM pluto AS p
    INNER JOIN mih_unioned AS m
        ON ST_INTERSECTS(p.geom, m.wkb_geometry)
),
grouped AS (
    SELECT
        id,
        bbl,
        mih_area_key,
        SUM(segbblgeom) AS segbblgeom,
        SUM(segmihgeom) AS segmihgeom,
        SUM(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        MAX(segmihgeom / allmihgeom) * 100 AS maxpermihgeom
    FROM mihper
    GROUP BY id, bbl, mih_area_key
)
SELECT
    id,
    bbl,
    mih_area_key,
    segbblgeom,
    perbblgeom,
    maxpermihgeom,
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY segbblgeom DESC, segmihgeom DESC
    ) AS row_number
FROM grouped
WHERE perbblgeom >= 10 OR maxpermihgeom >= 50;

-- assign the MIH project ID with the highest overlap to each lot
UPDATE pluto a
SET miharea = mih_area_key
FROM mihperorder AS b
WHERE
    a.id = b.id
    AND row_number = 1;
