-- calculate how much (total area and percentage) of each lot is covered by a commercial overlay
-- assign the commercial overlay(s) to each tax lot
-- the order commercial overlays are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district

DROP TABLE IF EXISTS commoverlayperorder;
CREATE TABLE commoverlayperorder AS
WITH commoverlayper AS (
    SELECT
        p.id,
        p.bbl,
        n.overlay,
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
    FROM pluto AS p
    INNER JOIN stg__dcp_commercialoverlay AS n
        ON ST_INTERSECTS(p.geom, n.geom)
),

grouped AS (
    SELECT
        id,
        bbl,
        overlay,
        SUM(segbblgeom) AS segbblgeom,
        SUM(segzonegeom) AS segzonegeom,
        SUM(segbblgeom / allbblgeom) * 100 AS perbblgeom,
        MAX(segzonegeom / allzonegeom) * 100 AS maxperzonegeom
    FROM commoverlayper
    GROUP BY id, bbl, overlay
)

SELECT
    id,
    bbl,
    overlay,
    segbblgeom,
    perbblgeom,
    maxperzonegeom,
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY segbblgeom DESC, segzonegeom DESC
    ) AS row_number
FROM grouped
WHERE perbblgeom >= 10 OR maxperzonegeom >= 50;

UPDATE pluto a
SET overlay1 = overlay
FROM commoverlayperorder AS b
WHERE
    a.id = b.id
    AND row_number = 1;

UPDATE pluto a
SET overlay2 = overlay
FROM commoverlayperorder AS b
WHERE
    a.id = b.id
    AND row_number = 2;
