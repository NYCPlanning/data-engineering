-- calculate how much (total area and percentage) of each lot is covered by a limited height district
-- assign the limited height district to each tax lot
-- the order limited height districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district

DROP TABLE IF EXISTS limitedheightperorder;
CREATE TABLE limitedheightperorder AS (
    WITH
    limitedheightper AS (
        SELECT
            p.id,
            p.bbl,
            n.lhlbl,
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
        INNER JOIN stg__dcp_limitedheight AS n
            ON ST_INTERSECTS(p.geom, n.geom)
    )

    SELECT
        id,
        bbl,
        lhlbl,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY segbblgeom DESC
        ) AS row_number
    FROM limitedheightper
);

UPDATE pluto a
SET ltdheight = lhlbl
FROM limitedheightperorder AS b
WHERE
    a.id = b.id
    AND perbblgeom >= 10;

DROP TABLE IF EXISTS limitedheightperorder;
