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
        FROM pluto AS p
        INNER JOIN dcp_limitedheight AS n
            ON st_intersects(p.geom, n.geom)
    )

    SELECT
        id,
        bbl,
        lhlbl,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        row_number()
            OVER (
                PARTITION BY id
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM limitedheightper
);

UPDATE pluto a
SET ltdheight = lhlbl
FROM limitedheightperorder AS b
WHERE
    a.id = b.id
    AND perbblgeom >= 10;

DROP TABLE IF EXISTS limitedheightperorder;
