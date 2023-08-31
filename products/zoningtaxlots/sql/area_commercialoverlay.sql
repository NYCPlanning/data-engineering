-- calculate how much (total area and percentage) of each lot is covered by a commercial overlay
-- assign the commercial overlay(s) to each tax lot
-- the order commercial overlays are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district

DROP TABLE IF EXISTS commoverlayperorder;
CREATE TABLE commoverlayperorder AS (
    WITH
    commoverlayper AS (
        SELECT
            p.bbl,
            n.overlay,
            (ST_AREA(CASE
                WHEN ST_COVEREDBY(p.geom, n.geom)
                    THEN p.geom
                ELSE
                    ST_MULTI(
                        ST_INTERSECTION(p.geom, n.geom)
                    )
            END)) AS segbblgeom,
            ST_AREA(p.geom) AS allbblgeom,
            (ST_AREA(CASE
                WHEN ST_COVEREDBY(n.geom, p.geom)
                    THEN n.geom
                ELSE
                    ST_MULTI(
                        ST_INTERSECTION(n.geom, p.geom)
                    )
            END)) AS segzonegeom,
            ST_AREA(n.geom) AS allzonegeom
        FROM dof_dtm AS p
        INNER JOIN dcp_commercialoverlay AS n
            ON ST_INTERSECTS(p.geom, n.geom)
    )

    SELECT
        bbl,
        overlay,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER()
            OVER (
                PARTITION BY bbl
                ORDER BY segbblgeom DESC, segzonegeom DESC
            )
        AS row_number
    FROM commoverlayper
);

UPDATE dcp_zoning_taxlot a
SET commercialoverlay1 = overlay
FROM commoverlayperorder AS b
WHERE
    a.bbl::TEXT = b.bbl::TEXT
    AND row_number = 1
    AND (
        perbblgeom >= 10
        OR perzonegeom >= 50
    );

UPDATE dcp_zoning_taxlot a
SET commercialoverlay2 = overlay
FROM commoverlayperorder AS b
WHERE
    a.bbl::TEXT = b.bbl::TEXT
    AND row_number = 2
    AND (
        perbblgeom >= 10
        OR perzonegeom >= 50
    );

--\copy (SELECT * FROM commoverlayperorder ORDER BY bbl) TO '/prod/db-zoningtaxlots/zoningtaxlots_build/output/intermediate_commoverlayperorder.csv' DELIMITER ',' CSV HEADER;
--
--DROP TABLE commoverlayperorder;
