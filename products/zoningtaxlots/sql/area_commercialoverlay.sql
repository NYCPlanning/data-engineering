-- calculate how much (total area and percentage) of each lot is covered by a commercial overlay
-- assign the commercial overlay(s) to each tax lot
-- the order commercial overlays are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR more than a specified area of the lot if covered by the district

DROP TABLE IF EXISTS commoverlayperorder;
CREATE TABLE commoverlayperorder AS (
    WITH commoverlayper AS (
        SELECT
            p.id AS dtm_id,
            p.bbl,
            n.overlay,
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
        FROM dof_dtm AS p
        INNER JOIN dcp_commercialoverlay AS n
            ON st_intersects(p.geom, n.geom)
    )

    SELECT
        dtm_id,
        bbl,
        overlay,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        row_number()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC, segzonegeom DESC
            )
        AS row_number
    FROM commoverlayper
);

UPDATE dcp_zoning_taxlot a
SET commercialoverlay1 = overlay
FROM commoverlayperorder AS b
WHERE
    a.dtm_id = b.dtm_id
    AND row_number = 1
    AND (
        perbblgeom >= 10
        OR perzonegeom >= 50
    );

UPDATE dcp_zoning_taxlot a
SET commercialoverlay2 = overlay
FROM commoverlayperorder AS b
WHERE
    a.dtm_id = b.dtm_id
    AND row_number = 2
    AND (
        perbblgeom >= 10
        OR perzonegeom >= 50
    );
