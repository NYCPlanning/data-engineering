-- calculate how much (total area and percentage) of each lot is covered by a mandatory inclusionary housing
-- assign the mandatory inclusionary housing to each tax lot
-- the order mandatory inclusionary housing districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot

DROP TABLE IF EXISTS mihperorder;
CREATE TABLE mihperorder AS
WITH mihper AS (
    SELECT
        p.id AS dtm_id,
        bbl,
        n.mih_option,
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
    INNER JOIN dcp_mih AS n
        ON st_intersects(p.geom, n.geom)
)

SELECT
    dtm_id,
    mih_option,
    segbblgeom,
    (segbblgeom / allbblgeom) * 100 AS perbblgeom,
    (segzonegeom / allzonegeom) * 100 AS perzonegeom,
    row_number()
        OVER (
            PARTITION BY dtm_id
            ORDER BY segbblgeom DESC
        )
    AS row_number
FROM mihper;

UPDATE dcp_zoning_taxlot a
SET
    mihflag = (coalesce(perbblgeom >= 10, FALSE)),
    mihoption
    = CASE
        WHEN perbblgeom >= 10 THEN mih_option
    END
FROM mihperorder AS b
WHERE a.dtm_id = b.dtm_id;
