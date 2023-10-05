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
    FROM dof_dtm AS p
    INNER JOIN dcp_mih AS n
        ON ST_INTERSECTS(p.geom, n.geom)
)

SELECT
    dtm_id,
    mih_option,
    segbblgeom,
    (segbblgeom / allbblgeom) * 100 AS perbblgeom,
    (segzonegeom / allzonegeom) * 100 AS perzonegeom,
    ROW_NUMBER()
        OVER (
            PARTITION BY dtm_id
            ORDER BY segbblgeom DESC
        )
    AS row_number
FROM mihper;

UPDATE dcp_zoning_taxlot a
SET
    mihflag = (COALESCE(perbblgeom >= 10, FALSE)),
    mihoption
    = CASE
        WHEN perbblgeom >= 10 THEN mih_option
    END
FROM mihperorder AS b
WHERE a.dtm_id = b.dtm_id;
