-- calculate how much (total area and percentage) of each lot is covered by a zoning map
-- assign the zoning map to each tax lot
-- the order zoning maps are assigned is based on which map covers the majority of the lot
-- a map is only assigned if more than 10% of the map covers the lot
-- OR more than a specified area of the lot if covered by the map
--DROP INDEX IF EXISTS dcp_zoningmapindex_gix;
--CREATE INDEX dcp_zoningmapindex_gix ON dcp_zoningmapindex USING GIST (geom);
DROP TABLE IF EXISTS zoningmapper;
CREATE TABLE zoningmapper AS (
    WITH validdtm AS (
        SELECT
            id AS dtm_id,
            bbl,
            st_makevalid(a.geom) AS geom
        FROM dof_dtm AS a
    ),

    validindex AS (
        SELECT
            a.zoning_map,
            st_makevalid(a.geom) AS geom
        FROM dcp_zoningmapindex AS a
    )

    SELECT
        dtm_id,
        p.bbl,
        n.zoning_map,
        st_area(
            CASE
                WHEN st_coveredby(st_makevalid(p.geom), n.geom) THEN p.geom
                ELSE st_multi(st_intersection(st_makevalid(p.geom), n.geom))
            END
        ) AS segbblgeom,
        st_area(p.geom) AS allbblgeom,
        st_area(
            CASE
                WHEN st_coveredby(n.geom, st_makevalid(p.geom)) THEN n.geom
                ELSE st_multi(st_intersection(n.geom, st_makevalid(p.geom)))
            END
        ) AS segzonegeom,
        st_area(n.geom) AS allzonegeom
    FROM validdtm AS p
    INNER JOIN validindex AS n
        ON st_intersects(p.geom, n.geom)
);

DROP TABLE IF EXISTS zoningmapperorder;
CREATE TABLE zoningmapperorder AS (
    SELECT
        dtm_id,
        bbl,
        zoning_map,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        row_number()
            OVER (
                PARTITION BY dtm_id
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM zoningmapper
    WHERE allbblgeom > 0
);

UPDATE dcp_zoning_taxlot a
SET zoningmapnumber = zoning_map
FROM zoningmapperorder AS b
WHERE
    a.dtm_id = b.dtm_id
    AND row_number = 1
    AND perbblgeom >= 10;

-- set the zoningmapcode to Y where a lot is covered by a second zoning map
UPDATE dcp_zoning_taxlot a
SET zoningmapcode = 'Y'
FROM zoningmapperorder AS b
WHERE
    a.dtm_id = b.dtm_id
    AND row_number = 2;
