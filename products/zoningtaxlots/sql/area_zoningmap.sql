-- calculate how much (total area and percentage) of each lot is covered by a zoning map
-- assign the zoning map to each tax lot
-- the order zoning maps are assigned is based on which map covers the majority of the lot
-- a map is only assigned if more than 10% of the map covers the lot
-- OR more than a specified area of the lot if covered by the map
--DROP INDEX IF EXISTS dcp_zoningmapindex_gix;
--CREATE INDEX dcp_zoningmapindex_gix ON dcp_zoningmapindex USING GIST (geom);

DROP TABLE IF EXISTS zoningmapperorder;
CREATE TABLE zoningmapperorder AS (
    WITH validdtm AS (
        SELECT
            a.bbl,
            ST_MAKEVALID(a.geom) AS geom
        FROM dof_dtm AS a
    ),

    validindex AS (
        SELECT
            a.zoning_map,
            ST_MAKEVALID(a.geom) AS geom
        FROM dcp_zoningmapindex AS a
    ),

    zoningmapper AS (
        SELECT
            p.bbl,
            n.zoning_map,
            (ST_AREA(CASE
                WHEN ST_COVEREDBY(ST_MAKEVALID(p.geom), n.geom)
                    THEN p.geom
                ELSE
                    ST_MULTI(
                        ST_INTERSECTION(ST_MAKEVALID(p.geom), n.geom)
                    )
            END)) AS segbblgeom,
            ST_AREA(p.geom) AS allbblgeom,
            (ST_AREA(CASE
                WHEN ST_COVEREDBY(n.geom, ST_MAKEVALID(p.geom))
                    THEN n.geom
                ELSE
                    ST_MULTI(
                        ST_INTERSECTION(n.geom, ST_MAKEVALID(p.geom))
                    )
            END)) AS segzonegeom,
            ST_AREA(n.geom) AS allzonegeom
        FROM validdtm AS p
        INNER JOIN validindex AS n
            ON ST_INTERSECTS(p.geom, n.geom)
    )

    SELECT
        bbl,
        zoning_map,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER()
            OVER (
                PARTITION BY bbl
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
    a.bbl::TEXT = b.bbl::TEXT
    AND row_number = 1
    AND perbblgeom >= 10;

-- set the zoningmapcode to Y where a lot is covered by a second zoning map
UPDATE dcp_zoning_taxlot a
SET zoningmapcode = 'Y'
FROM zoningmapperorder AS b
WHERE
    a.bbl::TEXT = b.bbl::TEXT
    AND row_number = 2;
