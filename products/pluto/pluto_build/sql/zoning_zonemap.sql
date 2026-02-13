-- calculate how much (total area and percentage) of each lot is covered by a zoning map
-- assign the zoning map to each tax lot
-- the order zoning maps are assigned is based on which map covers the majority of the lot
-- a map is only assigned if more than 10% of the map covers the lot
-- OR more than a specified area of the lot if covered by the map
DROP TABLE IF EXISTS zoningmapperorder;
CREATE TABLE zoningmapperorder AS (
    WITH
    zoningmapper AS (
        SELECT
            p.id,
            p.bbl,
            n.zoning_map,
            ST_AREA(
                CASE
                    WHEN ST_COVEREDBY(ST_MAKEVALID(p.geom), n.geom) THEN p.geom
                    ELSE ST_MULTI(ST_INTERSECTION(ST_MAKEVALID(p.geom), n.geom))
                END
            ) AS segbblgeom,
            ST_AREA(p.geom) AS allbblgeom,
            ST_AREA(
                CASE
                    WHEN ST_COVEREDBY(n.geom, ST_MAKEVALID(p.geom)) THEN n.geom
                    ELSE ST_MULTI(ST_INTERSECTION(n.geom, ST_MAKEVALID(p.geom)))
                END
            ) AS segzonegeom,
            ST_AREA(n.geom) AS allzonegeom
        FROM pluto AS p
        INNER JOIN stg__dcp_zoningmapindex AS n
            ON ST_INTERSECTS(p.geom, n.geom)
    )

    SELECT
        id,
        bbl,
        zoning_map,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY segbblgeom DESC
        ) AS row_number
    FROM zoningmapper
);

UPDATE pluto a
SET zonemap = LOWER(zoning_map)
FROM zoningmapperorder AS b
WHERE
    a.id = b.id
    AND row_number = 1
    AND perbblgeom >= 10;

-- set the zoningmapcode to Y where a lot is covered by a second zoning map
UPDATE pluto a
SET zmcode = 'Y'
FROM zoningmapperorder AS b
WHERE
    a.id = b.id
    AND row_number = 2;

DROP TABLE zoningmapperorder;
