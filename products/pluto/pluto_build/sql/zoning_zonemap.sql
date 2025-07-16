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
            ST_Area(
                CASE
                    WHEN ST_CoveredBy(ST_MakeValid(p.geom), n.geom) THEN p.geom
                    ELSE ST_Multi(ST_Intersection(ST_MakeValid(p.geom), n.geom))
                END
            ) AS segbblgeom,
            ST_Area(p.geom) AS allbblgeom,
            ST_Area(
                CASE
                    WHEN ST_CoveredBy(n.geom, ST_MakeValid(p.geom)) THEN n.geom
                    ELSE ST_Multi(ST_Intersection(n.geom, ST_MakeValid(p.geom)))
                END
            ) AS segzonegeom,
            ST_Area(n.geom) AS allzonegeom
        FROM pluto AS p
        INNER JOIN dcp_zoningmapindex AS n
            ON ST_Intersects(p.geom, n.geom)
    )

    SELECT
        id,
        bbl,
        zoning_map,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        row_number()
            OVER (
                PARTITION BY id
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM zoningmapper
);

UPDATE pluto a
SET zonemap = lower(zoning_map)
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
