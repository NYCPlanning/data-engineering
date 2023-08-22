DROP TABLE IF EXISTS shoreline_bbl;
SELECT DISTINCT bbl::bigint
INTO shoreline_bbl
FROM pluto AS a, dof_shoreline_subdivide AS b
WHERE st_intersects(a.geom, b.geom);

DROP TABLE IF EXISTS pluto_geom_tmp;
SELECT
    bbl,
    st_makevalid(st_transform(a.geom, 2263)) AS geom_2263,
    st_makevalid(a.geom) AS geom_4326,
    (CASE
        WHEN
            bbl::bigint IN
            (SELECT bbl::bigint FROM shoreline_bbl)
            THEN 1
        ELSE 0
    END) AS shoreline
INTO pluto_geom_tmp
FROM pluto AS a;

DROP TABLE IF EXISTS shoreline_bbl;

DROP TABLE IF EXISTS pluto_geom;
WITH
subdivided AS (
    SELECT
        a.bbl,
        b.geom
    FROM pluto_geom_tmp AS a, dof_shoreline_subdivide AS b
    WHERE
        shoreline = 1
        AND st_intersects(a.geom_4326, b.geom)
),

subdivided_union AS (
    SELECT
        bbl::bigint,
        st_makevalid(st_union(geom)) AS geom
    FROM subdivided
    GROUP BY bbl
),

clipped AS (
    SELECT
        a.bbl::bigint,
        st_multi(st_collectionextract(st_difference(a.geom_4326, b.geom), 3)) AS geom
    FROM pluto_geom_tmp AS a, subdivided_union AS b
    WHERE a.bbl::bigint = b.bbl::bigint
)

SELECT
    a.bbl,
    a.geom_2263,
    a.geom_4326,
    coalesce(b.geom, a.geom_4326) AS clipped_4326,
    coalesce(st_transform(b.geom, 2263), a.geom_2263) AS clipped_2263
INTO pluto_geom
FROM pluto_geom_tmp AS a
LEFT JOIN clipped AS b
    ON a.bbl::bigint = b.bbl::bigint;
