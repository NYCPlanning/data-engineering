UPDATE pluto a
SET plutomapid = '5'
WHERE a.bbl IN (
    SELECT a.bbl FROM (
        SELECT
            a.bbl,
            st_union(b.geom) AS geom
        FROM pluto AS a, dof_shoreline_subdivide AS b
        WHERE
            a.plutomapid = '3'
            AND a.geom IS NOT NULL
            AND a.geom && st_makevalid(b.geom)
            AND st_intersects(a.geom, st_makevalid(b.geom))
        GROUP BY a.bbl
    ) AS a
    INNER JOIN pluto
        ON a.bbl = pluto.bbl
    WHERE st_within(pluto.geom, a.geom)
);
