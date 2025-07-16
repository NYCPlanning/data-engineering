UPDATE pluto a
SET plutomapid = '4'
WHERE a.bbl IN (
    SELECT a.bbl FROM (
        SELECT
            a.bbl,
            ST_Union(b.geom) AS geom
        FROM pluto AS a, dof_shoreline_subdivide AS b
        WHERE
            a.plutomapid = '1'
            AND a.geom IS NOT NULL
            AND a.geom && ST_MakeValid(b.geom)
            AND ST_Intersects(a.geom, ST_MakeValid(b.geom))
        GROUP BY a.bbl
    ) AS a
    INNER JOIN pluto
        ON a.bbl = pluto.bbl
    WHERE ST_Within(pluto.geom, a.geom)
);
