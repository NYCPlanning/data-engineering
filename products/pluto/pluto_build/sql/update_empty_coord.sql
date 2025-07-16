-- Update X/Y and Lat/Long fields that are NULL with values from Lot Centroids
-- Make sure lot centroids fall within the polygon
WITH update_table AS (
    SELECT
        bbl,
        CASE -- check whether the centroid locates in the polygon, otherwise use the PointOnSurface function
            WHEN _st_contains(geom, st_setsrid(centroid, 4326)) IS TRUE THEN centroid
            ELSE point_surface
        END AS centroid
    FROM (
        SELECT
            p.bbl,
            p.geom,
            st_centroid(p.geom) AS centroid,
            st_pointonsurface(p.geom) AS point_surface
        FROM pluto AS p
        WHERE p.geom IS NOT NULL AND p.xcoord IS NULL
    ) AS tmp
)

UPDATE pluto b
SET
    xcoord = st_x(st_transform(t.centroid, 2263)),
    ycoord = st_y(st_transform(t.centroid, 2263)),
    longitude = st_x(t.centroid),
    latitude = st_y(t.centroid),
    centroid = st_setsrid(t.centroid, 4326)
FROM update_table AS t
WHERE
    b.xcoord IS NULL AND b.bbl = t.bbl
    AND st_x(st_transform(t.centroid, 2263)) IS NOT NULL;
