-- Update X/Y and Lat/Long fields that are NULL with values from Lot Centroids
-- Make sure lot centroids fall within the polygon
WITH update_table AS (
    SELECT
        bbl,
        CASE -- check whether the centroid locates in the polygon, otherwise use the PointOnSurface function
            WHEN _ST_Contains(geom, ST_SetSRID(centroid, 4326)) IS TRUE THEN centroid
            ELSE point_surface
        END AS centroid
    FROM (
        SELECT
            p.bbl,
            p.geom,
            ST_Centroid(p.geom) AS centroid,
            ST_Pointonsurface(p.geom) AS point_surface
        FROM pluto AS p
        WHERE p.geom IS NOT NULL AND p.xcoord IS NULL
    ) AS tmp
)

UPDATE pluto b
SET
    xcoord = ST_X(ST_Transform(t.centroid, 2263)),
    ycoord = ST_Y(ST_Transform(t.centroid, 2263)),
    longitude = ST_X(t.centroid),
    latitude = ST_Y(t.centroid),
    centroid = ST_SetSRID(t.centroid, 4326)
FROM update_table AS t
WHERE
    b.xcoord IS NULL AND b.bbl = t.bbl
    AND ST_X(ST_Transform(t.centroid, 2263)) IS NOT NULL;
