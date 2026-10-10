-- int__topology__ap_police, aps CTE
CREATE OR REPLACE TABLE police_aps AS
SELECT
    atomicid,
    geom,
    ST_Centroid(geom) AS centroid,
    ST_Within(ST_Centroid(geom), geom) AS centroid_inside
FROM atomicpolygons;
