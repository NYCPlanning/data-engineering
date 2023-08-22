-- spatial join to get bbls

WITH bx AS (
    SELECT
        bbl,
        ST_TRANSFORM(ST_SETSRID(geom, 2263), 4326) AS geom
    FROM bxmappluto
)

UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM bx AS b
WHERE
    a.bbl IS NULL
    AND ST_WITHIN(a.geom, b.geom);

WITH bk AS (
    SELECT
        bbl,
        ST_TRANSFORM(ST_SETSRID(geom, 2263), 4326) AS geom
    FROM bkmappluto
)

UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM bk AS b
WHERE
    a.bbl IS NULL
    AND ST_WITHIN(a.geom, b.geom);

WITH mn AS (
    SELECT
        bbl,
        ST_TRANSFORM(ST_SETSRID(geom, 2263), 4326) AS geom
    FROM mnmappluto
)

UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM mn AS b
WHERE
    a.bbl IS NULL
    AND ST_WITHIN(a.geom, b.geom);

WITH qn AS (
    SELECT
        bbl,
        ST_TRANSFORM(ST_SETSRID(geom, 2263), 4326) AS geom
    FROM qnmappluto
)

UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM qn AS b
WHERE
    a.bbl IS NULL
    AND ST_WITHIN(a.geom, b.geom);

WITH si AS (
    SELECT
        bbl,
        ST_TRANSFORM(ST_SETSRID(geom, 2263), 4326) AS geom
    FROM simappluto
)

UPDATE cpdb_dcpattributes a
SET bbl = b.bbl
FROM si AS b
WHERE
    a.bbl IS NULL
    AND ST_WITHIN(a.geom, b.geom);
