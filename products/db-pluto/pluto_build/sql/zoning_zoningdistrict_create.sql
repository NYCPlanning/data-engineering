DROP TABLE IF EXISTS validdtm;
CREATE TABLE validdtm AS (
    SELECT
        a.bbl,
        ST_MAKEVALID(a.geom) AS geom
    FROM pluto AS a
    WHERE ST_GEOMETRYTYPE(ST_MAKEVALID(a.geom)) = 'ST_MultiPolygon'
);
CREATE INDEX validdtm_geom_idx ON validdtm USING gist (geom gist_geometry_ops_2d);

ANALYZE validdtm;

DROP TABLE IF EXISTS validzones;
CREATE TABLE validzones AS (
    SELECT
        a.zonedist,
        ST_MAKEVALID(a.geom) AS geom
    FROM dcp_zoningdistricts AS a
    WHERE ST_GEOMETRYTYPE(ST_MAKEVALID(a.geom)) = 'ST_MultiPolygon'
);
CREATE INDEX validzones_geom_idx ON validzones USING gist (geom gist_geometry_ops_2d);

ANALYZE validzones;

DROP TABLE IF EXISTS lotzoneper;
CREATE TABLE lotzoneper AS (
    SELECT
        p.bbl,
        n.zonedist,
        (ST_AREA(
            CASE
                WHEN ST_COVEREDBY(p.geom, n.geom) THEN p.geom::geography
                ELSE ST_MULTI(ST_INTERSECTION(p.geom, n.geom))::geography
            END
        )) AS segbblgeom,

        (ST_AREA(
            CASE
                WHEN ST_COVEREDBY(n.geom, p.geom) THEN n.geom::geography
                ELSE ST_MULTI(ST_INTERSECTION(n.geom, p.geom))::geography
            END
        )) AS segzonegeom,

        ST_AREA(p.geom::geography) AS allbblgeom,

        ST_AREA(n.geom::geography) AS allzonegeom

    FROM validdtm AS p
    INNER JOIN validzones AS n
        ON ST_INTERSECTS(p.geom, n.geom)
);

ALTER TABLE lotzoneper
SET (parallel_workers = 30);

ANALYZE lotzoneper;

DROP TABLE IF EXISTS lotzoneperorder;
CREATE TABLE lotzoneperorder AS (
    SELECT
        bbl,
        zonedist,
        segbblgeom,
        allbblgeom,
        segzonegeom,
        allzonegeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        ROW_NUMBER()
            OVER (
                PARTITION BY bbl
                ORDER BY segbblgeom DESC
            )
        AS row_number
    FROM lotzoneper
);
