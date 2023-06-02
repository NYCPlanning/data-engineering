DROP TABLE IF EXISTS validdtm;
CREATE TABLE validdtm AS (
SELECT a.bbl, ST_MakeValid(a.geom) as geom 
FROM pluto a
WHERE ST_GeometryType(ST_MakeValid(a.geom)) = 'ST_MultiPolygon');
CREATE INDEX validdtm_geom_idx ON validdtm USING GIST (geom gist_geometry_ops_2d);

VACUUM ANALYZE validdtm;

DROP TABLE IF EXISTS validzones;
CREATE TABLE validzones AS (
SELECT a.zonedist, ST_MakeValid(a.geom) as geom  
FROM dcp_zoningdistricts a
WHERE ST_GeometryType(ST_MakeValid(a.geom)) = 'ST_MultiPolygon'); 
CREATE INDEX validzones_geom_idx ON validzones USING GIST (geom gist_geometry_ops_2d);

VACUUM ANALYZE validzones;

DROP TABLE IF EXISTS lotzoneper;
CREATE TABLE lotzoneper AS (
    SELECT p.bbl, n.zonedist
    , (ST_Area(
        CASE 
            WHEN ST_CoveredBy(p.geom, n.geom) THEN p.geom::geography
            ELSE ST_Multi(ST_Intersection(p.geom,n.geom))::geography
        END)) AS segbblgeom,

        (ST_Area(
        CASE 
            WHEN ST_CoveredBy(n.geom, p.geom) THEN n.geom::geography 
            ELSE ST_Multi(ST_Intersection(n.geom,p.geom))::geography
        END)) AS segzonegeom,

        ST_Area(p.geom::geography) AS allbblgeom,

        ST_Area(n.geom::geography) AS allzonegeom

    FROM validdtm AS p 
    INNER JOIN validzones AS n 
        ON ST_Intersects(p.geom, n.geom));

ALTER TABLE lotzoneper
SET (parallel_workers=30);

VACUUM ANALYZE lotzoneper;

DROP TABLE IF EXISTS lotzoneperorder; 
CREATE TABLE lotzoneperorder AS (
    SELECT bbl, zonedist, segbblgeom, allbblgeom, (segbblgeom/allbblgeom)*100 as perbblgeom, 
            segzonegeom, allzonegeom, (segzonegeom/allzonegeom)*100 as perzonegeom, ROW_NUMBER()
        OVER (PARTITION BY bbl
            ORDER BY segbblgeom DESC) AS row_number
        FROM lotzoneper);