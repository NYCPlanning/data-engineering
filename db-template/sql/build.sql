ALTER TABLE _dcp_zoningdistricts ADD new_build_column text;

DROP TABLE IF EXISTS validzones;

CREATE TABLE validzones AS (
    SELECT
        zoningdistricts.zonedist,
        ST_MakeValid(zoningdistricts.geom) AS geom
    FROM _dcp_zoningdistricts AS zoningdistricts
    WHERE
        ST_GeometryType(ST_MakeValid(zoningdistricts.geom)) = 'ST_MultiPolygon'
);

CREATE INDEX validzones_geom_idx ON validzones
USING GIST(geom gist_geometry_ops_2d);
