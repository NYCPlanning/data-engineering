-- create index on pluto and shoreline file
DROP INDEX IF EXISTS pluto_gix;
CREATE INDEX pluto_gix ON pluto USING gist (geom gist_geometry_ops_2d);
