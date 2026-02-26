-- Create pluto_input_geocodes from DBT staging model with transformations
DROP TABLE IF EXISTS pluto_input_geocodes;
CREATE TABLE pluto_input_geocodes AS
SELECT * FROM stg__pluto_input_geocodes;

ALTER TABLE pluto_input_geocodes RENAME bbl TO geo_bbl;
ALTER TABLE pluto_input_geocodes ADD COLUMN xcoord text;
ALTER TABLE pluto_input_geocodes ADD COLUMN ycoord text;

UPDATE pluto_input_geocodes
SET
    xcoord = ST_X(ST_TRANSFORM(geom, 2263))::integer,
    ycoord = ST_Y(ST_TRANSFORM(geom, 2263))::integer,
    ct2010 = (CASE WHEN ct2010::numeric = 0 THEN NULL ELSE ct2010 END);
