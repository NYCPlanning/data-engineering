-- populate the lat and long fields
UPDATE pluto a
SET
    latitude = ST_Y(ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(a.xcoord::double precision, a.ycoord::double precision), 2263), 4326)),
    longitude = ST_X(ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT(a.xcoord::double precision, a.ycoord::double precision), 2263), 4326))
WHERE a.xcoord IS NOT NULL;

ALTER TABLE pluto ADD COLUMN centroid GEOMETRY (GEOMETRY, 4326);

UPDATE pluto SET centroid = ST_SETSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision), 4326);
