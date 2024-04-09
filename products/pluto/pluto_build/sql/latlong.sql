-- populate the lat and long fields
UPDATE pluto a
SET
    latitude
    = st_y(st_transform(st_setsrid(st_makepoint(a.xcoord::double precision, a.ycoord::double precision), 2263), 4326)),
    longitude
    = st_x(st_transform(st_setsrid(st_makepoint(a.xcoord::double precision, a.ycoord::double precision), 2263), 4326))
WHERE a.xcoord IS NOT NULL;

ALTER TABLE pluto ADD COLUMN centroid GEOMETRY (GEOMETRY, 4326);

UPDATE pluto SET centroid = st_setsrid(st_makepoint(longitude::double precision, latitude::double precision), 4326);
