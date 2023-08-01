-- Assign geoms based on geocoding returned values
ALTER TABLE _cbbr_submissions
    DROP COLUMN IF EXISTS geo_from_geom,
    DROP COLUMN IF EXISTS geo_to_geom,
    DROP COLUMN IF EXISTS geom;

ALTER TABLE _cbbr_submissions
    ADD geo_from_geom GEOMETRY,
    ADD geo_to_geom GEOMETRY,
    ADD geom GEOMETRY;

--clean up empty values
UPDATE
    _cbbr_submissions
SET
    geo_bbl = nullif (geo_bbl, ''),
    geo_bin = nullif (geo_bin, ''),
    geo_longitude = nullif (geo_longitude, ''),
    geo_latitude = nullif (geo_latitude, ''),
    geo_x_coord = nullif (geo_x_coord, ''),
    geo_y_coord = nullif (geo_y_coord, '');

-- Assign geoms based on the centroid of the bin
UPDATE
    _cbbr_submissions a
SET
    geom = (
        CASE WHEN a.geo_bbl || a.geo_bin IS NOT NULL THEN
            ST_Centroid (b.geom)
        ELSE
            a.geom
        END)
FROM
    _doitt_buildingfootprints b
WHERE
    a.geo_bbl || a.geo_bin = b.base_bbl || b.bin;

-- Convert from_x_coord, from_y_coord to from_geom
-- Convert to_x_coord, to_y_coord to to_geom
UPDATE
    _cbbr_submissions
SET
    geo_from_geom = ST_TRANSFORM (ST_SetSRID (ST_MakePoint (geo_from_x_coord::numeric, geo_from_y_coord::numeric), 2263), 4326),
    geo_to_geom = ST_TRANSFORM (ST_SetSRID (ST_MakePoint (geo_to_x_coord::numeric, geo_to_y_coord::numeric), 2263), 4326);

-- Assign geoms based on the centroid of the bin
-- based on geo_longitude, geo_latitude, geo_x_coord and geo_y_coord
UPDATE
    _cbbr_submissions
SET
    geom = (
        CASE WHEN geo_longitude IS NOT NULL
            AND geom IS NULL THEN
            ST_SetSRID (ST_MakePoint (geo_longitude::double PRECISION, geo_latitude::double PRECISION), 4326)
        WHEN geo_x_coord IS NOT NULL
            AND geom IS NULL THEN
            ST_TRANSFORM (ST_SetSRID (ST_MakePoint (geo_x_coord::numeric, geo_y_coord::numeric), 2263), 4326)
        WHEN geo_from_geom IS NOT NULL
            AND geom IS NULL THEN
            ST_MakeLine (geo_from_geom, geo_to_geom)
        ELSE
            geom
        END);

