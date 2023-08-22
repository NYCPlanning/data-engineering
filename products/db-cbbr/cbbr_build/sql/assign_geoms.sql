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
    geo_bbl = nullif(geo_bbl, ''),
    geo_bin = nullif(geo_bin, ''),
    geo_longitude = nullif(geo_longitude, ''),
    geo_latitude = nullif(geo_latitude, ''),
    geo_x_coord = nullif(geo_x_coord, ''),
    geo_y_coord = nullif(geo_y_coord, '');

-- Assign geoms based on the centroid of the bin
UPDATE
_cbbr_submissions a
SET
    geom = (
        CASE
            WHEN a.geo_bbl || a.geo_bin IS NOT NULL
                THEN
                    st_centroid(b.geom)
            ELSE
                a.geom
        END
    )
FROM
    _doitt_buildingfootprints AS b
WHERE
    a.geo_bbl || a.geo_bin = b.base_bbl || b.bin;

-- Convert from_x_coord, from_y_coord to from_geom
-- Convert to_x_coord, to_y_coord to to_geom
UPDATE
_cbbr_submissions
SET
    geo_from_geom = st_transform(st_setsrid(st_makepoint(geo_from_x_coord::NUMERIC, geo_from_y_coord::NUMERIC), 2263), 4326),
    geo_to_geom = st_transform(st_setsrid(st_makepoint(geo_to_x_coord::NUMERIC, geo_to_y_coord::NUMERIC), 2263), 4326);

-- Assign geoms based on the centroid of the bin
-- based on geo_longitude, geo_latitude, geo_x_coord and geo_y_coord
UPDATE
_cbbr_submissions
SET
    geom = (
        CASE
            WHEN
                geo_longitude IS NOT NULL
                AND geom IS NULL
                THEN
                    st_setsrid(st_makepoint(geo_longitude::DOUBLE PRECISION, geo_latitude::DOUBLE PRECISION), 4326)
            WHEN
                geo_x_coord IS NOT NULL
                AND geom IS NULL
                THEN
                    st_transform(st_setsrid(st_makepoint(geo_x_coord::NUMERIC, geo_y_coord::NUMERIC), 2263), 4326)
            WHEN
                geo_from_geom IS NOT NULL
                AND geom IS NULL
                THEN
                    st_makeline(geo_from_geom, geo_to_geom)
            ELSE
                geom
        END
    );
