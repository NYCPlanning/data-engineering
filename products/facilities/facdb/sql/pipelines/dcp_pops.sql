DROP TABLE IF EXISTS _dcp_pops;
SELECT
    pops_number AS uid,
    source,
    (CASE
        WHEN building_name IS NOT NULL AND building_name <> '' THEN building_name
        ELSE building_address_with_zip_code
    END) AS facname,
    address_number AS addressnum,
    street_name AS streetname,
    address_number || ' ' || street_name AS address,
    NULL AS city,
    zip_code AS zipcode,
    borough_name AS boro,
    borough_code AS borocode,
    bin,
    bbl,
    'Privately Owned Public Space' AS factype,
    'Privately Owned Public Space' AS facsubgrp,
    'Not Available' AS opname,
    'Non-public' AS opabbrev,
    'NYCDCP' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    (CASE
        WHEN location IS NULL
            THEN ST_TRANSFORM(ST_SETSRID(ST_POINT(
                xcoordinate::DOUBLE PRECISION,
                ycoordinate::DOUBLE PRECISION
            ),
            2263), 4326)
        ELSE ST_SETSRID(location::GEOMETRY, 4326)
    END) AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dcp_pops
FROM dcp_pops;

CALL APPEND_TO_FACDB_BASE('_dcp_pops');
