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
            THEN st_transform(st_setsrid(st_point(
                xcoordinate::double precision,
                ycoordinate::double precision
            ),
            2263), 4326)
        ELSE st_setsrid(location::geometry, 4326)
    END) AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dcp_pops
FROM dcp_pops;

CALL append_to_facdb_base('_dcp_pops');
