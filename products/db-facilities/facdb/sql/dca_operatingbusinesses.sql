DROP TABLE IF EXISTS _dca_operatingbusinesses;

SELECT
    uid,
    source,
    address_building AS addressnum,
    address_street_name AS streetname,
    address_city AS city,
    address_zip AS zipcode,
    address_borough AS boro,
    borough_code AS borocode,
    bin AS bin,
    bbl AS bbl,
    'Non-public' AS opabbrev,
    'NYCDCA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkt::geometry AS wkb_geometry,
    geo_1b AS geo_1b,
    geo_bl AS geo_bl,
    geo_bn AS geo_bn,
    initcap(business_name) AS facname,
    address_building || ' ' || address_street_name AS address,
    (
        CASE
            WHEN industry LIKE '%Scrap Metal%' THEN 'Scrap Metal Processing'
            WHEN industry LIKE '%Tow%' THEN 'Tow Truck Company'
            ELSE concat('Commercial ', industry)
        END
    ) AS factype,
    (
        CASE
            WHEN industry = 'Scrap Metal Processor' THEN 'Solid Waste Processing'
            WHEN industry = 'Parking Lot' THEN 'Parking Lots and Garages'
            WHEN industry = 'Garage' THEN 'Parking Lots and Garages'
            WHEN industry = 'Garage and Parking Lot' THEN 'Parking Lots and Garages'
            WHEN industry = 'Tow Truck Company' THEN 'Parking Lots and Garages'
        END
    ) AS facsubgrp,
    initcap(business_name) AS opname
INTO _dca_operatingbusinesses
FROM dca_operatingbusinesses;

CALL append_to_facdb_base('_dca_operatingbusinesses');
