DROP TABLE IF EXISTS _dca_operatingbusinesses;

SELECT
    uid,
    source,
    initcap(business_name) AS facname,
    building_number AS addressnum,
    street1 AS streetname,
    building_number || ' ' || street1 AS address,
    city,
    zip_code AS zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    (
        CASE
            WHEN business_category LIKE '%Scrap Metal%' THEN 'Scrap Metal Processing'
            WHEN business_category LIKE '%Tow%' THEN 'Tow Truck Company'
            ELSE concat('Commercial ', business_category)
        END
    ) AS factype,
    (
        CASE
            WHEN business_category = 'Scrap Metal Processor' THEN 'Solid Waste Processing'
            WHEN business_category IN (
                'Parking Lot',
                'Garage',
                'Garage and Parking Lot',
                'Garage & Parking Lot',
                'Tow Truck Company'
            ) THEN 'Parking Lots and Garages'
        END
    ) AS facsubgrp,
    initcap(business_name) AS opname,
    'Non-public' AS opabbrev,
    'NYCDCA' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geom AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _dca_operatingbusinesses
FROM dca_operatingbusinesses;

CALL append_to_facdb_base('_dca_operatingbusinesses');
