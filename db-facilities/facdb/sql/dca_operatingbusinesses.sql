DROP TABLE IF EXISTS _dca_operatingbusinesses;

SELECT uid,
    source,
    initcap(business_name) as facname,
    address_building as addressnum,
    address_street_name as streetname,
    address_building || ' ' || address_street_name as address,
    address_city as city,
    address_zip as zipcode,
    address_borough as boro,
    borough_code as borocode,
    bin as bin,
    bbl as bbl,
    (
        CASE
            WHEN industry LIKE '%Scrap Metal%' THEN 'Scrap Metal Processing'
            WHEN industry LIKE '%Tow%' THEN 'Tow Truck Company'
            ELSE CONCAT('Commercial ', industry)
        END
    ) as factype,
    (
        CASE
            WHEN industry = 'Scrap Metal Processor' THEN 'Solid Waste Processing'
            WHEN industry = 'Parking Lot' THEN 'Parking Lots and Garages'
            WHEN industry = 'Garage' THEN 'Parking Lots and Garages'
            WHEN industry = 'Garage and Parking Lot' THEN 'Parking Lots and Garages'
            WHEN industry = 'Tow Truck Company' THEN 'Parking Lots and Garages'
        END
    ) as facsubgrp,
    initcap(business_name) as opname,
    'Non-public' as opabbrev,
    'NYCDCA' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b as geo_1b,
    geo_bl as geo_bl,
    geo_bn as geo_bn INTO _dca_operatingbusinesses
FROM dca_operatingbusinesses;

CALL append_to_facdb_base('_dca_operatingbusinesses');
