DROP TABLE IF EXISTS _nysdoh_nursinghomes;
SELECT uid,
    source,
    facility_name as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    street_address as address,
    city,
    zip as zipcode,
    county as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    (
        CASE
            WHEN bed_type ~* 'NHBEDSAV' THEN 'Nursing Home'
            WHEN bed_type ~* 'ADHCPSLOTSAV' THEN 'Adult Day Care'
        END
    ) as factype,
    (
        CASE
            WHEN bed_type = 'NHBEDSAV' THEN 'Residential Health Care'
            ELSE 'Other Health Care'
        END
    ) as facsubgrp,
    facility_name as opname,
    NULL as opabbrev,
    'NYSDOH' as overabbrev,
    total_capacity as capacity,
    (
        CASE
            WHEN bed_type = 'NHBEDSAV' THEN 'beds'
            WHEN bed_type = 'ADHCPSLOTSAV' THEN 'seats'
        END
    ) as captype,
    location::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn INTO _nysdoh_nursinghomes
FROM nysdoh_nursinghomes
WHERE bed_type ~* 'NHBEDSAV|ADHCPSLOTSAV';
CALL append_to_facdb_base('_nysdoh_nursinghomes');
