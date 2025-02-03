DROP TABLE IF EXISTS _nysdoh_nursinghomes;
SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    street_address AS address,
    city,
    zip AS zipcode,
    county AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (
        CASE
            WHEN bed_type ~* 'NHBEDSAV' THEN 'Nursing Home'
            WHEN bed_type ~* 'ADHCPSLOTSAV' THEN 'Adult Day Care'
        END
    ) AS factype,
    (
        CASE
            WHEN bed_type = 'NHBEDSAV' THEN 'Residential Health Care'
            ELSE 'Other Health Care'
        END
    ) AS facsubgrp,
    facility_name AS opname,
    NULL AS opabbrev,
    'NYSDOH' AS overabbrev,
    total_capacity AS capacity,
    (
        CASE
            WHEN bed_type = 'NHBEDSAV' THEN 'beds'
            WHEN bed_type = 'ADHCPSLOTSAV' THEN 'seats'
        END
    ) AS captype,
    geom AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysdoh_nursinghomes
FROM nysdoh_nursinghomes
WHERE bed_type ~* 'NHBEDSAV|ADHCPSLOTSAV';
CALL append_to_facdb_base('_nysdoh_nursinghomes');
