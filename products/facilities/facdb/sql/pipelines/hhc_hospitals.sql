DROP TABLE IF EXISTS _hhc_hospitals;

SELECT
    uid,
    source,
    facility_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    regexp_replace(split_part(location_1, '(', 1), '.{6}$', '') AS address,
    NULL AS city,
    postcode AS zipcode,
    borough AS boro,
    NULL AS borocode,
    bin,
    bbl,
    facility_type AS factype,
    'Hospitals and Clinics' AS facsubgrp,
    'NYC Health and Hospitals Corporation' AS opname,
    'NYCHHC' AS opabbrev,
    'NYSDOH' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    -- geom arrives as WKT text, not geometry: sanitize_df's replace() drops the
    -- GeoDataFrame subclass for this dataset, so the loader writes it with to_sql.
    coalesce(
        st_setsrid(geom::geometry, 4326),
        st_point(longitude::double precision, latitude::double precision)
    ) AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _hhc_hospitals
FROM hhc_hospitals
WHERE facility_type <> 'Nursing Home';

CALL append_to_facdb_base('_hhc_hospitals');
