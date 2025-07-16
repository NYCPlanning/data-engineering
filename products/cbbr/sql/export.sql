-- Create tables to export
-- cbbr_submissions_needgeoms: all records without a geometry
DROP TABLE IF EXISTS cbbr_submissions_needgeoms;

SELECT *
INTO cbbr_submissions_needgeoms
FROM _cbbr_submissions
WHERE geom IS NULL
ORDER BY borough_code, street_name, address;

-- cbbr_submissions_needgeoms_c: lowest priority
DROP TABLE IF EXISTS cbbr_submissions_needgeoms_c;

SELECT
    unique_id,
    tracking_code,
    borough,
    borough_code,
    cd,
    commdist,
    cb_label,
    type_br,
    type,
    need,
    request,
    explanation,
    location_specific,
    site_or_facility_name,
    address,
    street_name,
    on_street,
    cross_street_1,
    cross_street_2,
    intersection_street_1,
    intersection_street_2,
    agency_acronym,
    agency,
    agency_category_response,
    agency_response,
    geo_function,
    geom
INTO cbbr_submissions_needgeoms_c
FROM _cbbr_submissions
WHERE geom IS NULL
ORDER BY cb_label, street_name, address;

-- cbbr_submissions_needgeoms_b
DROP TABLE IF EXISTS cbbr_submissions_needgeoms_b;

SELECT *
INTO cbbr_submissions_needgeoms_b
FROM cbbr_submissions_needgeoms_c
WHERE type_br = 'C' AND location_specific = 'Yes'
ORDER BY cb_label, street_name, address;

-- remove B from C table
DELETE FROM cbbr_submissions_needgeoms_c
WHERE EXISTS (
    SELECT 1
    FROM cbbr_submissions_needgeoms_b
    WHERE cbbr_submissions_needgeoms_c.unique_id = cbbr_submissions_needgeoms_b.unique_id
);

--
-- cbbr_submissions_needgeoms_a: highest priority
DROP TABLE IF EXISTS cbbr_submissions_needgeoms_a;

SELECT *
INTO cbbr_submissions_needgeoms_a
FROM cbbr_submissions_needgeoms_b
WHERE
    location_specific = 'Yes'
    AND (
        address IS NOT NULL
        OR site_or_facility_name IS NOT NULL
        OR (on_street IS NOT NULL AND cross_street_1 IS NOT NULL AND cross_street_2 IS NOT NULL)
        OR (intersection_street_1 IS NOT NULL AND intersection_street_2 IS NOT NULL)
    )
ORDER BY cb_label, street_name, address;

-- remove A from B table
DELETE FROM cbbr_submissions_needgeoms_b
WHERE EXISTS (
    SELECT 1
    FROM cbbr_submissions_needgeoms_a
    WHERE cbbr_submissions_needgeoms_b.unique_id = cbbr_submissions_needgeoms_a.unique_id
);

-- cbbr_export: final table
DROP TABLE IF EXISTS cbbr_export;

SELECT
    unique_id,
    tracking_code,
    borough,
    borough_code,
    cd,
    commdist,
    cb_label,
    type_br,
    -- type,
    priority,
    need,
    request,
    explanation,
    location_specific,
    site_or_facility_name AS site_name,
    address,
    street_name,
    on_street,
    cross_street_1,
    cross_street_2,
    intersection_street_1,
    intersection_street_2,
    CASE
        WHEN
            supporters_1 IS NULL
            OR supporters_1 IN ('', ' ', 'n/a')
            THEN NULL
        ELSE
            supporters_1
    END AS supporters_1,
    CASE
        WHEN
            supporters_2 IS NULL
            OR supporters_2 IN ('', ' ', 'n/a')
            THEN
                NULL
        ELSE
            supporters_2
    END AS supporters_2,
    agency_acronym,
    agency,
    agency_category_response,
    agency_response,
    geo_function,
    geom
INTO cbbr_export
FROM _cbbr_submissions;

-- cbbr_export_poly
DROP TABLE IF EXISTS cbbr_export_poly;

SELECT *
INTO cbbr_export_poly
FROM cbbr_export
WHERE ST_GeometryType(geom) = 'ST_MultiPolygon';

-- cbbr_export_pts
DROP TABLE IF EXISTS cbbr_export_pts;

SELECT *
INTO cbbr_export_pts
FROM cbbr_export
WHERE ST_GeometryType(geom) = 'ST_MultiPoint';

-- Export Records with GeometryCollection introduced in the manual mapping process 

DROP TABLE IF EXISTS cbbr_export_geocollection;

SELECT *
INTO cbbr_export_geocollection
FROM cbbr_export
WHERE ST_GeometryType(geom) = 'ST_GeometryCollection';


-- -- drop geom column from cbbr_export
-- ALTER TABLE cbbr_export
--     DROP COLUMN IF EXISTS geom;
