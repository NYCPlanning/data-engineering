-- update geoms where names are the exact same
WITH master AS (
    WITH singlename AS (
        WITH filtered AS (
            SELECT
                facname,
                COUNT(facname) AS namecount
            FROM
                _dcp_facilities
            GROUP BY
                facname
        )

        SELECT facname
        FROM
            filtered
        WHERE
            namecount = 1
    )

    SELECT
        a.unique_id,
        a.facility_or_park_name,
        b.facname,
        b.geom
    FROM
        _cbbr_submissions AS a,
        _dcp_facilities AS b
    WHERE
        a.geom IS NULL
        AND a.facility_or_park_name IS NOT NULL
        AND b.facname LIKE '%' || ' ' || '%' || ' ' || '%'
        AND '%' || UPPER(a.facility_or_park_name) || '%' = '%' || UPPER(b.facname) || '%'
        AND b.facname IN (
            SELECT facname
            FROM
                singlename
        )
        AND UPPER(a.borough) = UPPER(b.boro)
)

UPDATE
_cbbr_submissions
SET
    geo_function = '_dcp_facilities',
    geom = master.geom
FROM
    master
WHERE
    _cbbr_submissions.unique_id = master.unique_id
    AND _cbbr_submissions.geom IS NULL;

-- update lib geoms using facdb
WITH master AS (
    SELECT
        a.unique_id,
        a.facility_or_park_name,
        b.facname,
        b.geom
    FROM
        _cbbr_submissions AS a,
        (
            SELECT *
            FROM
                _dcp_facilities
            WHERE
                UPPER(facgroup) = 'LIBRARIES'
        ) AS b
    WHERE (
        UPPER(a.facility_or_park_name)
        LIKE '%LIBRARY%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%BRANCH%'
    )
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%AND%'
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%&%'
    AND (
        ('%' || UPPER(REPLACE(a.facility_or_park_name, ' Branch', ' Library')) || '%' LIKE '%' || UPPER(b.facname) || '%')
        OR ('%' || UPPER(REPLACE(a.facility_or_park_name, ' Branch', '')) || '%' LIKE '%' || UPPER(b.facname) || '%')
        OR ('%' || UPPER(a.facility_or_park_name) || '%' LIKE '%' || UPPER(b.facname) || '%')
    )
    AND UPPER(a.borough) = UPPER(b.boro)
    AND a.geom IS NULL
)

UPDATE
_cbbr_submissions
SET
    geo_function = '_dcp_facilities',
    geom = master.geom
FROM
    master
WHERE
    _cbbr_submissions.unique_id = master.unique_id
    AND _cbbr_submissions.geom IS NULL;

-- update precinct geoms using facdb
WITH master AS (
    SELECT
        a.unique_id,
        a.facility_or_park_name,
        b.facname,
        b.geom
    FROM
        _cbbr_submissions AS a,
        (
            SELECT *
            FROM
                _dcp_facilities
            WHERE
                UPPER(facsubgrp) = 'POLICE SERVICES'
        ) AS b
    WHERE (
        UPPER(a.facility_or_park_name)
        LIKE '%PRECINCT%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%PCT%'
    )
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%AND%'
    AND REGEXP_REPLACE(a.facility_or_park_name, '\D', '', 'g') = REGEXP_REPLACE(b.facname, '\D', '', 'g')
    AND REGEXP_REPLACE(a.facility_or_park_name, '\D', '', 'g') IS NOT NULL
    AND UPPER(a.borough) = UPPER(b.boro)
    AND a.geom IS NULL
)

UPDATE
_cbbr_submissions
SET
    geo_function = '_dcp_facilities',
    geom = master.geom
FROM
    master
WHERE
    _cbbr_submissions.unique_id = master.unique_id
    AND _cbbr_submissions.geom IS NULL;

-- update school geoms using facdb
WITH master AS (
    SELECT
        a.unique_id,
        a.facility_or_park_name,
        b.facname,
        b.geom
    FROM
        _cbbr_submissions AS a,
        (
            SELECT *
            FROM
                _dcp_facilities
            WHERE
                UPPER(facsubgrp) = 'PUBLIC K-12 SCHOOLS'
        ) AS b
    WHERE (
        UPPER(a.facility_or_park_name)
        LIKE '%SCHOOL%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%P.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%I.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%M.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%H.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE 'PS%'
        OR UPPER(a.facility_or_park_name)
        LIKE 'P.S%'
    )
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%AND%'
    AND REGEXP_REPLACE(a.facility_or_park_name, '\D', '', 'g') = REGEXP_REPLACE(b.facname, '\D', '', 'g')
    AND REGEXP_REPLACE(a.facility_or_park_name, '\D', '', 'g') != ''
    AND UPPER(a.borough) = UPPER(b.boro)
    AND a.geom IS NULL
)

UPDATE
_cbbr_submissions
SET
    geo_function = '_dcp_facilities',
    geom = master.geom
FROM
    master
WHERE
    _cbbr_submissions.unique_id = master.unique_id
    AND _cbbr_submissions.geom IS NULL;

-- update school geoms using facdb
-- round 2 after removing leading 0s
WITH master AS (
    SELECT
        a.unique_id,
        a.facility_or_park_name,
        b.facname,
        b.geom
    FROM
        _cbbr_submissions AS a,
        (
            SELECT *
            FROM
                _dcp_facilities
            WHERE
                UPPER(facsubgrp) = 'PUBLIC K-12 SCHOOLS'
        ) AS b
    WHERE (
        UPPER(a.facility_or_park_name)
        LIKE '%SCHOOL%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%P.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%I.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%M.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%H.S.%'
        OR UPPER(a.facility_or_park_name)
        LIKE 'PS%'
        OR UPPER(a.facility_or_park_name)
        LIKE 'P.S%'
    )
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%AND%'
    AND REGEXP_REPLACE(a.facility_or_park_name, '\D', '', 'g') = REGEXP_REPLACE(REPLACE(REPLACE(b.facname, ' 0', ' '), ' 0', ' '), '\D', '', 'g')
    AND REGEXP_REPLACE(a.facility_or_park_name, '\D', '', 'g') != ''
    AND UPPER(a.borough) = UPPER(b.boro)
    AND a.geom IS NULL
)

UPDATE
_cbbr_submissions
SET
    geo_function = '_dcp_facilities',
    geom = master.geom
FROM
    master
WHERE
    _cbbr_submissions.unique_id = master.unique_id
    AND _cbbr_submissions.geom IS NULL;

-- update park geoms using facdb
WITH master AS (
    SELECT
        a.unique_id,
        a.facility_or_park_name,
        b.facname,
        a.agency_acronym,
        a.borough,
        b.geom
    FROM
        _cbbr_submissions AS a,
        (
            SELECT *
            FROM
                _dcp_facilities
            WHERE
                UPPER(facgroup) = 'PARKS AND PLAZAS'
        ) AS b
    WHERE (
        UPPER(a.facility_or_park_name)
        LIKE '%PARK%'
        OR UPPER(a.facility_or_park_name)
        LIKE '%PLAYGROUND%'
        OR agency_acronym = 'DPR'
    )
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%AND%'
    AND (
        ('%' || REPLACE(UPPER(a.facility_or_park_name), ' PARK', '') || '%' LIKE '%' || REPLACE(UPPER(b.facname), ' PARK', '') || '%')
        OR ('%' || REPLACE(UPPER(a.facility_or_park_name), ' PARK', '') || '%' LIKE '%' || REPLACE(UPPER(b.facname), ' PLAYGROUND', '') || '%')
        OR ('%' || REPLACE(UPPER(a.facility_or_park_name), ' PLAYGROUND', '') || '%' LIKE '%' || REPLACE(UPPER(b.facname), ' PLAYGROUND', '') || '%')
        OR ('%' || REPLACE(UPPER(a.facility_or_park_name), ' PLAYGROUND', '') || '%' LIKE '%' || REPLACE(UPPER(b.facname), ' PARK', '') || '%')
    )
    AND UPPER(b.facname) != 'PARK'
    AND UPPER(b.facname) != 'LOT'
    AND UPPER(b.facname) != 'GARDEN'
    AND UPPER(b.facname) != 'TRIANGLE'
    AND UPPER(b.facname) != 'SITTING AREA'
    AND UPPER(b.facname) != 'BRIDGE PARK'
    AND UPPER(b.facname)
    NOT LIKE '%GARDEN%'
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%GARDEN%'
    AND UPPER(a.facility_or_park_name)
    NOT LIKE '%WOOD%'
    AND UPPER(a.borough) = UPPER(b.boro)
    AND a.geom IS NULL
)

UPDATE
_cbbr_submissions
SET
    geo_function = '_dcp_facilities',
    geom = master.geom
FROM
    master
WHERE
    _cbbr_submissions.unique_id = master.unique_id
    AND _cbbr_submissions.geom IS NULL;
