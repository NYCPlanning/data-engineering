/*
DESCRIPTION:
    Creates various QAQC tables. The majority are designed to identify invalid data in IPIS.
    Some are designed to help GRU research potential new addresses.
    There is also a version-to-version comparison for COLP review.
INPUTS:
	dcas_ipis
    dcas_ipis_geocodes
    ipis_colp_georesults
    _colp
    dcp_colp
OUTPUTS:
	ipis_unmapped
    ipis_modified_hnums
    ipis_modified_names
    ipis_colp_geoerrors
    ipis_sname_errors
    ipis_hnum_errors
    ipis_bbl_errors
    ipis_cd_errors
    usetype_changes
*/

-- Create QAQC table of unmappable input records
DROP TABLE IF EXISTS ipis_unmapped;
SELECT
    a.*,
    b.geo_bbl,
    b.grc,
    b.rsn,
    b.msg
INTO ipis_unmapped
FROM dcas_ipis AS a
INNER JOIN dcas_ipis_geocodes AS b
    ON
        a.bbl = b.input_bbl
        AND md5(
            cast((a.*) AS text)) IN (
            SELECT DISTINCT uid FROM _colp
            WHERE "XCOORD" IS NULL
        );

-- Create QAQC table of records with modified house numbers
DROP TABLE IF EXISTS ipis_modified_hnums;
SELECT
    a.uid,
    a.dcas_bbl,
    a.dcas_hnum,
    a.display_hnum,
    a.dcas_sname,
    a.sname_1b,
    b.parcel_name,
    b.agency,
    b.primary_use_code,
    b.primary_use_text
INTO ipis_modified_hnums
FROM ipis_colp_georesults AS a
INNER JOIN dcas_ipis AS b
    ON a.uid = md5(cast((b.*) AS text))
WHERE
    a.dcas_hnum <> a.display_hnum
    OR (a.dcas_hnum IS NOT NULL AND a.display_hnum = '')
    OR (a.dcas_hnum IS NULL AND a.display_hnum <> '');

-- Create QAQC table of records with modified parcel names
DROP TABLE IF EXISTS ipis_modified_names;
WITH _ipis_modified_names AS (
    SELECT
        a.uid,
        a.dcas_bbl,
        a.dcas_hnum,
        a.display_hnum,
        a.dcas_sname,
        a.sname_1b,
        b.parcel_name,
        a."PARCELNAME" AS display_name,
        b.agency,
        b.primary_use_code,
        b.primary_use_text
    FROM ipis_colp_georesults AS a
    INNER JOIN dcas_ipis AS b
        ON a.uid = md5(cast((b.*) AS text))
    WHERE b.parcel_name <> a."PARCELNAME"
),
distinct_rows AS (
    SELECT DISTINCT
        a.*,
        b.reviewed
    FROM _ipis_modified_names AS a
    LEFT JOIN reviewed_modified_names AS b
        ON
            a.dcas_bbl = b.dcas_bbl
            AND a.parcel_name = b.parcel_name
            AND a.display_name = b.display_name
)
SELECT *
INTO ipis_modified_names
FROM distinct_rows
ORDER BY reviewed, dcas_bbl, parcel_name;

-- Create QAQC table of addresses that return errors from 1B
DROP TABLE IF EXISTS ipis_colp_geoerrors;
SELECT
    a.uid,
    a.dcas_bbl,
    b."MAPBBL" AS dcas_map_bbl,
    a.display_hnum,
    a.dcas_hnum,
    a.dcas_sname,
    a.hnum_1b,
    a.sname_1b,
    a.bbl_1b,
    a.grc_1e,
    a.rsn_1e,
    a.warn_1e,
    a.msg_1e,
    a.grc_1a,
    a.rsn_1a,
    a.warn_1a,
    a.msg_1a,
    a."AGENCY",
    a."PARCELNAME",
    a."USECODE",
    a."USETYPE",
    a."OWNERSHIP",
    a."CATEGORY",
    a."EXPANDCAT",
    a."EXCATDESC",
    a."LEASED",
    a."FINALCOM",
    a."AGREEMENT",
    (CASE
        WHEN
            left(a."USECODE", 2)
            IN ('01', '02', '03', '05', '06', '07', '12')
            THEN '1'
        ELSE '0'
    END) AS building
INTO ipis_colp_geoerrors
FROM ipis_colp_georesults AS a
INNER JOIN _colp AS b
    ON a.uid = b.uid
-- Exclude records where both GRC are 00
WHERE (a.grc_1e <> '00' OR a.grc_1a <> '00')
/*
Exclude records where either one or both are 01
having reasons other than 1 - 9, B, C, I, J
*/
AND NOT (
    (
        a.grc_1e = '00'
        AND (
            a.grc_1a = '01'
            AND a.rsn_1a IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', 'B', 'C', 'I', 'J')
        )
    )
    OR
    (
        a.grc_1a = '00'
        AND (
            a.grc_1e = '01'
            AND a.rsn_1e IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', 'B', 'C', 'I', 'J')
        )
    )
    OR
    (
        (
            a.grc_1a = '01'
            AND a.rsn_1a IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', 'B', 'C', 'I', 'J')
        )
        AND
        (
            a.grc_1e = '01'
            AND a.rsn_1e IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', 'B', 'C', 'I', 'J')
        )
    )
);

-- Create QAQC table of addresses that return streetname errors from 1B
DROP TABLE IF EXISTS ipis_sname_errors;
SELECT
    a.uid,
    a.dcas_bbl,
    b."MAPBBL" AS dcas_map_bbl,
    a.display_hnum,
    a.dcas_hnum,
    a.dcas_sname,
    a.hnum_1b,
    a.sname_1b,
    a.bbl_1b,
    a.grc_1e,
    a.rsn_1e,
    a.warn_1e,
    a.msg_1e,
    a.grc_1a,
    a.rsn_1a,
    a.warn_1a,
    a.msg_1a,
    a."AGENCY",
    a."PARCELNAME",
    a."USECODE",
    a."USETYPE",
    a."OWNERSHIP",
    a."CATEGORY",
    a."EXPANDCAT",
    a."EXCATDESC",
    a."LEASED",
    a."FINALCOM",
    a."AGREEMENT",
    (CASE
        WHEN
            left(a."USECODE", 2)
            IN ('01', '02', '03', '05', '06', '07', '12')
            THEN '1'
        ELSE '0'
    END) AS building
INTO ipis_sname_errors
FROM ipis_colp_georesults AS a
INNER JOIN _colp AS b
    ON a.uid = b.uid
-- Include records where one or both GRC is 11 or EE
WHERE (
    a.grc_1e IN ('11', 'EE')
    OR a.grc_1a IN ('11', 'EE')
);

-- Create QAQC table of addresses that return address errors from 1B
DROP TABLE IF EXISTS ipis_hnum_errors;
SELECT
    a.uid,
    a.dcas_bbl,
    b."MAPBBL" AS dcas_map_bbl,
    a.display_hnum,
    a.dcas_hnum,
    a.dcas_sname,
    a.hnum_1b,
    a.sname_1b,
    a.bbl_1b,
    a.grc_1e,
    a.rsn_1e,
    a.warn_1e,
    a.msg_1e,
    a.grc_1a,
    a.rsn_1a,
    a.warn_1a,
    a.msg_1a,
    a."AGENCY",
    a."PARCELNAME",
    a."USECODE",
    a."USETYPE",
    a."OWNERSHIP",
    a."CATEGORY",
    a."EXPANDCAT",
    a."EXCATDESC",
    a."LEASED",
    a."FINALCOM",
    a."AGREEMENT",
    (CASE
        WHEN
            left(a."USECODE", 2)
            IN ('01', '02', '03', '05', '06', '07', '12')
            THEN '1'
        ELSE '0'
    END) AS building
INTO ipis_hnum_errors
FROM ipis_colp_georesults AS a
INNER JOIN _colp AS b
    ON a.uid = b.uid
-- Include records where one or both GRC is 41 or 42
WHERE (
    a.grc_1e IN ('41', '42')
    OR a.grc_1a IN ('41', '42')
);

-- Create QAQC table of records where address isn't valid for BBL
DROP TABLE IF EXISTS ipis_bbl_errors;
SELECT
    a.uid,
    a.dcas_bbl,
    b."MAPBBL" AS dcas_map_bbl,
    a.display_hnum,
    a.dcas_hnum,
    a.dcas_sname,
    a.hnum_1b,
    a.sname_1b,
    a.bbl_1b,
    a.grc_1e,
    a.rsn_1e,
    a.warn_1e,
    a.msg_1e,
    a.grc_1a,
    a.rsn_1a,
    a.warn_1a,
    a.msg_1a,
    a."AGENCY",
    a."PARCELNAME",
    a."USECODE",
    a."USETYPE",
    a."OWNERSHIP",
    a."CATEGORY",
    a."EXPANDCAT",
    a."EXCATDESC",
    a."LEASED",
    a."FINALCOM",
    a."AGREEMENT",
    (CASE
        WHEN
            left(a."USECODE", 2)
            IN ('01', '02', '03', '05', '06', '07', '12')
            THEN '1'
        ELSE '0'
    END) AS building
INTO ipis_bbl_errors
FROM ipis_colp_georesults AS a
INNER JOIN _colp AS b
    ON a.uid = b.uid
/*
Include records where billing BBL associated with the
DCAS input BBL does not match the address's returned BBL
*/
WHERE cast(a.bbl_1b AS numeric) <> b."MAPBBL";

-- Create QAQC table of mismatch between IPIS community district and PLUTO
DROP TABLE IF EXISTS ipis_cd_errors;
SELECT
    a.uid,
    a."BBL",
    a."CD" AS pluto_cd,
    b.cd AS dcas_cd,
    a."HNUM",
    a."SNAME",
    a."PARCELNAME"
INTO ipis_cd_errors
FROM _colp AS a
INNER JOIN dcas_ipis AS b
    ON a.uid = md5(cast((b.*) AS text))
WHERE
    a."CD" <> cast(b.cd AS smallint)
    OR (a."CD" IS NULL AND b.cd IS NOT NULL)
    OR (a."CD" IS NOT NULL AND b.cd IS NULL);

-- Create QAQC table of version-to-version changes in the number of records per use type
DROP TABLE IF EXISTS usetype_changes;
WITH
prev AS (
    SELECT
        data_library_version AS v_previous,
        usetype,
        count(*) AS num_records_previous
    FROM dcp_colp
    GROUP BY v_previous, usetype
),
current AS (
    SELECT
        to_char(current_date, 'YYYY/MM/DD') AS v_current,
        "USETYPE" AS usetype,
        count(*) AS num_records_current
    FROM colp
    GROUP BY "USETYPE"
)
SELECT
    a.usetype,
    a.v_previous,
    b.v_current,
    b.num_records_current,
    a.num_records_previous,
    b.num_records_current - a.num_records_previous AS difference
INTO usetype_changes
FROM prev AS a
INNER JOIN current AS b
    ON a.usetype = b.usetype;

-- Create QAQC tables of count of records by agency and usetype
DROP TABLE IF EXISTS records_by_agency;
SELECT
    "AGENCY",
    count(*) AS count
INTO records_by_agency
FROM colp
GROUP BY "AGENCY";

DROP TABLE IF EXISTS records_by_usetype;
SELECT
    "USETYPE",
    count(*) AS count
INTO records_by_usetype
FROM colp
GROUP BY "USETYPE";

DROP TABLE IF EXISTS records_by_agency_usetype;
SELECT
    "AGENCY",
    "USETYPE",
    count(*) AS count
INTO records_by_agency_usetype
FROM colp
GROUP BY "AGENCY", "USETYPE";
