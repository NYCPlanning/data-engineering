/** QAQC
	JOB_TYPE:
		dem_nb_overlap
    UNITS:
        units_init_null
	    units_init_null
        dup_equal_units -> dup_bbl_address_units
        dup_diff_units -> dup_bbl_address
    OCC:
        b_nonres_with_units
	    units_res_accessory
	    b_likely_occ_desc
    CO:
        units_co_prop_mismatch
    STATUS:
        incomp_tract_home
**/

DROP TABLE IF EXISTS MATCH_DEM_NB;
SELECT
    A.JOB_NUMBER AS JOB_NUMBER_DEM,
    B.JOB_NUMBER AS JOB_NUMBER_NB,
    A.GEO_BBL
INTO MATCH_DEM_NB
FROM MID_DEVDB AS A
INNER JOIN MID_DEVDB AS B
    ON A.GEO_BBL = B.GEO_BBL
WHERE
    A.JOB_TYPE = 'Demolition'
    AND B.JOB_TYPE = 'New Building';

-- JOBNUMBER_duplicates
DROP TABLE IF EXISTS JOBNUMBER_DUPLICATES;
WITH
BBL_ADDRESS_GROUPS AS (
    SELECT
        ADDRESS,
        COALESCE(GEO_BBL, 'NULL BBL') AS GEO_BBL
    FROM MID_DEVDB
    WHERE
        JOB_INACTIVE IS NULL
        AND ADDRESS IS NOT NULL
    GROUP BY COALESCE(GEO_BBL, 'NULL BBL'), ADDRESS
    HAVING COUNT(*) > 1
),

BBL_ADDRESS_UNIT_GROUPS AS (
    SELECT
        ADDRESS,
        CLASSA_NET,
        COALESCE(GEO_BBL, 'NULL BBL') AS GEO_BBL
    FROM MID_DEVDB
    WHERE
        JOB_INACTIVE IS NULL
        AND ADDRESS IS NOT NULL
        AND CLASSA_NET IS NOT NULL
    GROUP BY COALESCE(GEO_BBL, 'NULL BBL'), ADDRESS, CLASSA_NET
    HAVING COUNT(*) > 1
)

SELECT
    JOB_NUMBER,
    CASE
        WHEN
            GEO_BBL || ADDRESS || CLASSA_NET
            IN (
                SELECT GEO_BBL || ADDRESS || CLASSA_NET
                FROM BBL_ADDRESS_UNIT_GROUPS
            )
            THEN COALESCE(GEO_BBL, 'NULL BBL') || ' : ' || ADDRESS || ' : ' || CLASSA_NET
    END AS DUP_BBL_ADDRESS_UNITS,
    CASE
        WHEN
            GEO_BBL || ADDRESS
            IN (
                SELECT GEO_BBL || ADDRESS
                FROM BBL_ADDRESS_GROUPS
            )
            THEN COALESCE(GEO_BBL, 'NULL BBL') || ' : ' || ADDRESS
    END AS DUP_BBL_ADDRESS
INTO JOBNUMBER_DUPLICATES
FROM MID_DEVDB;

DROP TABLE IF EXISTS MID_QAQC;
WITH
JOBNUMBER_DEM_NB AS (
    SELECT DISTINCT JOB_NUMBER_DEM AS JOB_NUMBER FROM MATCH_DEM_NB
    UNION
    SELECT DISTINCT JOB_NUMBER_NB AS JOB_NUMBER FROM MATCH_DEM_NB
),

JOBNUMBER_CO_PROP_MISMATCH AS (
    SELECT
        JOB_NUMBER,
        CO_LATEST_CERTTYPE
    FROM MID_DEVDB
    WHERE
        JOB_TYPE ~* 'New Building|Alteration'
        AND CO_LATEST_UNITS != CLASSA_PROP
),

JOBNUMBER_INCOMPLETE_TRACT AS (
    SELECT JOB_NUMBER
    FROM MID_DEVDB
    WHERE
        TRACTHOMES = 'Y'
        AND JOB_STATUS ~* '1|2|3'
)

SELECT DISTINCT
    STATUS_QAQC.*,
    (
        JOB_TYPE IN ('Demolition', 'Alteration')
        AND RESID_FLAG = 'Residential'
        AND CLASSA_INIT IS NULL
    )::integer AS UNITS_INIT_NULL,
    (
        JOB_TYPE IN ('New Building', 'Alteration')
        AND RESID_FLAG = 'Residential'
        AND CLASSA_PROP IS NULL
    )::integer AS UNITS_PROP_NULL,
    (
        (OCC_INITIAL !~* 'residential' AND CLASSA_INIT != 0 AND CLASSA_INIT IS NOT NULL)
        OR (OCC_PROPOSED !~* 'residential' AND CLASSA_PROP != 0 AND CLASSA_PROP IS NOT NULL)
    )::integer AS B_NONRES_WITH_UNITS,
    (
        (
            (
                ADDRESS_NUMBR LIKE '%GAR%'
                OR JOB_DESC ~* 'pool|shed|gazebo|garage'
            ) AND (
                CLASSA_INIT::numeric IN (1, 2)
                OR CLASSA_PROP::numeric IN (1, 2)
            )
        )
        OR (
            (
                OCC_INITIAL LIKE '%(U)%'
                OR OCC_INITIAL LIKE '%(K)%'
                OR OCC_PROPOSED LIKE '%(U)%'
                OR OCC_PROPOSED LIKE '%(K)%'
            )
            AND (
                CLASSA_INIT::numeric > 0
                OR CLASSA_PROP::numeric > 0
            )
        )
    )::integer AS UNITS_RES_ACCESSORY,
    (
        OCC_INITIAL ~* 'hotel|assisted|incapacitated|restrained'
        OR OCC_PROPOSED ~* 'hotel|assisted|incapacitated|restrained'
        OR JOB_DESC ~* CONCAT(
            'Hotel|Motel|Boarding|Hostel|Lodge|UG 5|UG5', '|',
            'Group 5|Grp 5|Class B|Class ''b''|Class "b"', '|',
            'SRO |Single room|Furnished|Rooming unit', '|',
            'Dorm |Dorms |Dormitor|Transient|Homeless', '|',
            'Shelter|Group quarter|Beds|Convent|Monastery', '|',
            'Accommodation|Harassment|CNH|Settlement|Halfway', '|',
            'Nursing home|Assisted|Supportive|Sleeping', '|',
            'UG3|UG 3|Group 3|Grp 3'
        )
    )::integer AS B_LIKELY_OCC_DESC,
    (
        TRACTHOMES = 'Y'
        AND JOB_STATUS ~* '1|2|3'
    )::integer AS INCOMP_TRACT_HOME,
    (A.JOB_NUMBER IN (SELECT JOB_NUMBER FROM JOBNUMBER_DEM_NB))::integer AS DEM_NB_OVERLAP,
    B.DUP_BBL_ADDRESS,
    B.DUP_BBL_ADDRESS_UNITS,
    (
        CASE
            WHEN A.JOB_NUMBER IN (SELECT JOB_NUMBER FROM JOBNUMBER_CO_PROP_MISMATCH)
                THEN C.CO_LATEST_CERTTYPE
        END
    ) AS UNITS_CO_PROP_MISMATCH
INTO MID_QAQC
FROM MID_DEVDB AS A
LEFT JOIN STATUS_QAQC ON A.JOB_NUMBER = STATUS_QAQC.JOB_NUMBER
LEFT JOIN JOBNUMBER_DUPLICATES AS B ON A.JOB_NUMBER = B.JOB_NUMBER
LEFT JOIN JOBNUMBER_CO_PROP_MISMATCH AS C ON A.JOB_NUMBER = C.JOB_NUMBER;
