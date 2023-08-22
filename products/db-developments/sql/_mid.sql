/*
DESCRIPTION:
    Merging INIT_devdb with (STATUS_Q_devdb, CO_devdb, UNITS_devdb, OCC_devdb)
    JOIN KEY: job_number

INPUTS:

    INIT_devdb (
        * job_number,
        ...
    )

    STATUS_Q_devdb (
        * job_number,
        date_permittd,
        permit_year,
        permit_qrtr
    )

    CO_devdb (
        * job_number,
        _date_complete,
        co_latest_certtype,
        co_latest_units
    )

    UNITS_devdb (
        * job_number,
        classa_init,
        classa_prop,
        hotel_init,
	    hotel_prop,
	    otherb_init,
	    otherb_prop,
        classa_net
    )

    OCC_devdb (
        * job_number,
        occ_initial,
        occ_proposed
    )

OUTPUTS:
    _MID_devdb (
        * job_number,
        date_permittd,
        complete_year,
        complete_qrtr,
        date_complete,
        co_latest_certtype,
        co_latest_units,
        classa_init,
        classa_prop,
        hotel_init,
	    hotel_prop,
	    otherb_init,
	    otherb_prop,
        classa_net,
        classa_complt_diff,
        occ_initial,
        occ_proposed,
        resid_flag,
        nonres_flag
        ...
    )
*/
DROP TABLE IF EXISTS JOIN_DATE_PERMITTD;
SELECT
    -- All INIT_devdb fields except for classa_init and classa_prop
    A.JOB_NUMBER,
    A.JOB_TYPE,
    A.JOB_DESC,
    A._OCC_INITIAL,
    A._OCC_PROPOSED,
    A.STORIES_INIT,
    A.STORIES_PROP,
    A.ZONINGSFT_INIT,
    A.ZONINGSFT_PROP,
    A.ZUG_INIT,
    A.ZUG_PROP,
    A.ZSFR_PROP,
    A.ZSFC_PROP,
    A.ZSFCF_PROP,
    A.ZSFM_PROP,
    A.PRKNGPROP,
    A._JOB_STATUS,
    A.DATE_LASTUPDT,
    A.DATE_FILED,
    A.DATE_STATUSD,
    A.DATE_STATUSP,
    A.DATE_STATUSR,
    A.DATE_STATUSX,
    A.ZONINGDIST1,
    A.ZONINGDIST2,
    A.ZONINGDIST3,
    A.SPECIALDIST1,
    A.SPECIALDIST2,
    A.LANDMARK,
    A.OWNERSHIP,
    A.OWNER_NAME,
    A.OWNER_BIZNM,
    A.OWNER_ADDRESS,
    A.OWNER_ZIPCODE,
    A.OWNER_PHONE,
    A.HEIGHT_INIT,
    A.HEIGHT_PROP,
    A.CONSTRUCTNSF,
    A.ENLARGEMENT,
    A.ENLARGEMENTSF,
    A.COSTESTIMATE,
    A.LOFTBOARDCERT,
    A.EDESIGNATION,
    A.CURBCUT,
    A.TRACTHOMES,
    A.ADDRESS_NUMBR,
    A.ADDRESS_STREET,
    A.ADDRESS,
    A.BIN,
    A.BBL,
    A.BORO,
    A.X_WITHDRAWAL,
    A.DATASOURCE,
    A.GEO_BBL,
    A.GEO_BIN,
    A.GEO_ADDRESS_NUMBR,
    A.GEO_ADDRESS_STREET,
    A.GEO_ADDRESS,
    A.GEO_ZIPCODE,
    A.GEO_BORO,
    A.GEO_CD,
    A.GEO_COUNCIL,
    A.GEO_NTA2020,
    A.GEO_NTANAME2020,
    A.GEO_CB2020,
    A.GEO_CT2020,
    A.BCTCB2020,
    A.BCT2020,
    A.GEO_CDTA2020,
    A.GEO_CDTANAME2020,
    A.GEO_CSD,
    A.GEO_POLICEPRCT,
    A.GEO_FIREDIVISION,
    A.GEO_FIREBATTALION,
    A.GEO_FIRECOMPANY,
    A.GEO_SCHOOLELMNTRY,
    A.GEO_SCHOOLMIDDLE,
    A.GEO_SCHOOLSUBDIST,
    A.GEO_LATITUDE,
    A.GEO_LONGITUDE,
    A.LATITUDE,
    A.LONGITUDE,
    A.GEOM,
    A.GEOMSOURCE,
    A.ZSF_PROP,
    A.ZSF_INIT,
    A.DESC_OTHER,
    A.BLDG_CLASS,
    B.DATE_PERMITTD,
    B.PERMIT_YEAR,
    B.PERMIT_QRTR
INTO JOIN_DATE_PERMITTD
FROM INIT_DEVDB AS A
LEFT JOIN STATUS_Q_DEVDB AS B
    ON A.JOB_NUMBER = B.JOB_NUMBER;

/*
CORRECTIONS: (implemeted 2021/02/22)
    date_permittd
*/
CALL apply_correction(: 'build_schema', 'JOIN_date_permittd', '_manual_corrections', 'date_permittd');

/*
CONTINUE
*/
DROP TABLE IF EXISTS _MID_DEVDB;
WITH
JOIN_CO AS (
    SELECT
        A.*,
        /** Complete dates for non-demolitions come from CO (_date_complete). For
            demolitions, complete dates are status Q date (date_permittd)
            when the record has a status X date, and NULL otherwise **/
        B.CO_LATEST_CERTTYPE,

        B.CO_LATEST_UNITS::numeric,
        (CASE
            WHEN A.JOB_TYPE = 'Demolition'
                THEN CASE
                    WHEN A.DATE_STATUSX IS NOT NULL
                        THEN A.DATE_PERMITTD
                END
            ELSE B._DATE_COMPLETE
        END) AS DATE_COMPLETE
    FROM JOIN_DATE_PERMITTD AS A
    LEFT JOIN CO_DEVDB AS B
        ON A.JOB_NUMBER = B.JOB_NUMBER
),

JOIN_UNITS AS (
    SELECT
        A.*,
        extract(YEAR FROM DATE_COMPLETE)::text AS COMPLETE_YEAR,
        B.CLASSA_INIT,
        B.CLASSA_PROP,
        B.CLASSA_NET,
        B.HOTEL_INIT,
        B.HOTEL_PROP,
        B.OTHERB_INIT,
        B.OTHERB_PROP,
        B.RESID_FLAG,
        year_quarter(DATE_COMPLETE) AS COMPLETE_QRTR,
        (CASE
            WHEN B.CLASSA_NET != 0
                THEN A.CO_LATEST_UNITS / B.CLASSA_NET
        END) AS CLASSA_COMPLT_PCT,
        B.CLASSA_NET - A.CO_LATEST_UNITS AS CLASSA_COMPLT_DIFF
    FROM JOIN_CO AS A
    LEFT JOIN UNITS_DEVDB AS B
        ON A.JOB_NUMBER = B.JOB_NUMBER
),

JOIN_OCC AS (
    SELECT
        A.*,
        B.OCC_INITIAL,
        B.OCC_PROPOSED,
        flag_nonres(
            A.RESID_FLAG,
            A.JOB_DESC,
            B.OCC_INITIAL,
            B.OCC_PROPOSED
        ) AS NONRES_FLAG
    FROM JOIN_UNITS AS A
    LEFT JOIN OCC_DEVDB AS B
        ON A.JOB_NUMBER = B.JOB_NUMBER
)

SELECT *
INTO _MID_DEVDB
FROM JOIN_OCC;

DROP TABLE JOIN_DATE_PERMITTD;
