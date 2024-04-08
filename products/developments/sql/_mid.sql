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
DROP TABLE IF EXISTS join_date_permittd;
SELECT
    -- All INIT_devdb fields except for classa_init and classa_prop
    a.job_number,
    a.job_type,
    a.job_desc,
    a._occ_initial,
    a._occ_proposed,
    a.stories_init,
    a.stories_prop,
    a.zoningsft_init,
    a.zoningsft_prop,
    a.zug_init,
    a.zug_prop,
    a.zsfr_prop,
    a.zsfc_prop,
    a.zsfcf_prop,
    a.zsfm_prop,
    a.prkngprop,
    a._job_status,
    a.date_lastupdt,
    a.date_filed,
    a.date_statusd,
    a.date_statusp,
    a.date_statusr,
    a.date_statusx,
    a.zoningdist1,
    a.zoningdist2,
    a.zoningdist3,
    a.specialdist1,
    a.specialdist2,
    a.landmark,
    a.ownership,
    a.owner_name,
    a.owner_biznm,
    a.owner_address,
    a.owner_zipcode,
    a.owner_phone,
    a.height_init,
    a.height_prop,
    a.constructnsf,
    a.enlargement,
    a.enlargementsf,
    a.costestimate,
    a.loftboardcert,
    a.edesignation,
    a.curbcut,
    a.tracthomes,
    a.address_numbr,
    a.address_street,
    a.address,
    a.bin,
    a.bbl,
    a.boro,
    a.x_withdrawal,
    a.datasource,
    a.geo_bbl,
    a.geo_bin,
    a.geo_address_numbr,
    a.geo_address_street,
    a.geo_address,
    a.geo_zipcode,
    a.geo_boro,
    a.geo_cd,
    a.geo_council,
    a.geo_nta2020,
    a.geo_ntaname2020,
    a.geo_cb2020,
    a.geo_ct2020,
    a.bctcb2020,
    a.bct2020,
    a.geo_cdta2020,
    a.geo_cdtaname2020,
    a.geo_csd,
    a.geo_policeprct,
    a.geo_firedivision,
    a.geo_firebattalion,
    a.geo_firecompany,
    a.geo_schoolelmntry,
    a.geo_schoolmiddle,
    a.geo_schoolsubdist,
    a.geo_latitude,
    a.geo_longitude,
    a.latitude,
    a.longitude,
    a.geom,
    a.geomsource,
    a.zsf_prop,
    a.zsf_init,
    a.desc_other,
    a.bldg_class,
    b.date_permittd,
    b.permit_year,
    b.permit_qrtr
INTO join_date_permittd
FROM init_devdb AS a
LEFT JOIN status_q_devdb AS b
    ON a.job_number = b.job_number;

/*
CORRECTIONS: (implemeted 2021/02/22)
    date_permittd
*/
CALL apply_correction(:'build_schema', 'JOIN_date_permittd', '_manual_corrections', 'date_permittd');

/*
CONTINUE
*/
DROP TABLE IF EXISTS _mid_devdb;
WITH join_co AS (
    SELECT
        a.*,
        /** Complete dates for non-demolitions come from CO (_date_complete). For
            demolitions, complete dates are status Q date (date_permittd)
            when the record has a status X date, and NULL otherwise **/
        CASE
            WHEN a.job_type = 'Demolition'
                THEN CASE
                    WHEN a.date_statusx IS NOT NULL THEN a.date_permittd
                END
            ELSE b._date_complete
        END AS date_complete,

        b.co_latest_certtype,
        b.co_latest_units::numeric
    FROM join_date_permittd AS a
    LEFT JOIN co_devdb AS b
        ON a.job_number = b.job_number
),
join_units AS (
    SELECT
        a.*,
        extract(YEAR FROM date_complete)::text AS complete_year,
        year_quarter(date_complete) AS complete_qrtr,
        b.classa_init,
        b.classa_prop,
        b.classa_net,
        b.hotel_init,
        b.hotel_prop,
        b.otherb_init,
        b.otherb_prop,
        CASE
            WHEN b.classa_net != 0 THEN a.co_latest_units / b.classa_net
        END AS classa_complt_pct,
        b.classa_net - a.co_latest_units AS classa_complt_diff,
        b.resid_flag
    FROM join_co AS a
    LEFT JOIN units_devdb AS b
        ON a.job_number = b.job_number
),
join_occ AS (
    SELECT
        a.*,
        b.occ_initial,
        b.occ_proposed,
        flag_nonres(
            a.resid_flag,
            a.job_desc,
            b.occ_initial,
            b.occ_proposed
        ) AS nonres_flag
    FROM join_units AS a
    LEFT JOIN occ_devdb AS b
        ON a.job_number = b.job_number
)
SELECT *
INTO _mid_devdb
FROM join_occ;

DROP TABLE join_date_permittd;
