/*
DESCRIPTION:
	Initial field mapping and prelimilary data cleaning for DOB NOW job applications data

INPUTS:
	dob_now_applications

OUTPUTS:
	_INIT_NOW_devdb (
		uid text,
		job_number text,
		job_type text,
		job_desc text,
		_occ_initial text,
		_occ_proposed text,
		stories_init numeric,
		stories_prop text,
		zoningsft_init numeric,
		zoningsft_prop numeric,
		classa_init numeric,
		classa_prop numeric,
		_job_status text,
		date_lastupdt text,
		date_filed text,
		date_statusd text,
		date_statusp text,
		date_statusr text,
		date_statusx text,
		zoningdist1 text,
		zoningdist2 text,
		zoningdist3 text,
		specialdist1 text,
		specialdist2 text,
		landmark text,
		ownership text,
		owner_name text,
		owner_biznm text,
		owner_address text,
		owner_zipcode text,
		owner_phone text,
		height_init text,
		height_prop text,
		constructnsf text,
		enlargement text,
		enlargementsf text,
		costestimate text,
		loftboardcert text,
		edesignation text,
		curbcut text,
		tracthomes text,
		address_numbr text,
		address_street text,
		address text,
		bin text,
		bbl text,
		boro text,
		x_withdrawal text,
		existingzoningsqft text,
		proposedzoningsqft text,
		zug_init,
		zug_prop,
		buildingclass text,
		otherdesc text,
		zsfr_prop numeric,
		zsfc_prop numeric,
		zsfcf_prop numeric,
		zsfm_prop numeric,
		Prking_prop numeric,

	)
*/

DROP TABLE IF EXISTS _INIT_NOW_DEVDB;
WITH
JOBNUMBER_RELEVANT AS (
    SELECT OGC_FID
    FROM DOB_NOW_APPLICATIONS
    WHERE
        JOBTYPE ~* 'CO|New'
        AND right(JOB_FILING_NUMBER, 2) = 'I1'
)

SELECT DISTINCT
    (OGC_FID::integer + (SELECT max(UID::integer) FROM _INIT_BIS_DEVDB))::text AS UID,
    left(JOB_FILING_NUMBER, strpos(JOB_FILING_NUMBER, '-') - 1)::text AS JOB_NUMBER,
    EXISTINGOCCUPANCYCLASSIFICATION::text AS _OCC_INITIAL,
    PROPOSEDOCCUPANCYCLASSIFICATION::text AS _OCC_PROPOSED,

    (CASE
        WHEN JOBTYPE ~* 'Alteration'
            THEN nullif(EXISTING_STORIES, '0')::numeric
    END)::numeric AS STORIES_INIT,
    (CASE
        WHEN JOBTYPE ~* 'Alteration'
            THEN nullif(PROPOSED_NO_OF_STORIES, '0')::numeric
    END)::numeric AS STORIES_PROP,

    -- set 0 -> null for jobtype = Alteration
    NULL::numeric AS ZONINGSFT_INIT,

    -- set 0 -> null for jobtype = Alteration
    NULL::numeric AS ZONINGSFT_PROP,

    (CASE
        WHEN JOBTYPE ~* 'New Building' THEN 0
        ELSE (CASE
            WHEN EXISTING_DWELLING_UNITS ~ '[^0-9]' THEN NULL
            ELSE EXISTING_DWELLING_UNITS::numeric
        END)
    END)::numeric AS CLASSA_INIT,
    (CASE
        WHEN JOBTYPE ~* 'Demolition' THEN 0
        ELSE (CASE
            WHEN PROPOSED_DWELLING_UNITS ~ '[^0-9]' THEN NULL
            ELSE PROPOSED_DWELLING_UNITS::numeric
        END)
    END)::numeric AS CLASSA_PROP,

    -- if existingdwellingunits is not a number then null
    FILING_STATUS AS _JOB_STATUS,

    -- if proposeddwellingunits is not a number then null
    CURRENT_STATUS_DATE AS DATE_LASTUPDT,

    -- one to one mappings
    FILING_DATE AS DATE_FILED,
    NULL AS DATE_STATUSD,
    "approved_(date)" AS DATE_STATUSP,
    PERMITISSUEDDATE AS DATE_STATUSR,
    SIGNOFFDATE AS DATE_STATUSX,
    NULL AS ZONINGDIST1,
    NULL AS ZONINGDIST2,
    NULL AS ZONINGDIST3,
    SPECIALDISTRICT1 AS SPECIALDIST1,
    SPECIALDISTRICT2 AS SPECIALDIST2,
    NULL AS OWNER_NAME,
    OWNER_S_BUSINESS_NAME AS OWNER_BIZNM,

    OWNER_S_STREET_NAME AS OWNER_ADDRESS,

    ZIP AS OWNER_ZIPCODE,

    OWNERPHONE AS OWNER_PHONE,
    (CASE
        WHEN JOBTYPE ~* 'Alteration'
            THEN nullif(EXISTINGBUILDINGHEIGHT, '0')
    END)::numeric AS HEIGHT_INIT,
    (CASE
        WHEN JOBTYPE ~* 'Alteration'
            THEN nullif(PROPOSEDBUILDINGHEIGHT, '0')
    END)::numeric AS HEIGHT_PROP,
    NULL AS CONSTRUCTNSF,
    NULL AS ENLARGEMENTSF,

    INITIAL_COST::money::text AS COSTESTIMATE,

    NULL AS LOFTBOARDCERT,

    LITTLE_E AS EDESIGNATION,

    NULL AS TRACTHOMES,

    BIN AS BIN,
    NULL::text AS ZSF_INIT,
    NULL::text AS ZSF_PROP,
    (SELECT DISTINCT array(
        SELECT DISTINCT E
        FROM
            unnest(
                ARRAY[string_to_array(
                    regexp_replace(
                        trim(lower(EXISTING_ZONING_USED_GROUP)),
                        '[^\wa-z0-9,]+', '', 'g'
                    ), ','
                )]
            ) AS A (E)
        ORDER BY E
    ))::text AS ZUG_INIT,

    (SELECT DISTINCT array(
        SELECT DISTINCT E
        FROM
            unnest(
                ARRAY[string_to_array(
                    regexp_replace(
                        trim(lower(PROPOSED_ZONING_USED_GROUP)),
                        '[^\wa-z0-9,]+', '', 'g'
                    ), ','
                )]
            ) AS A (E)
        ORDER BY E
    ))::text AS ZUG_PROP,

    (CASE
        WHEN USELABEL ~* 'Residential' THEN TOTAL_FLOOR_AREA
    END)::numeric AS ZSFR_PROP,
    (CASE
        WHEN USELABEL ~* 'Commercial' THEN TOTAL_FLOOR_AREA
    END)::numeric AS ZSFC_PROP,
    (CASE
        WHEN USELABEL ~* 'Community Facility' THEN TOTAL_FLOOR_AREA
    END)::numeric AS ZSFCF_PROP,
    (CASE
        WHEN USELABEL ~* 'Manufacturing' THEN TOTAL_FLOOR_AREA
    END)::numeric AS ZSFM_PROP,
    NO_OF_PARKING_SPACES::numeric AS PRKNGPROP,
    BUILDING_TYPE AS BLDG_CLASS,
    NULL AS DESC_OTHER,
    (CASE
        WHEN JOBTYPE = 'ALT-CO - New Building with Existing Elements to Remain' THEN 'Alteration'
        WHEN JOBTYPE = 'Alteration CO' THEN 'Alteration'
        ELSE JOBTYPE
    END) AS JOB_TYPE,
    (CASE
        WHEN JOBDESCRIPTION !~ '[a-zA-Z]'
            THEN NULL
        ELSE JOBDESCRIPTION
    END) AS JOB_DESC,
    (CASE
        WHEN LANDMARK ~* 'L' THEN 'Yes'
    END) AS LANDMARK,
    ownership_translate(
        left(CITY_OWNED, 1),
        upper(OWNERTYPE),
        left(NONPROFIT, 1)
    ) AS OWNERSHIP,
    -- Requested enhancement from Housing with new columns from DOB source data
    (CASE
        WHEN (HORIZONTALENLARGEMENT ~* 'true' AND NOT VERTICALENLARGEMENT ~* 'false')
            THEN 'Horizontal'
        WHEN (NOT HORIZONTALENLARGEMENT ~* 'false' AND VERTICALENLARGEMENT ~* 'true')
            THEN 'Vertical'
        WHEN (HORIZONTALENLARGEMENT ~* 'true' AND VERTICALENLARGEMENT ~* 'true')
            THEN 'Horizontal and Vertical'
    END) AS ENLARGEMENT,
    (CASE
        WHEN WORKTYPES = 'CC' THEN 'Yes'
    END) AS CURBCUT,
    regexp_replace(
        trim(HOUSE_NO),
        '(^|)0*', '', ''
    ) AS ADDRESS_NUMBR,
    trim(STREET_NAME) AS ADDRESS_STREET,
    regexp_replace(
        trim(HOUSE_NO),
        '(^|)0*', '', ''
    ) || ' ' || trim(STREET_NAME) AS ADDRESS,
    left(BIN, 1) || lpad(BLOCK, 5, '0') || lpad(right(LOT, 4), 4, '0') AS BBL,
    (CASE
        WHEN BOROUGH ~* 'Manhattan' THEN '1'
        WHEN BOROUGH ~* 'Bronx' THEN '2'
        WHEN BOROUGH ~* 'Brooklyn' THEN '3'
        WHEN BOROUGH ~* 'Queens' THEN '4'
        WHEN BOROUGH ~* 'Staten Island' THEN '5'
    END) AS BORO,
    (CASE
        WHEN FILING_STATUS ~* 'Withdrawn' THEN 'W'
    END) AS X_WITHDRAWAL,
    st_setsrid(st_point(
        LONGITUDE::double precision,
        LATITUDE::double precision
    ), 4326) AS DOB_GEOM
INTO _INIT_NOW_DEVDB
FROM DOB_NOW_APPLICATIONS
WHERE OGC_FID IN (SELECT OGC_FID FROM JOBNUMBER_RELEVANT);
