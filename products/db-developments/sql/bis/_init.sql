/*
DESCRIPTION:
	Initial field mapping and prelimilary data cleaning for BIS job applications data

INPUTS:
	dob_jobapplications

OUTPUTS:
	_INIT_BIS_devdb (
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
		buildingclass text,
		otherdesc text
	)
*/


DROP TABLE IF EXISTS _INIT_BIS_DEVDB;
WITH
-- identify relevant_jobs
JOBNUMBER_RELEVANT AS (
    SELECT OGC_FID
    FROM DOB_JOBAPPLICATIONS
    WHERE
        JOBDOCNUMBER = '01'
        AND
        (
            JOBTYPE ~* 'A1|DM|NB'
            OR
            (
                JOBTYPE = 'A2'
                AND SPRINKLER IS NULL
                AND lower(JOBDESCRIPTION) LIKE '%combin%'
                AND lower(JOBDESCRIPTION) NOT LIKE '%sprinkler%'
            )
        )
        AND GID = 1
)

SELECT DISTINCT
    OGC_FID::text AS UID,
    JOBNUMBER::text AS JOB_NUMBER,

    -- Job Type recoding
    JOBSTATUSDESC AS _JOB_STATUS,

    LATESTACTIONDATE AS DATE_LASTUPDT,

    -- removing '.' for existingoccupancy 
    -- and proposedoccupancy (3 records affected)
    PREFILINGDATE AS DATE_FILED,
    FULLYPAID AS DATE_STATUSD,

    -- set 0 -> null for jobtype = A1 or DM
    APPROVED AS DATE_STATUSP,

    -- set 0 -> null for jobtype = A1 or NB
    FULLYPERMITTED AS DATE_STATUSR,

    -- set 0 -> null for jobtype = A1 or DM\
    SIGNOFFDATE AS DATE_STATUSX,

    -- set 0 -> null for jobtype = A1 or DM
    ZONINGDIST1 AS ZONINGDIST1,

    -- if existingdwellingunits is not a number then null
    ZONINGDIST2 AS ZONINGDIST2,

    -- if proposeddwellingunits is not a number then null
    ZONINGDIST3 AS ZONINGDIST3,

    -- one to one mappings
    SPECIALDISTRICT1 AS SPECIALDIST1,
    SPECIALDISTRICT2 AS SPECIALDIST2,
    OWNERBUSINESSNAME AS OWNER_BIZNM,
    OWNERHOUSESTREETNAME AS OWNER_ADDRESS,
    ZIP AS OWNER_ZIPCODE,
    OWNERPHONE AS OWNER_PHONE,
    (CASE
        WHEN JOBTYPE ~* 'A1|DM'
            THEN nullif(EXISTINGHEIGHT, '0')
    END)::numeric AS HEIGHT_INIT,
    (CASE
        WHEN JOBTYPE ~* 'A1|NB'
            THEN nullif(PROPOSEDHEIGHT, '0')
    END)::numeric AS HEIGHT_PROP,
    TOTALCONSTRUCTIONFLOORAREA AS CONSTRUCTNSF,
    ENLARGEMENTSQFOOTAGE AS ENLARGEMENTSF,
    INITIALCOST AS COSTESTIMATE,
    CLUSTER AS TRACTHOMES,

    BIN AS BIN,

    EXISTINGZONINGSQFT AS ZSF_INIT,

    PROPOSEDZONINGSQFT AS ZSF_PROP,
    NULL::text AS ZUG_INIT,
    NULL::text AS ZUG_PROP,
    NULL::numeric AS ZSFR_PROP,
    NULL::numeric AS ZSFC_PROP,

    NULL::numeric AS ZSFCF_PROP,

    NULL::numeric AS ZSFM_PROP,

    NULL::numeric AS PRKNGPROP,

    BUILDINGCLASS AS BLDG_CLASS,

    OTHERDESC AS DESC_OTHER,
    SPECIALACTIONSTATUS AS X_WITHDRAWAL,

    (CASE
        WHEN JOBTYPE = 'A1' THEN 'Alteration'
        WHEN JOBTYPE = 'DM' THEN 'Demolition'
        WHEN JOBTYPE = 'NB' THEN 'New Building'
        WHEN JOBTYPE = 'A2' THEN 'Alteration (A2)'
        ELSE JOBTYPE
    END) AS JOB_TYPE,

    (CASE
        WHEN JOBDESCRIPTION !~ '[a-zA-Z]'
            THEN NULL
        ELSE JOBDESCRIPTION
    END) AS JOB_DESC,

    replace(EXISTINGOCCUPANCY, '.', '') AS _OCC_INITIAL,

    replace(PROPOSEDOCCUPANCY, '.', '') AS _OCC_PROPOSED,
    (CASE
        WHEN JOBTYPE ~* 'A1|DM'
            THEN nullif(EXISTINGNUMSTORIES, '0')::numeric
    END) AS STORIES_INIT,
    (CASE
        WHEN JOBTYPE ~* 'A1|NB'
            THEN nullif(PROPOSEDNUMSTORIES, '0')::numeric
    END) AS STORIES_PROP,
    (CASE
        WHEN JOBTYPE ~* 'A1|DM'
            THEN nullif(EXISTINGZONINGSQFT, '0')::numeric
        ELSE EXISTINGZONINGSQFT::numeric
    END) AS ZONINGSFT_INIT,
    (CASE
        WHEN JOBTYPE ~* 'A1|DM'
            THEN nullif(PROPOSEDZONINGSQFT, '0')::numeric
        ELSE PROPOSEDZONINGSQFT::numeric
    END) AS ZONINGSFT_PROP,
    (CASE
        WHEN JOBTYPE ~* 'NB' THEN 0
        ELSE (CASE
            WHEN EXISTINGDWELLINGUNITS ~ '[^0-9]' THEN NULL
            ELSE EXISTINGDWELLINGUNITS::numeric
        END)
    END) AS CLASSA_INIT,
    (CASE
        WHEN JOBTYPE ~* 'DM' THEN 0
        ELSE (CASE
            WHEN PROPOSEDDWELLINGUNITS ~ '[^0-9]' THEN NULL
            ELSE PROPOSEDDWELLINGUNITS::numeric
        END)
    END) AS CLASSA_PROP,
    -- Add dummy columns for union to now applications for _init_devdb
    (CASE
        WHEN LANDMARKED = 'Y' THEN 'Yes'
    END) AS LANDMARK,
    ownership_translate(
        CITYOWNED,
        OWNERTYPE,
        NONPROFIT
    ) AS OWNERSHIP,
    OWNERLASTNAME || ', ' || OWNERFIRSTNAME AS OWNER_NAME,
    (CASE
        WHEN (HORIZONTALENLRGMT = 'Y' AND VERTICALENLRGMT != 'Y')
            THEN 'Horizontal'
        WHEN (HORIZONTALENLRGMT != 'Y' AND VERTICALENLRGMT = 'Y')
            THEN 'Vertical'
        WHEN (HORIZONTALENLRGMT = 'Y' AND VERTICALENLRGMT = 'Y')
            THEN 'Horizontal and Vertical'
    END) AS ENLARGEMENT,
    (CASE
        WHEN LOFTBOARD = 'Y' THEN 'Yes'
    END) AS LOFTBOARDCERT,
    (CASE
        WHEN LITTLEE = 'Y' THEN 'Yes'
        WHEN LITTLEE = 'H' THEN 'Yes'
    END) AS EDESIGNATION,
    (CASE
        WHEN CURBCUT = 'X' THEN 'Yes'
    END) AS CURBCUT,
    regexp_replace(
        trim(HOUSENUMBER),
        '(^|)0*', '', ''
    ) AS ADDRESS_NUMBR,
    trim(STREETNAME) AS ADDRESS_STREET,
    -- End Dummy columns 
    regexp_replace(
        trim(HOUSENUMBER),
        '(^|)0*', '', ''
    ) || ' ' || trim(STREETNAME) AS ADDRESS,
    left(BIN, 1) || lpad(BLOCK, 5, '0') || lpad(right(LOT, 4), 4, '0') AS BBL,
    CASE
        WHEN BOROUGH ~* 'Manhattan' THEN '1'
        WHEN BOROUGH ~* 'Bronx' THEN '2'
        WHEN BOROUGH ~* 'Brooklyn' THEN '3'
        WHEN BOROUGH ~* 'Queens' THEN '4'
        WHEN BOROUGH ~* 'Staten Island' THEN '5'
    END AS BORO,
    st_setsrid(st_point(
        LONGITUDE::double precision,
        LATITUDE::double precision
    ), 4326) AS DOB_GEOM
INTO _INIT_BIS_DEVDB
FROM DOB_JOBAPPLICATIONS
WHERE OGC_FID IN (SELECT OGC_FID FROM JOBNUMBER_RELEVANT);
