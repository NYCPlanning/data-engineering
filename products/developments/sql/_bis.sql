/*
DESCRIPTION:
	Initial field mapping and prelimilary data cleaning for BIS job applications data

INPUTS:
	dob_jobapplications

OUTPUTS:
	_INIT_BIS_devdb (
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
		work_type_codes text,
		work_types text,
		x_withdrawal text,
		existingzoningsqft text,
		proposedzoningsqft text,
		buildingclass text,
		otherdesc text
	)
*/


DROP TABLE IF EXISTS _init_bis_devdb;

WITH applications AS (SELECT * FROM dob_jobapplications),

parking_spaces AS (SELECT * FROM dob_jobapplications_parkingspaces),

applications_of_interest AS (
    SELECT * FROM applications
    WHERE
        jobdocnumber = '01' AND (
            jobtype ~* 'A1|DM|NB' OR (
                jobtype = 'A2'
                AND sprinkler IS NULL
                AND lower(jobdescription) LIKE '%combin%'
                AND lower(jobdescription) NOT LIKE '%sprinkler%'
            )
        ) AND gid = 1
),

mapping_and_cleaning AS (
    SELECT
        jobnumber::text AS job_number,

        -- Job Type recoding
        CASE
            WHEN jobtype = 'A1' THEN 'Alteration'
            WHEN jobtype = 'DM' THEN 'Demolition'
            WHEN jobtype = 'NB' THEN 'New Building'
            WHEN jobtype = 'A2' THEN 'Alteration (A2)'
            ELSE jobtype
        END AS job_type,

        CASE
            WHEN jobdescription !~ '[a-zA-Z]'
                THEN NULL
            ELSE jobdescription
        END AS job_desc,

        -- removing '.' for existingoccupancy 
        -- and proposedoccupancy (3 records affected)
        replace(existingoccupancy, '.', '') AS _occ_initial,
        replace(proposedoccupancy, '.', '') AS _occ_proposed,

        -- set 0 -> null for jobtype = A1 or DM
        CASE
            WHEN jobtype ~* 'A1|DM'
                THEN nullif(existingnumstories, '0')::numeric
        END AS stories_init,

        -- set 0 -> null for jobtype = A1 or NB
        CASE
            WHEN jobtype ~* 'A1|NB'
                THEN nullif(proposednumstories, '0')::numeric
        END AS stories_prop,

        -- set 0 -> null for jobtype = A1 or DM\
        CASE
            WHEN jobtype ~* 'A1|DM'
                THEN nullif(existingzoningsqft, '0')::numeric
            ELSE existingzoningsqft::numeric
        END AS zoningsft_init,

        -- set 0 -> null for jobtype = A1 or DM
        CASE
            WHEN jobtype ~* 'A1|DM'
                THEN nullif(proposedzoningsqft, '0')::numeric
            ELSE proposedzoningsqft::numeric
        END AS zoningsft_prop,

        -- if existingdwellingunits is not a number then null
        CASE
            WHEN jobtype ~* 'NB' THEN 0
            WHEN existingdwellingunits ~ '[^0-9]' THEN NULL
            ELSE existingdwellingunits::numeric
        END AS classa_init,

        -- if proposeddwellingunits is not a number then null
        CASE
            WHEN jobtype ~* 'DM' THEN 0
            WHEN proposeddwellingunits ~ '[^0-9]' THEN NULL
            ELSE proposeddwellingunits::numeric
        END AS classa_prop,

        -- one to one mappings
        jobstatusdesc AS _job_status,
        latestactiondate AS date_lastupdt,
        prefilingdate AS date_filed,
        fullypaid AS date_statusd,
        approved AS date_statusp,
        fullypermitted AS date_statusr,
        signoffdate AS date_statusx,
        zoningdist1,
        zoningdist2,
        zoningdist3,
        specialdistrict1 AS specialdist1,
        specialdistrict2 AS specialdist2,

        CASE
            WHEN landmarked = 'Y' THEN 'Yes'
        END AS landmark,

        ownership_translate(
            cityowned,
            ownertype,
            nonprofit
        ) AS ownership,

        ownerlastname || ', ' || ownerfirstname AS owner_name,
        ownerbusinessname AS owner_biznm,
        ownerhousestreetname AS owner_address,
        zip AS owner_zipcode,
        ownerphone AS owner_phone,

        CASE
            WHEN jobtype ~* 'A1|DM'
                THEN nullif(existingheight, '0')
        END::numeric AS height_init,

        CASE
            WHEN jobtype ~* 'A1|NB'
                THEN nullif(proposedheight, '0')
        END::numeric AS height_prop,

        totalconstructionfloorarea AS constructnsf,

        CASE
            WHEN horizontalenlrgmt = 'Y' AND verticalenlrgmt != 'Y'
                THEN 'Horizontal'
            WHEN horizontalenlrgmt != 'Y' AND verticalenlrgmt = 'Y'
                THEN 'Vertical'
            WHEN horizontalenlrgmt = 'Y' AND verticalenlrgmt = 'Y'
                THEN 'Horizontal and Vertical'
        END AS enlargement,

        enlargementsqfootage AS enlargementsf,
        initialcost AS costestimate,

        CASE
            WHEN loftboard = 'Y' THEN 'Yes'
        END AS loftboardcert,

        CASE
            WHEN littlee = 'Y' THEN 'Yes'
            WHEN littlee = 'H' THEN 'Yes'
        END AS edesignation,

        CASE
            WHEN curbcut = 'X' THEN 'Yes'
        END AS curbcut,

        cluster AS tracthomes,
        regexp_replace(
            trim(housenumber),
            '(^|)0*', '', ''
        ) AS address_numbr,
        trim(streetname) AS address_street,
        regexp_replace(
            trim(housenumber),
            '(^|)0*', '', ''
        ) || ' ' || trim(streetname) AS address,
        bin,
        left(bin, 1) || lpad(block, 5, '0') || lpad(right(lot, 4), 4, '0') AS bbl,
        CASE
            WHEN borough ~* 'Manhattan' THEN '1'
            WHEN borough ~* 'Bronx' THEN '2'
            WHEN borough ~* 'Brooklyn' THEN '3'
            WHEN borough ~* 'Queens' THEN '4'
            WHEN borough ~* 'Staten Island' THEN '5'
        END AS boro,
        existingzoningsqft AS zsf_init,
        proposedzoningsqft AS zsf_prop,
        buildingclass AS bldg_class,
        otherdesc AS desc_other,
        specialactionstatus AS x_withdrawal,
        st_setsrid(st_point(
            longitude::double precision,
            latitude::double precision
        ), 4326) AS dob_geom
    FROM applications_of_interest
),

add_parking_spaces AS (
    SELECT
        mapping_and_cleaning.*,
        parking_spaces.existing_parking_spaces::numeric AS prkng_init,
        parking_spaces.proposed_parking_spaces::numeric AS prkng_prop
    FROM mapping_and_cleaning
    LEFT JOIN parking_spaces ON mapping_and_cleaning.job_number = parking_spaces.jobnumber
),

missing_columns AS (
    SELECT
        *,
        NULL::text AS work_type_codes,
        NULL::text AS work_types,
        NULL::text AS zug_init,
        NULL::text AS zug_prop,
        NULL::numeric AS zsfr_prop,
        NULL::numeric AS zsfc_prop,
        NULL::numeric AS zsfcf_prop,
        NULL::numeric AS zsfm_prop
    FROM add_parking_spaces
),

final AS (
    SELECT
        job_number,
        job_type,
        job_desc,
        _occ_initial,
        _occ_proposed,
        stories_init,
        stories_prop,
        zoningsft_init,
        zoningsft_prop,
        classa_init,
        classa_prop,
        _job_status,
        date_lastupdt,
        date_filed,
        date_statusd,
        date_statusp,
        date_statusr,
        date_statusx,
        zoningdist1,
        zoningdist2,
        zoningdist3,
        specialdist1,
        specialdist2,
        landmark,
        ownership,
        owner_name,
        owner_biznm,
        owner_address,
        owner_zipcode,
        owner_phone,
        height_init,
        height_prop,
        constructnsf,
        enlargement,
        enlargementsf,
        costestimate,
        loftboardcert,
        edesignation,
        curbcut,
        tracthomes,
        address_numbr,
        address_street,
        address,
        bin,
        bbl,
        boro,
        work_type_codes,
        work_types,
        zsf_init,
        zsf_prop,
        zug_init,
        zug_prop,
        zsfr_prop,
        zsfc_prop,
        zsfcf_prop,
        zsfm_prop,
        prkng_init,
        prkng_prop,
        bldg_class,
        desc_other,
        x_withdrawal,
        dob_geom
    FROM missing_columns
)

SELECT *
INTO _init_bis_devdb
FROM final;
