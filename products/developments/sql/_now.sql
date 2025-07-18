/*
DESCRIPTION:
	Initial field mapping and prelimilary data cleaning for DOB NOW job applications data

INPUTS:
	dob_now_applications

OUTPUTS:
	_INIT_NOW_devdb (
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

DROP TABLE IF EXISTS _init_now_devdb;

WITH source_data AS (SELECT * FROM dob_now_applications),

work_types_lookup AS (SELECT * FROM lookup_now_types),

jobs_of_interest AS (
    SELECT * FROM source_data
    WHERE
        lower(jobtype) != 'no work'
        AND right(job_filing_number, 2) = 'I1'
),

mapping_and_cleaning AS (
    SELECT
        left(job_filing_number, strpos(job_filing_number, '-') - 1)::text AS job_number,
        CASE
            WHEN jobtype = 'ALT-CO - New Building with Existing Elements to Remain' THEN 'Alteration'
            WHEN jobtype = 'Alteration' THEN 'Alteration (Non-CO)'
            WHEN jobtype = 'Alteration CO' THEN 'Alteration'
            WHEN jobtype = 'Full Demolition' THEN 'Demolition'
            ELSE jobtype
        END AS job_type,
        CASE
            WHEN jobdescription !~ '[a-zA-Z]'
                THEN NULL
            ELSE jobdescription
        END AS job_desc,

        existingoccupancyclassification::text AS _occ_initial,
        proposedoccupancyclassification::text AS _occ_proposed,

        -- set 0 -> null for jobtype = Alteration
        (CASE
            WHEN jobtype ~* 'Alteration' THEN nullif(existing_stories, '0')::numeric
        END)::numeric AS stories_init,

        -- set 0 -> null for jobtype = Alteration
        (CASE
            WHEN jobtype ~* 'Alteration' THEN nullif(proposed_no_of_stories, '0')::numeric
        END)::numeric AS stories_prop,

        -- if existingdwellingunits is not a number then null
        (CASE
            WHEN jobtype ~* 'New Building' THEN 0
            WHEN existing_dwelling_units ~ '[^0-9]' THEN NULL
            ELSE existing_dwelling_units::numeric
        END)::numeric AS classa_init,

        -- if proposeddwellingunits is not a number then null
        (CASE
            WHEN jobtype ~* 'Demolition' THEN 0
            WHEN proposed_dwelling_units ~ '[^0-9]' THEN NULL
            ELSE proposed_dwelling_units::numeric
        END)::numeric AS classa_prop,

        -- one to one mappings
        filing_status AS _job_status,
        current_status_date AS date_lastupdt,
        filing_date AS date_filed,

        "approved_(date)" AS date_statusp,
        permitissueddate AS date_statusr,
        signoffdate AS date_statusx,

        specialdistrict1 AS specialdist1,
        specialdistrict2 AS specialdist2,

        CASE
            WHEN landmark ~* 'L' THEN 'Yes'
        END AS landmark,

        ownership_translate(
            left(city_owned, 1),
            upper(ownertype),
            left(nonprofit, 1)
        ) AS ownership,

        owner_s_business_name AS owner_biznm,
        owner_s_street_name AS owner_address,
        zip AS owner_zipcode,
        ownerphone AS owner_phone,

        (CASE
            WHEN jobtype ~* 'Alteration'
                THEN nullif(existingbuildingheight, '0')
        END)::numeric AS height_init,

        (CASE
            WHEN jobtype ~* 'Alteration'
                THEN nullif(proposedbuildingheight, '0')
        END)::numeric AS height_prop,

        CASE
            WHEN horizontalenlargement ~* 'true' AND NOT verticalenlargement ~* 'false'
                THEN 'Horizontal'
            WHEN NOT horizontalenlargement ~* 'false' AND verticalenlargement ~* 'true'
                THEN 'Vertical'
            WHEN horizontalenlargement ~* 'true' AND verticalenlargement ~* 'true'
                THEN 'Horizontal and Vertical'
        END AS enlargement,

        initial_cost::money::text AS costestimate,
        little_e AS edesignation,

        worktypes AS work_type_codes,
        CASE
            WHEN worktypes = 'CC' THEN 'Yes'
        END AS curbcut,

        regexp_replace(
            trim(house_no),
            '(^|)0*', '', ''
        ) AS address_numbr,
        trim(street_name) AS address_street,

        regexp_replace(
            trim(house_no),
            '(^|)0*', '', ''
        ) || ' ' || trim(street_name) AS address,

        bin,
        left(bin, 1) || lpad(block, 5, '0') || lpad(right(lot, 4), 4, '0') AS bbl,
        CASE
            WHEN borough ~* 'Manhattan' THEN '1'
            WHEN borough ~* 'Bronx' THEN '2'
            WHEN borough ~* 'Brooklyn' THEN '3'
            WHEN borough ~* 'Queens' THEN '4'
            WHEN borough ~* 'Staten Island' THEN '5'
        END AS boro,

        (SELECT DISTINCT array(
            SELECT DISTINCT e
            FROM
                unnest(
                    ARRAY[string_to_array(
                        regexp_replace(
                            trim(lower(existing_zoning_used_group)),
                            '[^\wa-z0-9,]+', '', 'g'
                        ), ','
                    )]
                ) AS a (e)
            ORDER BY e
        ))::text AS zug_init,
        (SELECT DISTINCT array(
            SELECT DISTINCT e
            FROM
                unnest(
                    ARRAY[string_to_array(
                        regexp_replace(
                            trim(lower(proposed_zoning_used_group)),
                            '[^\wa-z0-9,]+', '', 'g'
                        ), ','
                    )]
                ) AS a (e)
            ORDER BY e
        ))::text AS zug_prop,

        (CASE
            WHEN uselabel ~* 'Residential' THEN total_floor_area
        END)::numeric AS zsfr_prop,
        (CASE
            WHEN uselabel ~* 'Commercial' THEN total_floor_area
        END)::numeric AS zsfc_prop,
        (CASE
            WHEN uselabel ~* 'Community Facility' THEN total_floor_area
        END)::numeric AS zsfcf_prop,
        (CASE
            WHEN uselabel ~* 'Manufacturing' THEN total_floor_area
        END)::numeric AS zsfm_prop,
        no_of_parking_spaces::numeric + gc_numberofenclosedparkingspaces::numeric AS prkng_init,
        total_number_of_open_parking_space::numeric + total_number_of_enclosed_parking_space::numeric AS prking_prop,
        building_type AS bldg_class,
        CASE
            WHEN filing_status ~* 'Withdrawn' THEN 'W'
        END AS x_withdrawal,

        st_setsrid(st_point(
            longitude::double precision,
            latitude::double precision
        ), 4326) AS dob_geom
    FROM jobs_of_interest
),

/*
Work type values are coded and often delimited by forward slashes
e.g. "ST", "GC", "GC/MS", "SF/SH/FN"
*/
exploded_codes AS (
    SELECT DISTINCT
        work_type_codes,
        unnest(string_to_array(mapping_and_cleaning.work_type_codes, '/')) AS partial_code
    FROM mapping_and_cleaning
),

description_lists AS (
    SELECT
        work_type_codes,
        string_agg(
            coalesce(work_types_lookup.description, concat('UNKNOWN (', work_type_codes, ')')), '/'
            ORDER BY work_types_lookup.description ASC
        ) AS work_type_descriptions
    FROM exploded_codes
    LEFT JOIN work_types_lookup
        ON exploded_codes.partial_code = work_types_lookup.code
    GROUP BY
        work_type_codes
),

decode_work_types AS (
    SELECT
        mapping_and_cleaning.*,
        description_lists.work_type_descriptions AS work_types
    FROM mapping_and_cleaning
    LEFT JOIN description_lists ON mapping_and_cleaning.work_type_codes = description_lists.work_type_codes
),

missing_columns AS (
    SELECT
        *,
        NULL::numeric AS zoningsft_init,
        NULL::numeric AS zoningsft_prop,
        NULL::text AS zsf_init,
        NULL::text AS zsf_prop,
        NULL AS date_statusd,
        NULL AS zoningdist1,
        NULL AS zoningdist2,
        NULL AS zoningdist3,
        NULL AS owner_name,
        NULL AS constructnsf,
        NULL AS enlargementsf,
        NULL AS loftboardcert,
        NULL AS tracthomes,
        NULL AS desc_other
    FROM decode_work_types
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
INTO _init_now_devdb
FROM final;
