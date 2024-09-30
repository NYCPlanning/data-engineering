WITH bis AS (
    SELECT * FROM {{ ref("int__jobs_bis_with_parking_spaces") }}
),

now AS (
    SELECT * FROM {{ ref("stg__jobs_now") }}
),

combined AS (
    SELECT * FROM bis
    UNION ALL
    SELECT * FROM NOW
)

SELECT
    job_number,
    job_type,
    job_desc,
    _occ_initial,
    _occ_proposed,

    CASE
        WHEN jobtype in ('Alteration', 'Demolition') AND stories_init = '0' THEN NULL
        ELSE stories_init
    END::numeric AS stories_init,
    CASE
        WHEN jobtype in ('Alteration', 'New Building') AND stories_prop = '0' THEN NULL
        ELSE stories_prop
    END::numeric AS stories_prop,

    zoningsft_init,
    NULL::numeric AS zoningsft_prop,

    (CASE
        WHEN jobtype ~* 'New Building' THEN 0
        WHEN existing_dwelling_units ~ '[^0-9]' THEN NULL
        ELSE existing_dwelling_units
    END)::numeric AS classa_init,

    (CASE
        WHEN jobtype ~* 'Demolition' THEN 0
        WHEN proposed_dwelling_units ~ '[^0-9]' THEN NULL
        ELSE proposed_dwelling_units
    END)::numeric AS classa_prop,

    filing_status AS _job_status,
    current_status_date AS date_lastupdt,
    filing_date AS date_filed,
    NULL AS date_statusd,
    "approved_(date)" AS date_statusp,
    permitissueddate AS date_statusr,
    signoffdate AS date_statusx,
    NULL AS zoningdist1,
    NULL AS zoningdist2,
    NULL AS zoningdist3,
    specialdistrict1 AS specialdist1,
    specialdistrict2 AS specialdist2,

    CASE
        WHEN landmark ~* 'L' THEN 'Yes'
    END AS landmark,

    NULL AS owner_name,
    owner_s_business_name AS owner_biznm,
    owner_s_street_name AS owner_address,
    zip AS owner_zipcode,
    ownerphone AS owner_phone,

    (CASE
        WHEN jobtype = 'Alteration'
            THEN nullif(existingbuildingheight, '0')
    END)::numeric AS height_init,
    (CASE
        WHEN jobtype = 'Alteration'
            THEN nullif(proposedbuildingheight, '0')
    END)::numeric AS height_prop,

    NULL AS constructnsf,

    CASE
        WHEN horizontalenlargement ~* 'true' AND NOT verticalenlargement ~* 'false'
            THEN 'Horizontal'
        WHEN NOT horizontalenlargement ~* 'false' AND verticalenlargement ~* 'true'
            THEN 'Vertical'
        WHEN horizontalenlargement ~* 'true' AND verticalenlargement ~* 'true'
            THEN 'Horizontal and Vertical'
    END AS enlargement,

    NULL AS enlargementsf,
    initial_cost::money::text AS costestimate,
    NULL AS loftboardcert,
    little_e AS edesignation,

    CASE
        WHEN worktypes = 'CC' THEN 'Yes'
    END AS curbcut,

    NULL AS tracthomes,
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
    NULL::text AS zsf_init,
    NULL::text AS zsf_prop,
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
    -- Requested enhancement from Housing with new columns from DOB source data
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
    NULL::numeric AS prkng_init,
    no_of_parking_spaces::numeric AS prkng_prop,
    building_type AS bldg_class,
    NULL AS desc_other,
    CASE
        WHEN filing_status ~* 'Withdrawn' THEN 'W'
    END AS x_withdrawal,
    st_setsrid(st_point(
        longitude::double precision,
        latitude::double precision
    ), 4326) AS dob_geom
INTO _init_now_devdb
FROM dob_now_applications
WHERE
    (jobtype IN ('New Building', 'Full Demolition') OR jobtype ~* 'CO')
    AND right(job_filing_number, 2) = 'I1';
