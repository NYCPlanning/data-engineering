WITH dob_bis_applications AS (
    SELECT * FROM {{ ref("sources.dob_jobapplications") }}
)

SELECT
    jobnumber AS job_number,

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

    replace(existingoccupancy, '.', '') AS _occ_initial,
    replace(proposedoccupancy, '.', '') AS _occ_proposed,

    CASE
        WHEN jobtype ~* 'A1|DM'
            THEN nullif(existingnumstories, '0')::numeric
    END AS stories_init,
    CASE
        WHEN jobtype ~* 'A1|NB'
            THEN nullif(proposednumstories, '0')::numeric
    END AS stories_prop,

    CASE
        WHEN jobtype ~* 'A1|DM'
            THEN nullif(existingzoningsqft, '0')::numeric
        ELSE existingzoningsqft::numeric
    END AS zoningsft_init,
    CASE
        WHEN jobtype ~* 'A1|DM'
            THEN nullif(proposedzoningsqft, '0')::numeric
        ELSE proposedzoningsqft::numeric
    END AS zoningsft_prop,

    CASE
        WHEN jobtype ~* 'NB' THEN 0
        WHEN existingdwellingunits ~ '[^0-9]' THEN NULL
        ELSE existingdwellingunits::numeric
    END AS classa_init,
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

    -- Add dummy columns for union to now applications for _init_devdb
    existingzoningsqft AS zsf_init,
    proposedzoningsqft AS zsf_prop,
    NULL::text AS zug_init,
    NULL::text AS zug_prop,
    NULL::numeric AS zsfr_prop,
    NULL::numeric AS zsfc_prop,
    NULL::numeric AS zsfcf_prop,
    NULL::numeric AS zsfm_prop,
    -- End Dummy columns 
    
    buildingclass AS bldg_class,
    otherdesc AS desc_other,
    specialactionstatus AS x_withdrawal,
    st_setsrid(st_point(
        longitude::double precision,
        latitude::double precision
    ), 4326) AS dob_geom
FROM dob_bis_applications
WHERE
    jobdocnumber = '01' AND (
        jobtype ~* 'A1|DM|NB' OR (
            jobtype = 'A2'
            AND sprinkler IS NULL
            AND lower(jobdescription) LIKE '%combin%'
            AND lower(jobdescription) NOT LIKE '%sprinkler%'
        )
    ) AND gid = 1;
