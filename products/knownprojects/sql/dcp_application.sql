/*
DESCRIPTION:


INPUTS:
	dcp_projects
	dcp_projectactions
    	dcp_project_bbls
    	dcp_mappluto_wi
    	dcp_knownprojects
    	corrections_main
		zap_record_ids
OUTPUTS:
	dcp_application
*/
DROP TABLE IF EXISTS _dcp_application;
WITH
zap_translated AS (
    SELECT
        dcp_name,
        dcp_projectname,
        dcp_projectbrief,
        dcp_projectdescription,
        (CASE
            WHEN dcp_applicanttype::numeric = 717170000 THEN 'DCP'
            WHEN
                dcp_applicanttype::numeric = 717170001
                THEN 'Other Public Agency'
            WHEN dcp_applicanttype::numeric = 717170002 THEN 'Private'
        END) AS dcp_applicanttype,
        (CASE
            WHEN dcp_visibility::numeric = 717170002 THEN 'Applicant Only'
            WHEN dcp_visibility::numeric = 717170001 THEN 'CPC Only'
            WHEN dcp_visibility::numeric = 717170003 THEN 'General Public'
            WHEN dcp_visibility::numeric = 717170000 THEN 'Internal DCP Only'
            WHEN dcp_visibility::numeric = 717170004 THEN 'LUP'
        END) AS dcp_visibility,
        (CASE
            WHEN dcp_borough::numeric = 717170000 THEN 'Bronx'
            WHEN dcp_borough::numeric = 717170002 THEN 'Brooklyn'
            WHEN dcp_borough::numeric = 717170001 THEN 'Manhattan'
            WHEN dcp_borough::numeric = 717170004 THEN 'Staten Island'
            WHEN dcp_borough::numeric = 717170005 THEN 'Citywide'
            WHEN dcp_borough::numeric = 717170003 THEN 'Queens'
        END) AS dcp_borough,
        (CASE
            WHEN statuscode::numeric = 1 THEN 'Active'
            WHEN statuscode::numeric = 717170000 THEN 'On-Hold'
            WHEN statuscode::numeric = 707070003 THEN 'Record Closed'
            WHEN statuscode::numeric = 707070000 THEN 'Complete'
            WHEN statuscode::numeric = 707070002 THEN 'Terminated'
            WHEN
                statuscode::numeric = 707070001
                THEN 'Withdrawn-Applicant Unresponsive'
            WHEN statuscode::numeric = 717170001 THEN 'Withdrawn-Other'
        END) AS statuscode,
        (CASE
            WHEN dcp_publicstatus::numeric = 717170000 THEN 'Filed'
            WHEN dcp_publicstatus::numeric = 717170001 THEN 'In Public Review'
            WHEN dcp_publicstatus::numeric = 717170002 THEN 'Completed'
            WHEN dcp_publicstatus::numeric = 717170005 THEN 'Noticed'
        END) AS dcp_publicstatus,
        (CASE
            WHEN dcp_projectphase::numeric = 717170000 THEN 'Study'
            WHEN dcp_projectphase::numeric = 717170001 THEN 'Pre-Pas'
            WHEN dcp_projectphase::numeric = 717170002 THEN 'Pre-Cert'
            WHEN dcp_projectphase::numeric = 717170003 THEN 'Public Review'
            WHEN dcp_projectphase::numeric = 717170004 THEN 'Public Completed'
            WHEN dcp_projectphase::numeric = 717170005 THEN 'Initiation'
        END) AS dcp_projectphase,
        (CASE
            WHEN dcp_projectcompleted IS NULL THEN NULL
            ELSE TO_CHAR(dcp_projectcompleted::timestamp, 'YYYY/MM/DD')
        END) AS dcp_projectcompleted,
        (CASE
            WHEN
                COALESCE(
                    dcp_certifiedreferred, dcp_dcptargetcertificationdate
                ) IS NULL
                THEN NULL
            ELSE
                TO_CHAR(
                    COALESCE(
                        dcp_certifiedreferred, dcp_dcptargetcertificationdate
                    )::timestamp,
                    'YYYY/MM/DD'
                )
        END) AS dcp_certifiedreferred,
        COALESCE(
            dcp_totalnoofdusinprojecd::numeric, 0
        ) AS dcp_totalnoofdusinprojecd,
        COALESCE(dcp_mihdushighernumber::numeric, 0) AS dcp_mihdushighernumber,
        COALESCE(dcp_mihduslowernumber::numeric, 0) AS dcp_mihduslowernumber,
        COALESCE(
            dcp_numberofnewdwellingunits::numeric, 0
        ) AS dcp_numberofnewdwellingunits,
        COALESCE(
            dcp_noofvoluntaryaffordabledus::numeric, 0
        ) AS dcp_noofvoluntaryaffordabledus,
        COALESCE(dcp_residentialsqft::numeric, 0) AS dcp_residentialsqft
    FROM dcp_projects
),

-- we exclude projects that have record closed, terminated or withdrawn as status
status_filter AS (
    SELECT dcp_name
    FROM zap_translated
    WHERE statuscode !~* 'Record Closed|Terminated|Withdrawn'
),

-- we include projects have any clear indication of residential components
resid_units_filter AS (
    SELECT dcp_name
    FROM zap_translated
    WHERE
        dcp_residentialsqft > 0
        OR dcp_totalnoofdusinprojecd > 0
        OR dcp_mihdushighernumber > 0
        OR dcp_mihduslowernumber > 0
        OR dcp_numberofnewdwellingunits > 0
        OR dcp_noofvoluntaryaffordabledus > 0
),

--all projects that are after 2010, and NULLs included
year_filter AS (
    SELECT dcp_name
    FROM zap_translated
    WHERE (
        EXTRACT(YEAR FROM dcp_projectcompleted::date) >= 2010
        OR EXTRACT(YEAR FROM dcp_certifiedreferred::date) >= 2010
        OR (dcp_projectcompleted IS NULL AND dcp_certifiedreferred IS NULL)
    )
),

records_last_kpdb AS (
    SELECT record_id AS dcp_name
    FROM dcp_knownprojects
    WHERE source = 'DCP Application'
),

records_corr_add AS (
    -- SELECT record_id as dcp_name
    -- FROM corrections_zap
    -- WHERE lower(action) = 'add'

    -- Everything in zap_record_ids -> add
    SELECT record_id AS dcp_name
    FROM zap_record_ids
),

records_corr_remove AS (
    -- SELECT record_id as dcp_name
    -- FROM corrections_zap
    -- WHERE lower(action) = 'remove'

    -- Everything not in zap_record_ids -> remove
    SELECT dcp_name FROM zap_translated
    WHERE dcp_name NOT IN (
        SELECT dcp_name FROM records_corr_add
    )
),

consolidated_add_filter AS (
    /*
    Add if a record satisfies:
    1) flagged as add in
    2) or ( flag_year = 1 and flag_status = 1 )
    */
    SELECT DISTINCT dcp_name FROM zap_translated
    WHERE
        dcp_name IN (SELECT dcp_name FROM status_filter)
        AND dcp_name IN (SELECT dcp_name FROM year_filter)
    UNION
    SELECT dcp_name FROM records_corr_add
),

consolidated_remove_filter AS (
    /*
    Remove if a record satisfies:
    1) flagged as remove
    2) or not in consolidated_add_filter
    */
    SELECT dcp_name FROM records_corr_remove
    UNION
    SELECT dcp_name AS dcp_name FROM zap_translated
    WHERE dcp_name NOT IN (SELECT dcp_name FROM consolidated_add_filter)
),

relevant_projects AS (
    SELECT DISTINCT dcp_name
    FROM zap_translated
    WHERE
        dcp_name IN (SELECT dcp_name FROM consolidated_add_filter)
        AND dcp_name NOT IN (SELECT dcp_name FROM consolidated_remove_filter)
)

SELECT DISTINCT
--descriptor fields
    'DCP Application' AS source,
    dcp_name AS record_id,

    dcp_projectname AS record_name,

    dcp_projectbrief,

    dcp_projectdescription,

    dcp_borough AS borough,

    statuscode,

    dcp_publicstatus AS publicstatus,

    dcp_certifiedreferred,
    dcp_applicanttype AS applicanttype,
    dcp_visibility AS visibility,
    dcp_numberofnewdwellingunits,
    dcp_totalnoofdusinprojecd,
    dcp_mihdushighernumber,
    dcp_mihduslowernumber,
    dcp_noofvoluntaryaffordabledus,
    dcp_residentialsqft,
    COALESCE(
        NULLIF(dcp_numberofnewdwellingunits, 0),
        NULLIF(dcp_totalnoofdusinprojecd, 0),
        NULLIF(
            dcp_mihdushighernumber
            + dcp_noofvoluntaryaffordabledus, 0
        ),
        NULLIF(
            dcp_mihduslowernumber
            + dcp_noofvoluntaryaffordabledus, 0
        )
    )::numeric AS units_gross,
    (CASE WHEN dcp_name IN (
        SELECT DISTINCT dcp_name
        FROM relevant_projects
    ) THEN 1
    ELSE 0 END) AS flag_relevant,

    --units fields
    (CASE WHEN dcp_name IN (
        SELECT DISTINCT dcp_name
        FROM year_filter
    ) THEN 1
    ELSE 0 END) AS flag_year,
    (CASE WHEN dcp_name IN (
        SELECT DISTINCT dcp_name
        FROM status_filter
    ) THEN 1
    ELSE 0 END) AS flag_status,
    (CASE WHEN dcp_name IN (
        SELECT DISTINCT dcp_name
        FROM resid_units_filter
    ) THEN 1
    ELSE 0 END) AS flag_resid_units,
    (CASE
        WHEN
            dcp_name IN (
                SELECT dcp_name FROM records_last_kpdb
            )
            THEN 1
        ELSE 0
    END) AS flag_in_last_kpdb,
    (CASE
        WHEN
            dcp_name NOT IN (
                SELECT dcp_name FROM records_last_kpdb
            )
            THEN 1
        ELSE 0
    END) AS flag_not_in_last_kpdb,
    (CASE
        WHEN
            dcp_name IN (SELECT dcp_name FROM records_corr_remove)
            THEN 'remove'
        WHEN dcp_name IN (SELECT dcp_name FROM records_corr_add) THEN 'add'
    END) AS flag_corrected,

    --calculate units_gross
    (CASE
        WHEN
            dcp_projectphase ~* 'project completed'
            THEN 'DCP 4: Zoning Implemented'
        WHEN
            dcp_projectphase ~* 'pre-pas|pre-cert'
            THEN 'DCP 2: Application in progress'
        WHEN
            dcp_projectphase ~* 'initiation'
            THEN 'DCP 1: Expression of interest'
        WHEN
            dcp_projectphase ~* 'public review'
            THEN 'DCP 3: Certified/Referred'
    END) AS status,

    --identify unit source
    COALESCE(
        (CASE
            WHEN NULLIF(dcp_numberofnewdwellingunits, 0) IS NOT NULL
                THEN 'dcp_numberofnewdwellingunits'
        END),
        (CASE
            WHEN NULLIF(dcp_totalnoofdusinprojecd, 0) IS NOT NULL
                THEN 'dcp_totalnoofdusinprojecd'
        END),
        (CASE
            WHEN
                NULLIF(
                    dcp_mihdushighernumber + dcp_noofvoluntaryaffordabledus, 0
                ) IS NOT NULL
                THEN 'dcp_mihdushighernumber + dcp_noofvoluntaryaffordabledus'
        END),
        (CASE
            WHEN
                NULLIF(
                    dcp_mihduslowernumber + dcp_noofvoluntaryaffordabledus, 0
                ) IS NOT NULL
                THEN 'dcp_mihduslowernumber + dcp_noofvoluntaryaffordabledus'
        END)
    )
    AS units_gross_source
INTO _dcp_application
FROM zap_translated;

DROP TABLE IF EXISTS dcp_application;
WITH
-- Assigning Geometry Using BBL
geom_pluto AS (
    SELECT
        a.record_id,
        ST_UNION(b.wkb_geometry) AS geom
    FROM (
        SELECT
            a.record_id,
            b.dcp_bblnumber AS bbl
        FROM _dcp_application AS a
        LEFT JOIN dcp_projectbbls AS b
            ON a.record_id = TRIM(SPLIT_PART(b.dcp_name, '-', 1))
        WHERE b.statuscode != '2'
    ) AS a LEFT JOIN dcp_mappluto_wi AS b
        ON a.bbl::numeric = b.bbl::numeric
    GROUP BY a.record_id
),

-- Assigning Geometry Using Previous version of KPDB
geom_kpdb AS (
    SELECT
        a.record_id,
        CASE
            WHEN b.geometry::geometry IS NULL
                THEN a.geom
            ELSE b.geometry::geometry
        END AS geom
    FROM geom_pluto AS a
    LEFT JOIN dcp_knownprojects AS b
        ON a.record_id = b.record_id
),

-- Assigning Geometry Using Zoning Map Amendments
geom_ulurp AS (
    SELECT
        a.record_id,
        CASE
            WHEN ST_UNION(b.wkb_geometry) IS NULL
                THEN a.geom
        END AS geom
    FROM (
        SELECT
            a.record_id,
            a.geom,
            b.dcp_ulurpnumber
        FROM (
            SELECT
                a.record_id,
                a.geom,
                b.dcp_projectid
            FROM geom_kpdb AS a
            LEFT JOIN dcp_projects AS b
                ON a.record_id = b.dcp_name
        ) AS a LEFT JOIN dcp_projectactions AS b
            ON a.dcp_projectid = b._dcp_project_value
    ) AS a LEFT JOIN dcp_zoningmapamendments AS b
        ON a.dcp_ulurpnumber = b.ulurpno
    GROUP BY a.record_id, a.geom
)

-- Main table with the geometry lookup
SELECT
    a.*,
    b.geom
INTO dcp_application
FROM _dcp_application AS a
LEFT JOIN geom_ulurp AS b
    ON a.record_id = b.record_id;
