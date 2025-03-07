/*
DESCRIPTION:
    
INPUT:
    doe_all_proposals.{version} (
        dbn character varying,
        main_building_id character varying,
        proposal_title character varying,
        other_impacted_building character varying,
        pep_vote character varying,
        approved character varying,
        at_scale_year character varying
    )

OUTPUT: 
    TEMP tmp (
        bldg_id character varying,
        org_id character varying,
        bldg_id_additional character varying,
        title character varying,
        at_scale_year character varying,
        school_year character varying,
        vote_date character varying
    )
*/

CREATE TEMP TABLE tmp as (
    SELECT
        main_building_id as bldg_id,
        RIGHT(dbn, 4) as org_id,
        other_impacted_building as bldg_id_additional,
        proposal_title as title,
        at_scale_year as at_scale_year,
        (CASE 
            WHEN EXTRACT(month from pep_vote::timestamp) < 9
                THEN (EXTRACT(year from pep_vote::timestamp) - 1)::text||
                    '-'||EXTRACT(year from pep_vote::timestamp)::text
            WHEN EXTRACT(month from pep_vote::timestamp) >= 9
                THEN EXTRACT(year from pep_vote::timestamp)::text||
                '-'||(EXTRACT(year from pep_vote::timestamp) + 1)::text
            ELSE NULL
        END) as school_year,
        NULLIF(
            regexp_replace(SPLIT_PART(at_scale_school_enrollment, '-', 1), '[^0-9]|\s', '', 'g'), 
            '')::integer as at_scale_enroll,
        pep_vote as vote_date
    FROM doe_all_proposals."2021/02/18"
    WHERE approved = 'Approved'
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;