{{ config(materialized='table', tags=['review']) }}

-- Every ZAP project with any milestone in the last three years, whether or not
-- it made it into KPDB, so reviewers can spot projects to add to zap_record_ids.

-- whole calendar years, so the window only moves when the build year does
WITH cutoff AS (
    SELECT MAKE_DATE(EXTRACT(YEAR FROM CURRENT_DATE)::int - 3, 1, 1) AS cutoff_date
),

projects AS (
    SELECT
        *,
        -- GREATEST ignores NULLs, so a project qualifies on any one recent date
        GREATEST(
            app_filed_date::date,
            noticed_date::date,
            certified_referred::date,
            approval_date::date,
            completed_date::date,
            current_milestone_date::date
        ) AS latest_date
    FROM {{ source('recipe_sources', 'dcp_projects') }}
),

zap_record_ids AS (
    SELECT DISTINCT record_id FROM {{ source('corrections', 'zap_record_ids') }}
),

kpdb AS (
    SELECT DISTINCT record_id FROM {{ ref('kpdb') }}
    WHERE source = 'DCP Application'
),

removed_by_correction AS (
    SELECT DISTINCT record_id FROM {{ source('corrections', 'corrections_main') }}
    WHERE field = 'remove'
),

no_geometry AS (
    SELECT DISTINCT record_id FROM {{ source('build_tables', 'review_no_geometry') }}
    WHERE source = 'DCP Application'
)

SELECT
    projects.project_id,
    projects.latest_date,
    -- lists every column that ties for latest, e.g. a completion that's also
    -- the current milestone
    CONCAT_WS(
        ', ',
        CASE WHEN projects.app_filed_date::date = projects.latest_date THEN 'app_filed_date' END,
        CASE WHEN projects.noticed_date::date = projects.latest_date THEN 'noticed_date' END,
        CASE
            WHEN projects.certified_referred::date = projects.latest_date THEN 'certified_referred'
        END,
        CASE WHEN projects.approval_date::date = projects.latest_date THEN 'approval_date' END,
        CASE WHEN projects.completed_date::date = projects.latest_date THEN 'completed_date' END,
        CASE
            WHEN projects.current_milestone_date::date = projects.latest_date THEN 'current_milestone_date'
        END
    ) AS latest_date_source,
    projects.current_milestone,
    COALESCE(projects.app_filed_date::date >= cutoff.cutoff_date, FALSE) AS filed_in_last_three_years,
    zap_record_ids.record_id IS NOT NULL AS in_zap_record_ids,
    kpdb.record_id IS NOT NULL AS in_kpdb,
    CASE
        WHEN zap_record_ids.record_id IS NULL OR kpdb.record_id IS NOT NULL THEN NULL
        WHEN removed_by_correction.record_id IS NOT NULL THEN 'removed by correction'
        WHEN no_geometry.record_id IS NOT NULL THEN 'no geometry'
        ELSE 'other'
    END AS not_in_kpdb_reason,
    projects.project_name,
    projects.crm_project_id,
    projects.project_brief,
    projects.dcp_projectdescription,
    projects.dcp_visibility,
    projects.lead_division,
    projects.fema_flood_zone_v,
    projects.fema_flood_zone_coastal,
    projects.wrp_review_required,
    projects.current_zoning_district,
    projects.proposed_zoning_district,
    projects.project_status,
    projects.public_status,
    projects.ulurp_non,
    projects.actions,
    projects.ulurp_numbers,
    projects.ceqr_type,
    projects.ceqr_number,
    projects.eas_eis,
    projects.ceqr_leadagency,
    projects.primary_applicant,
    projects.applicant_type,
    projects.borough,
    projects.community_district,
    projects.cc_district,
    projects.flood_zone_a,
    projects.flood_zone_shadedx,
    projects.current_milestone_date,
    projects.current_envmilestone,
    projects.current_envmilestone_date,
    projects.app_filed_date,
    projects.noticed_date,
    projects.certified_referred,
    projects.approval_date,
    projects.completed_date,
    projects.mih_flag,
    projects.mih_option1,
    projects.mih_option2,
    projects.mih_workforce,
    projects.mih_deepaffordability,
    projects.mih_mapped_no_res,
    projects.dcp_projectphase,
    projects.dcp_residentialsqft,
    projects.dcp_totalnoofdusinprojecd,
    projects.dcp_dcptargetcertificationdate,
    projects.dcp_mihdushighernumber,
    projects.dcp_mihduslowernumber,
    projects.dcp_numberofnewdwellingunits,
    projects.dcp_noofvoluntaryaffordabledus,
    projects.data_library_version
FROM projects
CROSS JOIN cutoff
LEFT JOIN zap_record_ids ON projects.project_id = zap_record_ids.record_id
LEFT JOIN kpdb ON projects.project_id = kpdb.record_id
LEFT JOIN removed_by_correction ON projects.project_id = removed_by_correction.record_id
LEFT JOIN no_geometry ON projects.project_id = no_geometry.record_id
WHERE projects.latest_date >= cutoff.cutoff_date
