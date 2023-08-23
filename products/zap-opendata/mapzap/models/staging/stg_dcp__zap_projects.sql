with source as (

    select * from {{ source('zap_projects', '20230607_test_recoded') }}

),

zap_projects as (

    select
        project_id as dcp_name,
        crm_project_id as project_id,
        project_name,
        certified_referred as project_certified_referred_date,
        applicant_type,
        ulurp_numbers,
        ulurp_non as ulurp_type,
        ceqr_number,
        ceqr_type,
        project_status,
        public_status,
        actions as action_codes,
        lead_division,
        fema_flood_zone_v,
        fema_flood_zone_coastal,
        wrp_review_required,
        current_zoning_district,
        proposed_zoning_district,
        EXTRACT(
            isoyear
            from
            certified_referred
        ) as project_certified_referred_year

    from source
)

select * from zap_projects
