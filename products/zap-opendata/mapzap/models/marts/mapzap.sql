with project_bbl_geometries as (
    select *
    from
        {{ ref('int_zap_project_bbl_geometries') }}
),

project_geometries_from_map as (
    select *
    from
        {{ ref('int_zap_project_map_geometries') }}
),

project_details as (
    select *
    from
        {{ ref('stg_dcp__zap_projects') }}
),

project_bbl_geometries_aggregated as (
    select
        project_id,
        pluto_version,
        SUM(ST_AREA(bbl_geometry_wkt)) as bbl_areas_sum,
        ARRAY_AGG(project_bbl) as project_bbls_array,
        ST_UNION_AGG(bbl_geometry_wkt) as project_geometry_wkt
    from project_bbl_geometries
    group by
        project_id,
        pluto_version
),

project_geometries_from_bbls as (
    select
        project_id,
        pluto_version,
        bbl_areas_sum,
        project_geometry_wkt as bbls_wkt,
        NULLIF(ARRAY_TO_STRING(project_bbls_array, '|'), '') as project_bbls
    from project_bbl_geometries_aggregated
),

project_geometries as (
    select
        project_geometries_from_bbls.project_id,
        project_geometries_from_bbls.pluto_version,
        project_geometries_from_bbls.bbl_areas_sum,
        project_geometries_from_bbls.project_bbls,
        project_geometries_from_bbls.bbls_wkt,
        project_geometries_from_map.ulurp_number as map_amendment_ulurp_numner,
        project_geometries_from_map.project_name as map_amendment_project_name,
        project_geometries_from_map.wkt as map_amendment_wkt
    from
        project_geometries_from_bbls
    left join
        project_geometries_from_map
        on
            project_geometries_from_bbls.project_id
            = project_geometries_from_map.project_id

),

project_geometries_resolved as (
    select
        project_details.dcp_name,
        project_details.project_id,
        project_details.project_name,
        project_details.project_certified_referred_date,
        project_details.project_certified_referred_year,
        project_details.applicant_type,
        project_details.ulurp_numbers,
        project_details.ulurp_type,
        project_details.ceqr_number,
        project_details.ceqr_type,
        project_details.project_status,
        project_details.public_status,
        project_details.action_codes,
        project_details.lead_division,
        project_details.fema_flood_zone_v,
        project_details.fema_flood_zone_coastal,
        project_details.wrp_review_required,
        project_details.current_zoning_district,
        project_details.proposed_zoning_district,
        project_geometries.map_amendment_ulurp_numner,
        project_geometries.map_amendment_project_name,
        project_geometries.pluto_version,
        project_geometries.bbl_areas_sum,
        project_geometries.project_bbls,
        project_geometries.map_amendment_wkt is not null
            as has_map_amendment_geometry,
        project_geometries.bbls_wkt is not null
            as has_bbls_geometry,
        case
            when
                project_geometries.map_amendment_wkt is not null
                then 'zoning map amendment'
            when project_geometries.bbls_wkt is not null then 'pluto bbls'
        end
            as geometry_source,
        COALESCE(
            project_geometries.map_amendment_wkt, project_geometries.bbls_wkt
        ) as wkt
    from
        project_details
    left join
        project_geometries on
        project_details.project_id = project_geometries.project_id

),

mapzap as (
    select
        *,
        ST_AREA(wkt) as project_area
    from
        project_geometries_resolved
)

select * from mapzap
