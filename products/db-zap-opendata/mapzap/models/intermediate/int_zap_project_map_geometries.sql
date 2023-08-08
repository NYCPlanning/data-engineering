with projects_source as (
    select
        project_id,
        ulurp_numbers
    from
        {{ ref('stg_dcp__zap_projects') }}
),

map_amendments_source as (
    select *
    from
        {{ ref('stg_dcp__zoning_map_amendments') }}
),

project_ulurp_arrays as (
    select
        project_id,
        SPLIT(ulurp_numbers, ';') as ulurp_numbers_split
    from projects_source
    where ulurp_numbers is not null
),

project_ulurps_unnest as (
    select
        project_id,
        ulurp_number
    from project_ulurp_arrays
    cross join UNNEST(project_ulurp_arrays.ulurp_numbers_split) as ulurp_number
),

project_ulurps as (
    select
        project_id,
        UPPER(TRIM(ulurp_number)) as ulurp_number
    from project_ulurps_unnest
),

zap_project_map_amendments as (
    select
        project_ulurps.project_id,
        project_ulurps.ulurp_number,
        map_amendments_source.tracking_number,
        map_amendments_source.project_name,
        map_amendments_source.wkt
    from
        project_ulurps
    inner join
        map_amendments_source
        on
            project_ulurps.ulurp_number
            = map_amendments_source.ulurp_number
)

select *
from
    zap_project_map_amendments
order by
    project_id desc,
    ulurp_number asc
