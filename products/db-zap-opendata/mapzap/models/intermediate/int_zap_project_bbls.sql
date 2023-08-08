with projects_source as (
    select
        project_id,
        project_name,
        project_certified_referred_date,
        project_certified_referred_year
    from
        {{ ref('stg_dcp__zap_projects') }}
),

bbls_source as (
    select *
    from
        {{ ref('stg_dcp__zap_bbls') }}
),

zap_project_bbls as (
    select
        projects_source.project_id,
        projects_source.project_name,
        projects_source.project_certified_referred_date,
        projects_source.project_certified_referred_year,
        bbls_source.project_bbl as project_bbl
    from
        projects_source
    left join
        bbls_source
        on
            projects_source.project_id
            = bbls_source.project_id
)

select *
from
    zap_project_bbls
order by
    project_id desc,
    project_bbl asc
