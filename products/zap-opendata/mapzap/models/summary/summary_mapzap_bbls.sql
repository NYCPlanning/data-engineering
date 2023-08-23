with mapzap as (
    select * from {{ ref('mapzap') }}
),

project_geometries_counts as (
    select
        project_certified_referred_year,
        pluto_version,
        COUNT(*) as project_count,
        SUM(case when project_bbls is null then 1 else 0 end)
            as projects_with_no_bbl,
        SUM(case when wkt is null then 1 else 0 end)
            as projects_with_no_geometry
    from
        mapzap
    group by
        project_certified_referred_year,
        pluto_version
),

project_geometries_counts_2 as (
    select
        *,
        (projects_with_no_geometry - projects_with_no_bbl)
            as projects_with_bbl_and_no_geometry
    from
        project_geometries_counts

),

project_geometries_stats as (
    select
        *,
        ROUND(
            projects_with_no_bbl
            / project_count * 100, 2
        ) as percent_projects_with_no_bbl,
        ROUND(
            projects_with_no_geometry
            / project_count * 100, 2
        ) as percent_projects_with_no_geometry,
        ROUND(
            projects_with_bbl_and_no_geometry
            / (project_count - projects_with_no_bbl) * 100, 2
        ) as percent_projects_with_bbl_and_no_geometry
    from
        project_geometries_counts_2
),

project_geometries_stats_ordered as (
    select *
    from project_geometries_stats
)

select * from project_geometries_stats_ordered
order by
    project_certified_referred_year
