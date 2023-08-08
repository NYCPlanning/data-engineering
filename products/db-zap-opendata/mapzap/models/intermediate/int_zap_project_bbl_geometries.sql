with zap_project_bbls as (
    select *
    from
        {{ ref('int_zap_project_bbls') }}
),

pluto_geometries as (
    select *
    from
        {{ ref('stg_dcp__pluto_bbls') }}
),

seed_pluto_versions as (
    select * from {{ ref('seed_pluto_versions') }}
),

zap_pluto as (
    select
        zap_project_bbls.project_id,
        zap_project_bbls.project_name,
        zap_project_bbls.project_certified_referred_date,
        zap_project_bbls.project_certified_referred_year,
        zap_project_bbls.project_bbl,
        seed_pluto_versions.primary_pluto_version as pluto_version
    from
        zap_project_bbls
    left join
        seed_pluto_versions on
        zap_project_bbls.project_certified_referred_year
        = seed_pluto_versions.year
),

zap_project_bbl_geometries as (
    select
        zap_pluto.project_id,
        zap_pluto.project_name,
        zap_pluto.project_certified_referred_date,
        zap_pluto.project_certified_referred_year,
        zap_pluto.project_bbl,
        zap_pluto.pluto_version,
        ST_GEOGFROMTEXT(pluto_geometries.wkt) as bbl_geometry_wkt
    from
        zap_pluto
    left join
        pluto_geometries
        on
            zap_pluto.pluto_version
            = pluto_geometries.pluto_version
            and
            zap_pluto.project_bbl
            = pluto_geometries.bbl
)

select *
from
    zap_project_bbl_geometries
order by
    project_id desc,
    project_bbl asc
