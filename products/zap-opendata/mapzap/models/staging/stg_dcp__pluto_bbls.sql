with pluto_with_bbls as (
    select * from {{ ref('base_dcp__pluto_with_bbls') }}
),

pluto_without_bbls as (
    select * from {{ ref('base_dcp__pluto_without_bbls') }}
),

seed_pluto_versions as (
    select * from {{ ref('seed_pluto_versions') }}
),

pluto_without_bbls_padded_strings as (
    select
        pluto_version,
        borocode,
        wkt,
        LPAD(block, 5, '0') as block,
        LPAD(lot, 4, '0') as lot
    from
        pluto_without_bbls
),

pluto_constructed_bbls as (
    select
        pluto_version,
        borocode,
        block,
        lot,
        wkt,
        CONCAT(borocode, block, lot) as bbl
    from
        pluto_without_bbls_padded_strings
),

pluto_bbls_all as (
    select
        pluto_version,
        bbl,
        wkt
    from
        pluto_with_bbls
    union all
    select
        pluto_version,
        bbl,
        wkt
    from
        pluto_constructed_bbls
),

pluto_bbls as (
    select
        pluto_bbls_all.pluto_version,
        seed_pluto_versions.year as pluto_year,
        pluto_bbls_all.bbl,
        pluto_bbls_all.wkt
    from pluto_bbls_all left join seed_pluto_versions on
        pluto_bbls_all.pluto_version
        = seed_pluto_versions.primary_pluto_version
)

select * from pluto_bbls
