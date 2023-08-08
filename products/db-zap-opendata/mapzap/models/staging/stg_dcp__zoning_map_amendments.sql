with source as (

    select * from {{ source('zoning_map_amendments', '20230519_internal') }}

),

map_amendments_not_unique as (

    select
        project_na as project_name,
        ST_GEOGFROMTEXT(wkt, make_valid => TRUE) as wkt,
        UPPER(trackingno) as tracking_number,
        UPPER(ulurpno) as ulurp_number
    from source
),

map_amendments as (
    select
        ulurp_number,
        STRING_AGG(tracking_number, '|') as tracking_number,
        STRING_AGG(project_name, '|') as project_name,
        ST_UNION_AGG(wkt) as wkt
    from
        map_amendments_not_unique
    group by
        ulurp_number

)

select * from map_amendments
