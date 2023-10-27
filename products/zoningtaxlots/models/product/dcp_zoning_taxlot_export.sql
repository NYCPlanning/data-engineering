-- export zoning tax lot database with desired headings
with dcp_zoning_taxlot as (
    select *
    from {{ ref('dcp_zoning_taxlot') }}
),

dcp_zoning_taxlot_export as (
    select
        boroughcode as "Borough Code",
        taxlot as "Tax Lot",
        bbl as "BBL",
        zoningdistrict1 as "Zoning District 1",
        zoningdistrict2 as "Zoning District 2",
        zoningdistrict3 as "Zoning District 3",
        zoningdistrict4 as "Zoning District 4",
        commercialoverlay1 as "Commercial Overlay 1",
        commercialoverlay2 as "Commercial Overlay 2",
        specialdistrict1 as "Special District 1",
        specialdistrict2 as "Special District 2",
        specialdistrict3 as "Special District 3",
        limitedheightdistrict as "Limited Height District",
        zoningmapnumber as "Zoning Map Number",
        zoningmapcode as "Zoning Map Code",
        trunc(taxblock::numeric) as "Tax Block"
    from
        dcp_zoning_taxlot
)

select * from dcp_zoning_taxlot_export
