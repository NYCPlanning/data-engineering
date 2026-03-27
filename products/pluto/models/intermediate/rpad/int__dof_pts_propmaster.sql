-- Migrated from: pluto_build/sql/create_pts.sql
-- Transforms raw PTS (Property Tax System) data into dof_pts_propmaster format

{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['boro', 'tb', 'tl'], 'unique': False},
            {'columns': ['bbl'], 'unique': False}
        ]
    )
}}

WITH base_pts AS (
    SELECT
        boro,
        block AS tb,
        lot AS tl,
        parid AS bbl,
        street_name,
        housenum_lo,
        housenum_hi,
        aptno,
        zip_code AS zip,
        bldg_class AS bldgcl,
        ease,
        av_owner AS owner,
        REPLACE(land_area, '+', '')::double precision AS land_area,
        REPLACE(gross_sqft, '+', '')::double precision AS gross_sqft,
        REPLACE(residential_area_gross, '+', '')::double precision AS residarea,
        REPLACE(office_area_gross, '+', '')::double precision AS officearea,
        REPLACE(retail_area_gross, '+', '')::double precision AS retailarea,
        REPLACE(garage_area, '+', '')::double precision AS garagearea,
        REPLACE(storage_area_gross, '+', '')::double precision AS storagearea,
        REPLACE(factory_area_gross, '+', '')::double precision AS factoryarea,
        REPLACE(other_area_gross, '+', '')::double precision AS otherarea,
        REPLACE(num_bldgs, '+', '')::double precision AS bldgs,
        REPLACE(bld_story, '+', '')::double precision AS story,
        REPLACE(coop_apts, '+', '')::double precision AS coop_apts,
        REPLACE(units, '+', '')::double precision AS units,
        bld_ext AS ext,
        lot_irreg AS irreg,
        REPLACE(curactland, '+', '')::double precision AS curavl_act,
        REPLACE(curacttot, '+', '')::double precision AS curavt_act,
        REPLACE(curactextot, '+', '')::double precision AS curext_act,
        yrbuilt,
        yralt1,
        yralt2,
        condo_number,
        appt_boro AS ap_boro,
        appt_block AS ap_block,
        appt_lot AS ap_lot,
        appt_ease AS ap_ease,
        appt_date AS ap_date,
        ROUND(REPLACE(lot_frt, '+', '')::numeric, 2) AS lfft,
        ROUND(REPLACE(lot_dep, '+', '')::numeric, 2) AS ldft,
        ROUND(REPLACE(bld_frt, '+', '')::numeric, 2) AS bfft,
        ROUND(REPLACE(bld_dep, '+', '')::numeric, 2) AS bdft
    FROM {{ ref('stg__pluto_pts') }}
),

-- Add primebbl logic (from primebbl.sql)
with_primebbl AS (
    SELECT
        pts.*,
        COALESCE(
            CASE
                WHEN geo.billingbbl IS NOT NULL AND geo.billingbbl != '0000000000'
                THEN geo.billingbbl
            END,
            pts.boro || pts.tb || pts.tl
        ) AS primebbl
    FROM base_pts pts
    LEFT JOIN {{ ref('stg__pluto_input_geocodes') }} geo
        ON pts.boro || pts.tb || pts.tl = geo.borough || LPAD(geo.block, 5, '0') || LPAD(geo.lot, 4, '0')
)

SELECT * FROM with_primebbl
