-- Migrated from: pluto_build/sql/create_rpad_geo.sql
-- Also incorporates logic from:
--   - zerovacantlots.sql
--   - lotarea.sql
--   - primebbl.sql
--   - apdate.sql
--   - geocode_billingbbl.sql
--
-- Joins DOF property tax data with DCP geocodes
-- This is the critical intermediate table that feeds into PLUTO

{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['bbl'], 'unique': False},
            {'columns': ['primebbl'], 'unique': False}
        ]
    )
}}

WITH

{% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
-- Dev mode: Sample 20 BBLs per borough for fast iteration
dev_sample_bbls AS (
    SELECT boro || tb || tl AS bbl_key
    FROM (
        SELECT DISTINCT boro, tb, tl,
               ROW_NUMBER() OVER (PARTITION BY boro ORDER BY RANDOM()) AS rn
        FROM {{ ref('int__dof_pts_propmaster') }}
    ) sub
    WHERE rn <= 20
),
{% endif %}

-- Prepare geocodes with coordinate transformations
geocodes_prepared AS (
    SELECT
        *,
        -- Compute coordinates from geometry as text (matching original behavior)
        ST_X(ST_TRANSFORM(geom, 2263))::integer::text AS xcoord_calc,
        ST_Y(ST_TRANSFORM(geom, 2263))::integer::text AS ycoord_calc,
        -- Handle ct2010 = 0 case
        CASE WHEN ct2010::numeric = 0 THEN NULL ELSE ct2010 END AS ct2010_fixed
    FROM {{ ref('stg__pluto_input_geocodes') }}
),

-- Deduplicate PTS records by BBL, keeping the best one
pluto_rpad_deduped AS (
    SELECT
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY boro || tb || tl
            ORDER BY curavt_act DESC, land_area DESC, ease ASC
        ) AS row_number
    FROM {{ ref('int__dof_pts_propmaster') }} AS a
    {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
    INNER JOIN dev_sample_bbls s
        ON a.boro || a.tb || a.tl = s.bbl_key
    {% endif %}
),

pluto_rpad_single AS (
    SELECT *
    FROM pluto_rpad_deduped
    WHERE row_number = 1
),

-- Join PTS data with geocodes
base_join AS (
    SELECT
        a.*,
        -- Include all geocode fields except geo_bbl (avoid name collision)
        b.billingbbl,
        b.cd,
        b.ct2010_fixed AS ct2010,
        b.cb2010,
        b.ct2020,
        b.cb2020,
        b.schooldist,
        b.council,
        b.zipcode,
        b.firecomp,
        b.policeprct,
        b.healthcenterdistrict,
        b.healtharea,
        b.sanitdistrict,
        b.sanitsub,
        b.boepreferredstreetname,
        b.taxmap,
        b.sanbornmapidentifier,
        b.latitude,
        b.longitude,
        b.grc,
        b.grc2,
        b.msg,
        b.msg2,
        b.borough,
        b.block,
        b.lot,
        b.easement,
        b.input_hnum,
        b.input_sname,
        -- Rename to match expected column name from original SQL
        b.numberofexistingstructures AS numberofexistingstructuresonlot,
        b.geom,
        b.ogc_fid,
        b.data_library_version,
        b.xcoord_calc AS xcoord_geo,
        b.ycoord_calc AS ycoord_geo
    FROM pluto_rpad_single AS a
    LEFT JOIN geocodes_prepared AS b
        ON a.boro || a.tb || a.tl = b.borough || LPAD(b.block, 5, '0') || LPAD(b.lot, 4, '0')
),

-- Incorporate zerovacantlots.sql logic
-- Zero out building metrics for vacant lots
with_vacant_adjustments AS (
    SELECT
        *,
        CASE
            WHEN curavl_act = curavt_act AND UPPER(bldgcl) LIKE 'V%'
            THEN 0
            ELSE bfft
        END AS bfft_adj,
        CASE
            WHEN curavl_act = curavt_act AND UPPER(bldgcl) LIKE 'V%'
            THEN 0
            ELSE bdft
        END AS bdft_adj,
        CASE
            WHEN curavl_act = curavt_act AND UPPER(bldgcl) LIKE 'V%'
            THEN 0
            ELSE story
        END AS story_adj,
        CASE
            WHEN curavl_act = curavt_act AND UPPER(bldgcl) LIKE 'V%'
            THEN 0
            ELSE bldgs
        END AS bldgs_adj
    FROM base_join
),

-- Incorporate lotarea.sql logic
-- Calculate lot area from frontage x depth when missing
with_lotarea_calculated AS (
    SELECT
        *,
        CASE
            WHEN (land_area IS NULL OR land_area = 0)
                AND irreg != 'I'
                AND lfft > 0
                AND ldft > 0
            THEN lfft * ldft
            ELSE land_area
        END AS land_area_calc
    FROM with_vacant_adjustments
),

-- Incorporate primebbl.sql logic
-- Assign prime BBL (for condos, use billingbbl; otherwise use own BBL)
with_primebbl AS (
    SELECT
        *,
        CASE
            WHEN billingbbl IS NOT NULL AND billingbbl != '0000000000'
            THEN billingbbl
            ELSE boro || tb || tl
        END AS primebbl_calc
    FROM with_lotarea_calculated
),

-- Incorporate geocode_billingbbl.sql logic
-- Parse billing block and lot from billingbbl
with_billing_parsed AS (
    SELECT
        *,
        CASE
            WHEN billingbbl IS NOT NULL
                AND billingbbl != '0000000000'
                AND billingbbl != 'none'
            THEN SUBSTRING(billingbbl, 2, 5)
        END AS billingblock,
        CASE
            WHEN billingbbl IS NOT NULL
                AND billingbbl != '0000000000'
                AND billingbbl != 'none'
            THEN RIGHT(billingbbl, 4)
        END AS billinglot
    FROM with_primebbl
),

-- Incorporate apdate.sql logic
-- Format ap_date from MM/DD/YY to MM/DD/YYYY
with_apdate_formatted AS (
    SELECT
        *,
        CASE
            WHEN ap_date IS NOT NULL
            THEN to_char(to_date(ap_date, 'MM/DD/YY'), 'MM/DD/YYYY')
        END AS ap_datef
    FROM with_billing_parsed
),

-- Calculate final BBL and backfill coordinates from lat/long if needed
final AS (
    SELECT
        -- Calculated/formatted fields
        boro || LPAD(tb, 5, '0') || LPAD(tl, 4, '0') AS bbl,
        primebbl_calc AS primebbl,
        ap_datef,
        billingblock,
        billinglot,
        
        -- Backfill xcoord/ycoord from latitude/longitude if geocode didn't have them
        -- Keep as text to match pluto table expectations
        COALESCE(
            xcoord_geo,
            CASE
                WHEN longitude IS NOT NULL
                THEN ST_X(ST_TRANSFORM(
                    ST_SETSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision), 4326),
                    2263
                ))::integer::text
            END
        ) AS xcoord,
        COALESCE(
            ycoord_geo,
            CASE
                WHEN latitude IS NOT NULL
                THEN ST_Y(ST_TRANSFORM(
                    ST_SETSRID(ST_MAKEPOINT(longitude::double precision, latitude::double precision), 4326),
                    2263
                ))::integer::text
            END
        ) AS ycoord,
        
        -- Adjusted fields
        bfft_adj AS bfft,
        bdft_adj AS bdft,
        story_adj AS story,
        bldgs_adj AS bldgs,
        land_area_calc AS land_area,
        
        -- All other PTS fields (from int__dof_pts_propmaster)
        boro,
        tb,
        tl,
        street_name,
        housenum_lo,
        housenum_hi,
        aptno,
        zip,
        bldgcl,
        ease,
        owner,
        gross_sqft,
        residarea,
        officearea,
        retailarea,
        garagearea,
        storagearea,
        factoryarea,
        otherarea,
        coop_apts,
        units,
        ext,
        irreg,
        curavl_act,
        curavt_act,
        curext_act,
        yrbuilt,
        yralt1,
        yralt2,
        condo_number,
        ap_boro,
        ap_block,
        ap_lot,
        ap_ease,
        ap_date,
        lfft,
        ldft,
        
        -- All geocode fields
        billingbbl,
        cd,
        ct2010,
        cb2010,
        ct2020,
        cb2020,
        schooldist,
        council,
        zipcode,
        firecomp,
        policeprct,
        healthcenterdistrict,
        healtharea,
        sanitdistrict,
        sanitsub,
        boepreferredstreetname,
        taxmap,
        sanbornmapidentifier,
        latitude,
        longitude,
        grc,
        grc2,
        msg,
        msg2,
        borough,
        block,
        lot,
        easement,
        input_hnum,
        input_sname,
        numberofexistingstructuresonlot,
        geom,
        ogc_fid,
        data_library_version
    FROM with_apdate_formatted
)

SELECT * FROM final
