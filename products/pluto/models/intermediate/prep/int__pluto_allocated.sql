-- Migrated from: pluto_build/sql/create_allocated.sql
-- Also incorporates: pluto_build/sql/yearbuiltalt.sql
-- Creates the allocated table by aggregating condo data from int__pluto_rpad_geo
-- This table is used to populate ~25 fields in the base PLUTO table

{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['bbl'], 'unique': True}
        ]
    )
}}

WITH

{% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
-- Dev mode: Sample BBLs for fast iteration
dev_sample_bbls AS (
    SELECT DISTINCT primebbl
    FROM {{ ref('int__pluto_rpad_geo') }}
    LIMIT 100
),
{% endif %}

-- Get distinct primebbl records as the base
base_bbls AS (
    SELECT DISTINCT primebbl AS bbl
    FROM {{ ref('int__pluto_rpad_geo') }}
    {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
    WHERE primebbl IN (SELECT primebbl FROM dev_sample_bbls)
    {% endif %}
),

-- One-to-one attributes for non-condo records
noncondo_attrs AS (
    SELECT
        primebbl AS bbl,
        bldgcl AS bldgclass,
        story::text AS numfloors,
        lfft::text AS lotfront,
        ldft::text AS lotdepth,
        bfft::text AS bldgfront,
        bdft::text AS bldgdepth,
        ext,
        condo_number AS condono,
        land_area::text AS lotarea,
        gross_sqft::text AS bldgarea,
        yrbuilt AS yearbuilt,
        yralt1 AS yearalter1,
        yralt2 AS yearalter2,
        owner AS ownername,
        irreg AS irrlotcode,
        concat(housenum_lo, ' ', street_name) AS address,
        CASE
            WHEN numberofexistingstructuresonlot::integer > 0 
            THEN numberofexistingstructuresonlot::integer::text
            ELSE bldgs::text
        END AS numbldgs,
        ap_boro || lpad(ap_block, 5, '0') || lpad(ap_lot, 4, '0') AS appbbl,
        ap_datef AS appdate
    FROM {{ ref('int__pluto_rpad_geo') }}
    WHERE
        tl NOT LIKE '75%'
        AND condo_number IS NULL
        {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
        AND primebbl IN (SELECT primebbl FROM dev_sample_bbls)
        {% endif %}
),

-- One-to-one attributes for condo records (75xx lots)
condo_attrs AS (
    SELECT
        primebbl AS bbl,
        bldgcl AS bldgclass,
        story::text AS numfloors,
        lfft::text AS lotfront,
        ldft::text AS lotdepth,
        bfft::text AS bldgfront,
        bdft::text AS bldgdepth,
        ext,
        condo_number AS condono,
        land_area::text AS lotarea,
        yrbuilt AS yearbuilt,
        yralt1 AS yearalter1,
        yralt2 AS yearalter2,
        owner AS ownername,
        irreg AS irrlotcode,
        concat(housenum_lo, ' ', street_name) AS address,
        CASE
            WHEN numberofexistingstructuresonlot::integer > 0 
            THEN numberofexistingstructuresonlot::integer::text
            ELSE bldgs::text
        END AS numbldgs,
        ap_boro || lpad(ap_block, 5, '0') || lpad(ap_lot, 4, '0') AS appbbl,
        ap_datef AS appdate
    FROM {{ ref('int__pluto_rpad_geo') }}
    WHERE
        tl LIKE '75%'
        AND condo_number IS NOT NULL
        AND condo_number <> '0'
        {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
        AND primebbl IN (SELECT primebbl FROM dev_sample_bbls)
        {% endif %}
),

-- Aggregate building area for condo units
bldgarea_agg AS (
    SELECT
        primebbl AS bbl,
        sum(gross_sqft::numeric)::text AS bldgarea
    FROM {{ ref('int__pluto_rpad_geo') }}
    WHERE
        tl NOT LIKE '75%'
        AND condo_number IS NOT NULL
        AND condo_number <> '0'
        {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
        AND primebbl IN (SELECT primebbl FROM dev_sample_bbls)
        {% endif %}
    GROUP BY primebbl
),

-- Aggregate unit counts
units_agg AS (
    SELECT
        primebbl AS bbl,
        sum(coop_apts::integer)::text AS unitsres,
        sum(units::integer)::text AS unitstotal
    FROM {{ ref('int__pluto_rpad_geo') }}
    WHERE tl NOT LIKE '75%'
    {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
    AND primebbl IN (SELECT primebbl FROM dev_sample_bbls)
    {% endif %}
    GROUP BY primebbl
),

-- Aggregate financial fields
financial_agg AS (
    SELECT
        primebbl AS bbl,
        sum(curavl_act::double precision)::text AS assessland,
        sum(curavt_act::double precision)::text AS assesstot,
        sum(curext_act::double precision)::text AS exempttot
    FROM {{ ref('int__pluto_rpad_geo') }}
    {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
    WHERE primebbl IN (SELECT primebbl FROM dev_sample_bbls)
    {% endif %}
    GROUP BY primebbl
),

-- Fill missing appbbl for condo lots from unit lots
unit_appbbls AS (
    SELECT
        prg.primebbl AS bbl,
        min(prg.ap_boro || lpad(prg.ap_block, 5, '0') || lpad(prg.ap_lot, 4, '0')) AS appbbl
    FROM {{ ref('int__pluto_rpad_geo') }} AS prg
    WHERE
        right(prg.primebbl, 4) LIKE '75%'
        AND prg.primebbl <> prg.bbl
        {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
        AND prg.primebbl IN (SELECT primebbl FROM dev_sample_bbls)
        {% endif %}
    GROUP BY prg.primebbl
),

-- Supplementary attributes from condo descriptive table
supplementary_attrs AS (
    SELECT
        "PARID" AS bbl,
        "LandSize"::text AS lotarea,
        "Story"::text AS numfloors,
        "YearBuilt"::text AS yearbuilt
    FROM {{ ref('pluto_input_condolot_descriptiveattributes') }}
    WHERE
        "LandSize"::numeric > 0
        OR "Story"::numeric > 0
        OR "YearBuilt"::numeric > 0
)

-- Final assembly with COALESCE to merge all sources
SELECT
    base.bbl,
    -- Prefer condo attributes, fallback to noncondo
    COALESCE(condo.bldgclass, noncondo.bldgclass) AS bldgclass,
    NULL AS ownertype,  -- This field is not populated in original SQL
    COALESCE(condo.ownername, noncondo.ownername) AS ownername,
    -- Lotarea: prefer noncondo/condo, fallback to supplementary if zero
    CASE
        WHEN COALESCE(condo.lotarea, noncondo.lotarea, '0')::numeric = 0
        THEN COALESCE(supp.lotarea, condo.lotarea, noncondo.lotarea)
        ELSE COALESCE(condo.lotarea, noncondo.lotarea)
    END AS lotarea,
    -- Bldgarea: prefer aggregated for condos, fallback to noncondo
    COALESCE(bldgarea_agg.bldgarea, noncondo.bldgarea) AS bldgarea,
    COALESCE(condo.numbldgs, noncondo.numbldgs) AS numbldgs,
    -- Numfloors: prefer noncondo/condo, fallback to supplementary if zero
    CASE
        WHEN COALESCE(condo.numfloors, noncondo.numfloors, '0')::numeric = 0
        THEN COALESCE(supp.numfloors, condo.numfloors, noncondo.numfloors)
        ELSE COALESCE(condo.numfloors, noncondo.numfloors)
    END AS numfloors,
    units.unitsres,
    units.unitstotal,
    COALESCE(condo.lotfront, noncondo.lotfront) AS lotfront,
    COALESCE(condo.lotdepth, noncondo.lotdepth) AS lotdepth,
    COALESCE(condo.bldgfront, noncondo.bldgfront) AS bldgfront,
    COALESCE(condo.bldgdepth, noncondo.bldgdepth) AS bldgdepth,
    COALESCE(condo.ext, noncondo.ext) AS ext,
    COALESCE(condo.irrlotcode, noncondo.irrlotcode) AS irrlotcode,
    fin.assessland,
    fin.assesstot,
    NULL AS exemptland,  -- Field no longer exists in source
    fin.exempttot,
    -- Yearbuilt: prefer noncondo/condo, fallback to supplementary if zero, normalize NULL/0 to '0'
    CASE
        WHEN COALESCE(condo.yearbuilt, noncondo.yearbuilt, '0')::numeric = 0
        THEN COALESCE(supp.yearbuilt, '0')
        ELSE COALESCE(condo.yearbuilt, noncondo.yearbuilt)
    END AS yearbuilt,
    -- Yearalter1: normalize NULL/0 to '0' (from yearbuiltalt.sql)
    CASE
        WHEN COALESCE(condo.yearalter1, noncondo.yearalter1) IS NULL
        OR COALESCE(condo.yearalter1, noncondo.yearalter1, '0')::numeric = 0
        THEN '0'
        ELSE COALESCE(condo.yearalter1, noncondo.yearalter1)
    END AS yearalter1,
    -- Yearalter2: normalize NULL/0 to '0' (from yearbuiltalt.sql)
    CASE
        WHEN COALESCE(condo.yearalter2, noncondo.yearalter2) IS NULL
        OR COALESCE(condo.yearalter2, noncondo.yearalter2, '0')::numeric = 0
        THEN '0'
        ELSE COALESCE(condo.yearalter2, noncondo.yearalter2)
    END AS yearalter2,
    COALESCE(condo.condono, noncondo.condono) AS condono,
    -- Appbbl: prefer noncondo/condo, fallback to unit_appbbls for missing condo lots
    COALESCE(condo.appbbl, noncondo.appbbl, unit_appbbls.appbbl) AS appbbl,
    COALESCE(condo.appdate, noncondo.appdate) AS appdate,
    COALESCE(condo.address, noncondo.address) AS address
FROM base_bbls AS base
LEFT JOIN noncondo_attrs AS noncondo ON base.bbl = noncondo.bbl
LEFT JOIN condo_attrs AS condo ON base.bbl = condo.bbl
LEFT JOIN bldgarea_agg ON base.bbl = bldgarea_agg.bbl
LEFT JOIN units_agg AS units ON base.bbl = units.bbl
LEFT JOIN financial_agg AS fin ON base.bbl = fin.bbl
LEFT JOIN unit_appbbls ON base.bbl = unit_appbbls.bbl
LEFT JOIN supplementary_attrs AS supp ON base.bbl = supp.bbl
