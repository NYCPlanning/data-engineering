-- Migrated from: pluto_build/sql/cama_lottype.sql
-- Assigns lot type based on CAMA data
-- Logic:
--   1. Remove 0s (Not Available) and 5 (none of the other types)
--   2. Select lowest lot type value where bldgnum is 1
--   3. If no value found, check for lot type value of 5
--   4. Assign '0' (Mixed or Unknown) to remaining records

{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

WITH

{% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
-- Dev mode: Sample 20 BBLs per borough for fast iteration
dev_sample_bbls AS (
    SELECT DISTINCT bbl
    FROM (
        SELECT bbl,
               ROW_NUMBER() OVER (PARTITION BY LEFT(bbl, 1) ORDER BY RANDOM()) AS rn
        FROM {{ target.schema }}.pluto
    ) sub
    WHERE rn <= 20
),
{% endif %}

base_pluto AS (
    SELECT bbl
    FROM {{ target.schema }}.pluto
    {% if env_var('PLUTO_DEV_MODE', 'false') == 'true' %}
    WHERE bbl IN (SELECT bbl FROM dev_sample_bbls)
    {% endif %}
),

-- Get lowest lot type value for each lot (excluding 0 and 5)
-- Filter to building number 1 only
cama_ranked AS (
    SELECT
        primebbl AS bbl,
        lottype,
        ROW_NUMBER() OVER (
            PARTITION BY primebbl
            ORDER BY lottype
        ) AS row_number
    FROM {{ source('build_sources', 'pluto_input_cama') }}
    WHERE
        lottype != '0'
        AND lottype != '5'
        AND bldgnum = '1'
),

dcpcamavals AS (
    SELECT DISTINCT
        bbl,
        lottype
    FROM cama_ranked
    WHERE row_number = 1
),

-- Get lots with lot type value of 5 (as fallback)
lottype_five AS (
    SELECT DISTINCT
        primebbl AS bbl,
        lottype
    FROM {{ source('build_sources', 'pluto_input_cama') }}
    WHERE lottype = '5'
),

-- Join with base pluto and apply priority logic
lottype_final AS (
    SELECT
        p.bbl,
        COALESCE(
            d.lottype,     -- First try the preferred lot types
            f.lottype,     -- Then try lot type 5
            '0'            -- Finally default to 0 (Mixed or Unknown)
        ) AS lottype
    FROM base_pluto AS p
    LEFT JOIN dcpcamavals AS d
        ON p.bbl = d.bbl
    LEFT JOIN lottype_five AS f
        ON p.bbl = f.bbl
)

SELECT
    bbl,
    lottype
FROM lottype_final
