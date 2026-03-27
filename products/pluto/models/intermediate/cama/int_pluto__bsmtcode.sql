-- Migrated from: pluto_build/sql/cama_bsmttype.sql
-- Assigns basement type code based on CAMA data
-- Logic:
--   1. Get highest bsmnt_type and bsmntgradient for each lot (where bldgnum = '1')
--   2. Match to pluto_input_bsmtcode lookup table to get bsmtcode
--   3. Assign '5' (Unknown) to lots without basement data

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

-- Get highest bsmnt_type and bsmntgradient value for each lot
-- Remove 0 (Unknown) bsmnt_type
-- Filter to building number 1 only
cama_ranked AS (
    SELECT
        primebbl AS bbl,
        bsmnt_type,
        bsmntgradient,
        ROW_NUMBER() OVER (
            PARTITION BY primebbl
            ORDER BY bsmnt_type DESC, bsmntgradient DESC
        ) AS row_number
    FROM {{ source('build_sources', 'pluto_input_cama') }}
    WHERE
        bsmnt_type != '0'
        AND bldgnum = '1'
),

-- Match bsmnt_type and bsmntgradient values to lookup table
dcpcamavals AS (
    SELECT DISTINCT
        x.bbl,
        x.bsmnt_type,
        x.bsmntgradient,
        b.bsmtcode
    FROM cama_ranked AS x
    LEFT JOIN {{ ref('pluto_input_bsmtcode') }} AS b
        ON x.bsmnt_type = b.bsmnt_type AND x.bsmntgradient = b.bsmntgradient
    WHERE x.row_number = 1
),

-- Join with base pluto and assign defaults
bsmtcode_final AS (
    SELECT
        p.bbl,
        COALESCE(d.bsmtcode, '5') AS bsmtcode
    FROM base_pluto AS p
    LEFT JOIN dcpcamavals AS d
        ON p.bbl = d.bbl
)

SELECT
    bbl,
    bsmtcode
FROM bsmtcode_final
