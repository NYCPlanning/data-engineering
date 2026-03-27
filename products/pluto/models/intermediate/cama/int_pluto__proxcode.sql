-- Migrated from: pluto_build/sql/cama_proxcode.sql
-- Assigns proxy code (property classification)
-- Logic:
--   1. Recode DOF values to DCP values (5->2, 4->3, 6->3)
--   2. Remove 0s (Not Available) and 'N' values
--   3. Select proxcode from record where bldgnum is 1
--   4. Take max proxcode if multiple values exist
--   5. Assign '0' (Not Available) to remaining records

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

-- Recode DOF values to DCP values and filter
-- Only use records where bldgnum is 1
dcpcamavals AS (
    SELECT
        primebbl AS bbl,
        (CASE
            WHEN proxcode = '5' THEN '2'
            WHEN proxcode = '4' THEN '3'
            WHEN proxcode = '6' THEN '3'
            ELSE proxcode
        END) AS proxcode
    FROM {{ source('build_sources', 'pluto_input_cama') }}
    WHERE
        proxcode != '0'
        AND proxcode != 'N'
        AND bldgnum = '1'
),

-- Take max proxcode for each BBL
max_bbl_proxcodes AS (
    SELECT
        bbl,
        MAX(proxcode) AS proxcode
    FROM dcpcamavals
    GROUP BY bbl
),

-- Join with base pluto and assign defaults
proxcode_final AS (
    SELECT
        p.bbl,
        COALESCE(m.proxcode, '0') AS proxcode
    FROM base_pluto AS p
    LEFT JOIN max_bbl_proxcodes AS m
        ON p.bbl = m.bbl
)

SELECT
    bbl,
    proxcode
FROM proxcode_final
