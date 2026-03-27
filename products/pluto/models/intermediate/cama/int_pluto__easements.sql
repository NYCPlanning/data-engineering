-- Migrated from: pluto_build/sql/cama_easements.sql
-- Sets the number of distinct easements associated with each lot
-- Logic:
--   1. Get distinct easements for each lot from int__dof_pts_propmaster
--   2. Count the number of distinct easements per lot
--   3. Set easements to 0 for lots with no easements

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

-- Get distinct easements for each lot
distincteasements AS (
    SELECT DISTINCT
        primebbl AS bbl,
        ease
    FROM {{ ref('int__dof_pts_propmaster') }}
    WHERE ease IS NOT NULL AND ease != ' '
),

-- Count the number of distinct easements for each lot
counteasements AS (
    SELECT
        bbl,
        COUNT(*) AS numeasements
    FROM distincteasements
    WHERE ease IS NOT NULL
    GROUP BY bbl
),

-- Join with base pluto and assign defaults
easements_final AS (
    SELECT
        p.bbl,
        COALESCE(c.numeasements::text, '0') AS easements
    FROM base_pluto AS p
    LEFT JOIN counteasements AS c
        ON p.bbl = c.bbl
)

SELECT
    bbl,
    easements
FROM easements_final
