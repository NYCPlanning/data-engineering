{{ config(
    materialized = 'table',
    indexes=[{'columns': ['geoid']}]
) }}

SELECT
    "GEOID"::text AS geoid, -- TODO: coerce to text in ingest
    "BORO" AS boro,
    "TRACT" AS tract,
    "BLKGRP" AS block_group,
    REPLACE("LOWMODUNIV", ',', '')::numeric AS total_population,
    REPLACE("LOWMOD", ',', '')::numeric AS lowmod_population,
    RTRIM("LOWMOD_PCT", '%')::numeric AS lowmod_pct
FROM {{ source("recipe_sources", "hud_lowmodincomebyblockgroup") }}
