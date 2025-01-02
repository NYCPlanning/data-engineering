{{ config(
    materialized = 'table',
    indexes=[{'columns': ['geoid']}]
) }}

SELECT
    "GEOID"::text AS geoid, -- TODO: coerce to text in ingest
    "BORO" AS boro,
    "TRACT" AS tract,
    "BLKGRP" AS block_group,
    REPLACE("LOWMODUNIV", ',', '')::numeric AS potential_lowmod_population,
    REPLACE("LOWMOD", ',', '')::numeric AS low_mod_income_population,
    RTRIM("LOWMOD_PCT", '%')::numeric AS low_mod_income_population_percentage
FROM {{ source("recipe_sources", "hud_lowmodincomebyblockgroup") }}
