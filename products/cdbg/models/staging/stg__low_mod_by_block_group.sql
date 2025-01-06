{{ config(
    materialized = 'table',
    indexes=[{'columns': ['geoid']}]
) }}

SELECT
    "GEOID"::text AS geoid, -- TODO: coerce to text in ingest
    "BORO" AS boro,
    "TRACT"::text AS tract,
    "BLKGRP"::text AS block_group,
    REPLACE("LOWMODUNIV", ',', '')::integer AS potential_lowmod_population,
    REPLACE("LOWMOD", ',', '')::integer AS low_mod_income_population,
    RTRIM("LOWMOD_PCT", '%')::numeric AS low_mod_income_population_percentage
FROM {{ source("recipe_sources", "hud_lowmodincomebyblockgroup") }}
