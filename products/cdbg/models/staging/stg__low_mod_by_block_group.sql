{{ config(
    materialized = 'table',
    indexes=[{'columns': ['geoid']}]
) }}

SELECT
    "GEOID"::text AS geoid, -- TODO: coerce to text in ingest
    "BORO" AS boro,
    "TRACT"::text AS tract,
    "BLKGRP"::text AS block_group,
    replace("LOWMODUNIV", ',', '')::decimal AS potential_lowmod_population,
    replace("LOW", ',', '')::decimal AS low_income_population,
    replace("LOWMOD", ',', '')::decimal - replace("LOW", ',', '')::decimal AS mod_income_population,
    replace("LOWMOD", ',', '')::decimal AS low_mod_income_population,
    rtrim("LOWMOD_PCT", '%')::numeric AS low_mod_income_population_percentage
FROM {{ source("recipe_sources", "hud_lowmodincomebyblockgroup") }}
