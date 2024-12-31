{{ config(
    materialized = 'table',
    indexes=[{'columns': ['geoid']}]
) }}

SELECT
    "GEOID"::text AS geoid, -- TODO: coerce to text in ingest
    "BORO" as boro,
    "TRACT" as tract,
    "BLKGRP" as block_group,
    RTRIM("LOWMOD_PCT", '%')::numeric AS lowmod_pct
FROM {{ source("recipe_sources", "hud_lowmodincomebyblockgroup") }}
