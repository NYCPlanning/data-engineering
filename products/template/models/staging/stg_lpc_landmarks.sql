{{ config(materialized='table') }}

SELECT
    lm_name AS landmark_name,
    boroughid AS borough_name_short,
    bbl,
    wkb_geometry
FROM {{ source('tdb_sources', 'lpc_landmarks') }}
