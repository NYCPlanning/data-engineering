WITH source AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_firecompany') }}
)

SELECT
    globalid,
    unit_short,
    boroughcommand AS fire_division,
    battalion AS fire_battalion,
    geom
FROM source
