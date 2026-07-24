{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT
    globalid,
    unit_short,
    boroughcommand AS fire_division,
    battalion AS fire_battalion,
    -- over half the source rows are MultiSurface; curved geometry silently drops
    -- parts in overlay operations, so linearize as the other polygon staging models do
    st_makevalid(linearize(geom)) AS geom,
    geom AS raw_geom
FROM {{ source('recipe_sources', 'dcp_cscl_firecompany') }}
